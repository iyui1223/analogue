#!/bin/bash
# =============================================================================
# Pipeline Orchestrator: F01 -> F02
# =============================================================================
# Submits preprocessing and analogue search jobs with SLURM dependencies.
#
# Usage:
#   ./run_pipeline.sh [OPTIONS]
#
# Options:
#   --dataset DATASET   Dataset to process: era5, mswx, jra3q (default: era5)
#   --event EVENT       Event name for analogue search (default: antarctica_peninsula_2020)
#   --f01-only          Run only F01 preprocessing
#   --f02-only          Run only F02 analogue search (assumes F01 complete)
#   --help              Show this help message
#
# Examples:
#   ./run_pipeline.sh                          # Full pipeline with ERA5
#   ./run_pipeline.sh --dataset era5           # Explicit ERA5
#   ./run_pipeline.sh --f01-only               # Only preprocessing
#   ./run_pipeline.sh --f02-only --event east_antarctica_2022
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
SCRIPT_DIR="${ROOT_DIR}/Sh"
LOG_DIR="${ROOT_DIR}/Log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
DATASET="era5"
EVENT="antarctica_peninsula_2020"
RUN_F01=true
RUN_F02=true

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
show_help() {
    echo "Pipeline Orchestrator: F01 (Preprocess) -> F02 (Analogue Search)"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dataset DATASET   Dataset to process: era5, mswx, jra3q (default: era5)"
    echo "  --event EVENT       Event name for analogue search (default: antarctica_peninsula_2020)"
    echo "  --f01-only          Run only F01 preprocessing"
    echo "  --f02-only          Run only F02 analogue search (assumes F01 complete)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                             # Full pipeline with ERA5"
    echo "  $0 --dataset era5 --event east_antarctica_2022"
    echo "  $0 --f01-only                  # Only preprocessing"
    echo "  $0 --f02-only                  # Only analogue search"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)
            DATASET="$2"
            shift 2
            ;;
        --event)
            EVENT="$2"
            shift 2
            ;;
        --f01-only)
            RUN_F01=true
            RUN_F02=false
            shift
            ;;
        --f02-only)
            RUN_F01=false
            RUN_F02=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate dataset
case "$DATASET" in
    era5|mswx|jra3q)
        ;;
    *)
        echo "ERROR: Invalid dataset '$DATASET'. Must be one of: era5, mswx, jra3q"
        exit 1
        ;;
esac

# -----------------------------------------------------------------------------
# Map dataset to preprocessing script
# -----------------------------------------------------------------------------
get_preprocess_script() {
    local ds=$1
    case "$ds" in
        era5)  echo "${SCRIPT_DIR}/preprocess_era5_slurm.sh" ;;
        mswx)  echo "${SCRIPT_DIR}/preprocess_mswx_slurm.sh" ;;
        jra3q) echo "${SCRIPT_DIR}/preprocess_jra3q_slurm.sh" ;;
    esac
}

# -----------------------------------------------------------------------------
# Submit jobs
# -----------------------------------------------------------------------------
echo "============================================================"
echo "Pipeline Orchestrator"
echo "============================================================"
echo "Time: $(date)"
echo "Dataset: $DATASET"
echo "Event: $EVENT"
echo "Run F01: $RUN_F01"
echo "Run F02: $RUN_F02"
echo "============================================================"
echo ""

F01_JOB_ID=""
F02_JOB_ID=""

# ----- F01: Preprocessing -----
if [ "$RUN_F01" = true ]; then
    PREPROCESS_SCRIPT=$(get_preprocess_script "$DATASET")
    
    if [ ! -f "$PREPROCESS_SCRIPT" ]; then
        echo "[ERROR] Preprocessing script not found: $PREPROCESS_SCRIPT"
        exit 1
    fi
    
    echo "------------------------------------------------------------"
    echo "F01: Submitting preprocessing for ${DATASET}..."
    echo "------------------------------------------------------------"
    echo "Script: $PREPROCESS_SCRIPT"
    
    result=$(sbatch "$PREPROCESS_SCRIPT" 2>&1)
    
    if [[ $result =~ Submitted\ batch\ job\ ([0-9]+) ]]; then
        F01_JOB_ID="${BASH_REMATCH[1]}"
        echo "  -> F01 Job ID: $F01_JOB_ID"
    else
        echo "  -> [ERROR] Failed to submit F01: $result"
        exit 1
    fi
    echo ""
fi

# ----- F02: Analogue Search -----
if [ "$RUN_F02" = true ]; then
    F02_SCRIPT="${SCRIPT_DIR}/F02_analogue_search_slurm.sh"
    
    if [ ! -f "$F02_SCRIPT" ]; then
        echo "[ERROR] Analogue search script not found: $F02_SCRIPT"
        exit 1
    fi
    
    echo "------------------------------------------------------------"
    echo "F02: Submitting analogue search for ${DATASET}/${EVENT}..."
    echo "------------------------------------------------------------"
    echo "Script: $F02_SCRIPT"
    
    # Build sbatch command with optional dependency
    SBATCH_ARGS=""
    if [ -n "$F01_JOB_ID" ]; then
        SBATCH_ARGS="--dependency=afterok:${F01_JOB_ID}"
        echo "Dependency: afterok:${F01_JOB_ID} (waits for F01 to complete successfully)"
    fi
    
    # Export dataset and event for the script
    result=$(DATASET="$DATASET" EVENT="$EVENT" sbatch $SBATCH_ARGS "$F02_SCRIPT" 2>&1)
    
    if [[ $result =~ Submitted\ batch\ job\ ([0-9]+) ]]; then
        F02_JOB_ID="${BASH_REMATCH[1]}"
        echo "  -> F02 Job ID: $F02_JOB_ID"
    else
        echo "  -> [ERROR] Failed to submit F02: $result"
        exit 1
    fi
    echo ""
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo "============================================================"
echo "Pipeline Submission Summary"
echo "============================================================"
echo ""
echo "Dataset: $DATASET"
echo "Event: $EVENT"
echo ""

if [ -n "$F01_JOB_ID" ]; then
    echo "F01 (Preprocess):      Job ID $F01_JOB_ID"
fi
if [ -n "$F02_JOB_ID" ]; then
    echo "F02 (Analogue Search): Job ID $F02_JOB_ID"
    if [ -n "$F01_JOB_ID" ]; then
        echo "    -> Will start after F01 completes successfully"
    fi
fi

echo ""
echo "Monitor with:"
echo "  squeue -u \$USER"
echo ""
echo "View logs:"
if [ -n "$F01_JOB_ID" ]; then
    echo "  F01: ${LOG_DIR}/preprocess_${DATASET}.out"
fi
if [ -n "$F02_JOB_ID" ]; then
    echo "  F02: ${LOG_DIR}/F02_analogue_search.out"
fi
echo "============================================================"
