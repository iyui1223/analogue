#!/bin/bash
# =============================================================================
# F01: Preprocessing Pipeline - Main Orchestrator
# =============================================================================
# This script submits SLURM jobs for preprocessing different reanalysis datasets.
#
# Usage:
#   ./F01_preprocess.sh [OPTIONS]
#
# Options:
#   --mswx     Process MSWX dataset
#   --era5     Process ERA5 dataset
#   --jra3q    Process JRA-3Q dataset
#   --all      Process all available datasets
#   --help     Show this help message
#
# Examples:
#   ./F01_preprocess.sh --mswx              # Process MSWX only
#   ./F01_preprocess.sh --era5 --mswx       # Process ERA5 and MSWX in parallel
#   ./F01_preprocess.sh --all               # Process all datasets
# =============================================================================

set -e

# TODO: It maybe better to use climatology file provided by dataset instead of computing it here?
# p19 has some description on how it is made for JRA-3Q: https://www.data.jma.go.jp/jra/html/JRA-3Q/document/JRA-3Q_LL125_format_v1_ja.pdf

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
SCRIPT_DIR="${ROOT_DIR}/Sh"
LOG_DIR="${ROOT_DIR}/Log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------------
# Parse command line arguments
# -----------------------------------------------------------------------------
PROCESS_MSWX=false
PROCESS_ERA5=false
PROCESS_JRA3Q=false

show_help() {
    echo "F01 Preprocessing Pipeline - Multi-Dataset Orchestrator"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --mswx     Process MSWX dataset"
    echo "  --era5     Process ERA5 dataset"
    echo "  --jra3q    Process JRA-3Q dataset (placeholder - not yet available)"
    echo "  --all      Process all available datasets"
    echo "  --help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --mswx              # Process MSWX only"
    echo "  $0 --era5 --mswx       # Process ERA5 and MSWX in parallel"
    echo "  $0 --all               # Process all datasets"
    echo ""
    exit 0
}

# If no arguments, show help
if [ $# -eq 0 ]; then
    show_help
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mswx)
            PROCESS_MSWX=true
            shift
            ;;
        --era5)
            PROCESS_ERA5=true
            shift
            ;;
        --jra3q)
            PROCESS_JRA3Q=true
            shift
            ;;
        --all)
            PROCESS_MSWX=true
            PROCESS_ERA5=true
            PROCESS_JRA3Q=true
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

# -----------------------------------------------------------------------------
# Submit jobs
# -----------------------------------------------------------------------------
echo "============================================================"
echo "F01: Preprocessing Pipeline - Job Submission"
echo "============================================================"
echo "Time: $(date)"
echo "Root directory: $ROOT_DIR"
echo ""

# Track submitted job IDs
declare -a JOB_IDS
declare -a JOB_NAMES

# Function to submit a job and track its ID
submit_job() {
    local script=$1
    local name=$2
    
    if [ ! -f "$script" ]; then
        echo "[ERROR] Script not found: $script"
        return 1
    fi
    
    echo "Submitting $name job..."
    local result=$(sbatch "$script" 2>&1)
    
    if [[ $result =~ Submitted\ batch\ job\ ([0-9]+) ]]; then
        local job_id="${BASH_REMATCH[1]}"
        JOB_IDS+=("$job_id")
        JOB_NAMES+=("$name")
        echo "  -> Job ID: $job_id"
        return 0
    else
        echo "  -> [ERROR] Failed to submit: $result"
        return 1
    fi
}

# Submit MSWX job
if [ "$PROCESS_MSWX" = true ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "MSWX Dataset"
    echo "------------------------------------------------------------"
    submit_job "${SCRIPT_DIR}/preprocess_mswx_slurm.sh" "MSWX"
fi

# Submit ERA5 job
if [ "$PROCESS_ERA5" = true ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "ERA5 Dataset"
    echo "------------------------------------------------------------"
    submit_job "${SCRIPT_DIR}/preprocess_era5_slurm.sh" "ERA5"
fi

# Submit JRA-3Q job
if [ "$PROCESS_JRA3Q" = true ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "JRA-3Q Dataset"
    echo "------------------------------------------------------------"
    if [ -f "${SCRIPT_DIR}/preprocess_jra3q_slurm.sh" ]; then
        echo "[WARNING] JRA-3Q data path is not yet configured."
        echo "          Edit Const/env_setting.sh to set JRA3Q_DIR before running."
        submit_job "${SCRIPT_DIR}/preprocess_jra3q_slurm.sh" "JRA-3Q"
    else
        echo "[SKIP] JRA-3Q script not found (template only)"
    fi
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Job Submission Summary"
echo "============================================================"

if [ ${#JOB_IDS[@]} -eq 0 ]; then
    echo "No jobs were submitted."
    exit 1
fi

echo "Submitted ${#JOB_IDS[@]} job(s):"
echo ""
for i in "${!JOB_IDS[@]}"; do
    echo "  ${JOB_NAMES[$i]}: Job ID ${JOB_IDS[$i]}"
done

echo ""
echo "Monitor jobs with:"
echo "  squeue -u \$USER"
echo ""
echo "View logs in: $LOG_DIR"
echo "  - preprocess_mswx.out / .err"
echo "  - preprocess_era5.out / .err"
echo "  - preprocess_jra3q.out / .err"
echo ""
echo "============================================================"
