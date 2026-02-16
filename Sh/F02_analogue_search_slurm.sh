#!/bin/bash
#SBATCH --job-name=F02_analogue
#SBATCH --output=../Log/F02_analogue_search.out
#SBATCH --error=../Log/F02_analogue_search.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=48G
#SBATCH --time=01:00:00

# =============================================================================
# F02: Analogue Search
# =============================================================================
# This script performs:
#   1. CDO pre-slice: extract ±calendar_window days + event bbox from anomaly data
#   2. Load pre-sliced data, use snapshot_date as reference pattern
#   3. Compute latitude-weighted Euclidean distances
#   4. Find top N analogues in past and present periods
#   5. Save results to CSV files
#
# Prerequisites: F01_preprocess must be completed for the specified dataset.
#
# Usage:
#   sbatch F02_analogue_search_slurm.sh              # Uses default (era5)
#   DATASET=era5 sbatch F02_analogue_search_slurm.sh
#   DATASET=mswx sbatch F02_analogue_search_slurm.sh
#   SKIP_CDO=true sbatch ...                         # Skip CDO, use Python/dask lazy loading (faster)
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Load environment settings (venv with poetry must be activated)
# -----------------------------------------------------------------------------
cd /lustre/soge1/projects/andante/cenv1201/proj/analogue
source Const/env_setting.sh

if ! command -v poetry &>/dev/null; then
    echo "ERROR: poetry not found. Ensure Const/env_setting.sh activates the venv that contains poetry."
    exit 1
fi

# -----------------------------------------------------------------------------
# Dataset, Event, and Period selection
# Can be overridden via environment variable:
#   DATASET=mswx sbatch ...
#   EVENT=east_antarctica_2022 sbatch ...
#   PERIOD=past sbatch ...    # Process only past period (1948-1987)
#   PERIOD=present sbatch ... # Process only present period (1988-2026)
# -----------------------------------------------------------------------------
DATASET="${DATASET:-era5}"
EVENT="${EVENT:-antarctica_peninsula_2020}"
PERIOD="${PERIOD:-}"  # Empty = process both periods
SKIP_CDO="${SKIP_CDO:-false}"  # Skip CDO pre-slicing, use Python/dask lazy loading instead

# Validate dataset
case "$DATASET" in
    era5|mswx|jra3q)
        ;;
    *)
        echo "ERROR: Invalid dataset '$DATASET'. Must be one of: era5, mswx, jra3q"
        exit 1
        ;;
esac

echo "============================================================"
echo "F02: Analogue Search"
echo "============================================================"
echo "ROOT_DIR: $ROOT_DIR"
echo "Dataset: $DATASET"
echo "Event: $EVENT"
if [ "$SKIP_CDO" = "true" ]; then
    echo "SKIP_CDO: true (using Python/dask lazy loading)"
fi
echo "============================================================"

# -----------------------------------------------------------------------------
# Check prerequisites
# -----------------------------------------------------------------------------
ANOM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/anomaly"

if [ ! -d "$ANOM_DIR" ] || [ -z "$(ls -A "$ANOM_DIR" 2>/dev/null)" ]; then
    echo "ERROR: No anomaly data found in ${ANOM_DIR}"
    echo "Please run F01_preprocess for ${DATASET} first."
    exit 1
fi

echo ""
echo "Anomaly data directory: ${ANOM_DIR}"
echo "Anomaly files found:"
ls -1 "$ANOM_DIR"/*.nc 2>/dev/null | head -5
echo "..."

# Show events with snapshot_date
echo ""
echo "Events configuration: ${EVENTS_CONFIG}"
echo "Events with snapshot_date:"
grep -A2 "snapshot_date" "${EVENTS_CONFIG}" | head -10 || echo "  (none found)"

# -----------------------------------------------------------------------------
# Step 1: CDO pre-slice (time window + lat/lon bbox) — before analogue search
# -----------------------------------------------------------------------------
SLICED_DIR="${DATA_DIR}/F02_analogue_search/sliced/${DATASET}/${EVENT}"
echo ""
if [ "$SKIP_CDO" = "true" ]; then
    echo "Step 1: Skipped (SKIP_CDO=true). Python will load full anomaly files with lazy slicing."
elif [ -d "$SLICED_DIR" ] && ls "${SLICED_DIR}"/anomaly_*_sliced.nc 1>/dev/null 2>&1; then
    echo "Step 1: Sliced data already exists in ${SLICED_DIR}, skipping."
else
    echo "Step 1: Pre-slicing anomaly data..."
    # Use sequential mode for reliability (avoids HDF5/netCDF threading issues)
    export HDF5_USE_FILE_LOCKING=FALSE
    SLICE_CMD="poetry run python3 Python/dask_slice.py --dataset $DATASET --event $EVENT --sequential"
    echo "Running: $SLICE_CMD"
    eval $SLICE_CMD || {
        echo "ERROR: Pre-slicing failed."
        exit 1
    }
fi

# -----------------------------------------------------------------------------
# Step 2: Run analogue search 
# -----------------------------------------------------------------------------
echo ""
echo "Step 2: Analogue search..."
if [ -n "$PERIOD" ]; then
    echo "Period filter: $PERIOD only"
fi
echo ""

# Build command with optional period argument
CMD="poetry run python3 Python/analogue_search.py --dataset $DATASET --event $EVENT --force"
if [ -n "$PERIOD" ]; then
    CMD="$CMD --period $PERIOD"
fi

echo "Running: $CMD"
eval $CMD

if [ $? -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "F02 Analogue Search Complete"
    echo "============================================================"
    echo "Dataset: $DATASET"
    echo "Event: $EVENT"
    if [ -n "$PERIOD" ]; then
        echo "Period: $PERIOD"
    fi
    echo "Results saved to: ${DATA_DIR}/F02_analogue_search/${DATASET}/${EVENT}/"
    echo ""
    echo "Output files:"
    ls -la "${DATA_DIR}/F02_analogue_search/${DATASET}/${EVENT}/" 2>/dev/null || echo "  (directory listing failed)"
    echo "============================================================"
else
    echo "ERROR in analogue search"
    exit 1
fi
