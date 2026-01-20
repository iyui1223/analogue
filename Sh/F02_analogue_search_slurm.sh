#!/bin/bash
#SBATCH --job-name=F02_analogue
#SBATCH --output=../Log/F02_analogue_search.out
#SBATCH --error=../Log/F02_analogue_search.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=02:00:00

# =============================================================================
# F02: Analogue Search
# =============================================================================
# This script performs:
#   1. Read event definition from Const/extreme_events.yaml
#   2. Load anomaly data from F01 preprocessing for specified dataset
#   3. Slice to event bounding box on-the-fly
#   4. Use snapshot_date as reference pattern
#   5. Compute latitude-weighted Euclidean distances
#   6. Find top N analogues in past and present periods
#   7. Save results to CSV files
#
# Prerequisites: F01_preprocess must be completed for the specified dataset.
#
# Usage:
#   sbatch F02_analogue_search_slurm.sh              # Uses default (era5)
#   DATASET=era5 sbatch F02_analogue_search_slurm.sh
#   DATASET=mswx sbatch F02_analogue_search_slurm.sh
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Load environment settings
# -----------------------------------------------------------------------------
ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

# -----------------------------------------------------------------------------
# Dataset selection
# Can be overridden via environment variable: DATASET=mswx sbatch ...
# -----------------------------------------------------------------------------
DATASET="${DATASET:-era5}"

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
# Run analogue search
# -----------------------------------------------------------------------------
echo ""
echo "Starting analogue search..."
echo ""

cd "$ROOT_DIR"
python Python/analogue_search.py --dataset "$DATASET" --all --force

if [ $? -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "F02 Analogue Search Complete"
    echo "============================================================"
    echo "Dataset: $DATASET"
    echo "Results saved to: ${DATA_DIR}/F02_analogue_search/${DATASET}/"
    echo ""
    echo "Output files:"
    ls -la "${DATA_DIR}/F02_analogue_search/${DATASET}/" 2>/dev/null || echo "  (directory listing failed)"
    echo "============================================================"
else
    echo "ERROR in analogue search"
    exit 1
fi
