#!/bin/bash
#SBATCH --job-name=F02_analogue
#SBATCH --output=Log/F02_analogue_search.out
#SBATCH --error=Log/F02_analogue_search.err
#SBATCH --partition=icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=02:00:00

# =============================================================================
# F02: Analogue Search
# =============================================================================
# This script performs:
#   1. Load smoothed bbox data from F01 preprocessing
#   2. Compute latitude-weighted Euclidean distances
#   3. Find top N analogues in past and present periods
#   4. Save results to CSV files
#
# Prerequisites: F01_preprocess must be completed first.
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Load environment settings
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

# Activate Python environment
eval "$PYTHON_ENV_CMD"

echo "============================================================"
echo "F02: Analogue Search"
echo "============================================================"
echo "ROOT_DIR: $ROOT_DIR"
echo "============================================================"

# -----------------------------------------------------------------------------
# Check prerequisites
# -----------------------------------------------------------------------------
EVENTS_DIR="${DATA_DIR}/F01_preprocess/events"

if [ ! -d "$EVENTS_DIR" ] || [ -z "$(ls -A "$EVENTS_DIR" 2>/dev/null)" ]; then
    echo "ERROR: No event data found in ${EVENTS_DIR}"
    echo "Please run F01_preprocess first."
    exit 1
fi

echo "Event data directory: ${EVENTS_DIR}"
echo "Events found:"
ls -1 "$EVENTS_DIR"

# -----------------------------------------------------------------------------
# Run analogue search
# -----------------------------------------------------------------------------
echo ""
echo "Starting analogue search..."
echo ""

cd "$ROOT_DIR"
python Python/analogue_search.py --all

if [ $? -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "F02 Analogue Search Complete"
    echo "============================================================"
    echo "Results saved to: ${DATA_DIR}/F02_analogue_search/"
    echo "============================================================"
else
    echo "ERROR in analogue search"
    exit 1
fi
