#!/bin/bash
# =============================================================================
# cvm_test: Cramér–von Mises test (past vs present analogue T2m distributions)
# =============================================================================
# Uses snapshot-date daily max T2m (domain mean over land in boxplot_region).
# Two tests: 1) CvM asymptotic, 2) Permutation + CvM.
#
# Usage:
#   ./cvm_test.sh --dataset era5 --event antarctica_peninsula_2020
#   NMEMBERS=10 ./cvm_test.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (boxplot_region)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#   - Data/F01_preprocess/era5/t2m_daily_max/YYYYMM.nc (daily max T2m)
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/cvm_test_results.txt
# =============================================================================

set -eox

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

PYTHON_DIR="${ROOT_DIR}/Python"
FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
NMEMBERS="${NMEMBERS:-15}"

DATA_SLICE_DIR="${DATA_SLICE_DIR:-${DATA_DIR}/F01_preprocess/era5/t2m_daily_max}"
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)   DATASET="$2";   shift 2 ;;
        --event)     EVENT="$2";     shift 2 ;;
        --nmembers)  NMEMBERS="$2";  shift 2 ;;
        --help|-h)
            echo "Usage: $0 --dataset DATASET --event EVENT [--nmembers N]"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$EVENT" ]; then
    echo "ERROR: --event is required"
    exit 1
fi

ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"

# -----------------------------------------------------------------------------
# Validate inputs
# -----------------------------------------------------------------------------
if [ ! -d "$DATA_SLICE_DIR" ]; then
    echo "ERROR: Data slice directory not found: $DATA_SLICE_DIR"
    exit 1
fi

if [ ! -f "$ANALOGUES_FILE" ]; then
    echo "ERROR: Analogues file not found: $ANALOGUES_FILE"
    echo "Please run F02_analogue_search first."
    exit 1
fi

echo "============================================================"
echo "cvm_test: Cramér–von Mises (past vs present)"
echo "============================================================"
echo "Event:      $EVENT"
echo "Dataset:    $DATASET"
echo "Data dir:   $DATA_SLICE_DIR"
echo "Members:    $NMEMBERS"
echo "Output:     $OUTPUT_DIR"
echo "============================================================"

# -----------------------------------------------------------------------------
# Run Python script
# -----------------------------------------------------------------------------
PY_SCRIPT="${PYTHON_DIR}/cvm_test.py"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: ${PY_SCRIPT} not found."
    exit 1
fi

CVM_ARGS="--data-dir $DATA_SLICE_DIR --analogues $ANALOGUES_FILE --events-yaml $EVENTS_FILE --event $EVENT --outdir $OUTPUT_DIR --nmembers $NMEMBERS"
[ "${NO_LAND_MASK:-0}" = "1" ] && CVM_ARGS="$CVM_ARGS --no-land-mask"
[ -n "${NPERM:-}" ] && CVM_ARGS="$CVM_ARGS --nperm $NPERM"

python3 "$PY_SCRIPT" $CVM_ARGS

echo ""
echo "============================================================"
echo "cvm_test complete."
echo "============================================================"
