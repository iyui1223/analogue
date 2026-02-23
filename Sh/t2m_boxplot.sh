#!/bin/bash
# =============================================================================
# t2m_boxplot: T2m Box-and-Whisker by Lead Time for Analogue Members
# =============================================================================
# Generates T2m box plots showing past/present analogue spread vs target event.
# Uses pre-sliced data (Data/data_slice) for fast loading.
#
# Usage:
#   ./t2m_boxplot.sh --dataset era5 --event antarctica_peninsula_2020
#   ./t2m_boxplot.sh --dataset era5 --event antarctica_peninsula_2020 --ntop 5
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#   - Data/data_slice/YYYYMM.nc (monthly T2m, absolute path)
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/t2m_boxplot_top{N}.png
# =============================================================================

set -eox

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

CONST_DIR="${ROOT_DIR}/Const"
PYTHON_DIR="${ROOT_DIR}/Python"
FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
NTOP="${NTOP:-5}"

# Absolute paths (no symlinks)
DATA_SLICE_DIR="${DATA_SLICE_DIR:-/lustre/soge1/projects/andante/cenv1201/proj/analogue/Data/data_slice}"
ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)  DATASET="$2";  shift 2 ;;
        --event)    EVENT="$2";    shift 2 ;;
        --ntop)     NTOP="$2";     shift 2 ;;
        --help|-h)
            echo "Usage: $0 --dataset DATASET --event EVENT [--ntop N]"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$EVENT" ]; then
    echo "ERROR: --event is required"
    exit 1
fi

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

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "t2m_boxplot: T2m Box-and-Whisker by Lead Time"
echo "============================================================"
echo "Event:      $EVENT"
echo "Dataset:    $DATASET"
echo "Data dir:   $DATA_SLICE_DIR"
echo "Top N:      $NTOP"
echo "Output:     $OUTPUT_DIR"
echo "============================================================"

# -----------------------------------------------------------------------------
# Run Python script
# -----------------------------------------------------------------------------
PY_SCRIPT="${PYTHON_DIR}/plot_t2m_boxplot.py"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: ${PY_SCRIPT} not found."
    exit 1
fi

python3 "$PY_SCRIPT" \
    --data-dir "$DATA_SLICE_DIR" \
    --analogues "$ANALOGUES_FILE" \
    --events-yaml "$EVENTS_FILE" \
    --event "$EVENT" \
    --outdir "$OUTPUT_DIR" \
    --ntop "$NTOP"

echo ""
echo "============================================================"
echo "t2m_boxplot complete."
echo "============================================================"
