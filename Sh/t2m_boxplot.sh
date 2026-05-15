#!/bin/bash
# =============================================================================
# t2m_boxplot: T2m Box-and-Whisker by Lead Time for Analogue Members
# =============================================================================
# Generates T2m box plots showing past/present analogue spread vs target event.
# Uses pre-sliced data (Data/F01_preprocess/era5/t2m_daily_max) for fast loading.
#
# Usage:
#   ./t2m_boxplot.sh --dataset era5 --event antarctica_peninsula_2020
#   ./t2m_boxplot.sh --dataset era5 --event antarctica_peninsula_2020 --ntop 5
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Const/peninsula_domain_masks.nc (optional eastern-slope domain mask)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#   - Data/F01_preprocess/era5/t2m_daily_max/YYYYMM.nc (monthly T2m daily max)
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/t2m_boxplot_top{N}.png
#   Figs/F03_visualization/{event}/{dataset}/t2m_boxplot_east_slope_top{N}.png
# =============================================================================

set -eox

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"
if ! ensure_poetry_env "${ROOT_DIR}"; then
    echo "ERROR: Failed to prepare Poetry environment."
    exit 1
fi

CONST_DIR="${ROOT_DIR}/Const"
PYTHON_DIR="${ROOT_DIR}/Python"
FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
NTOP="${NTOP:-5}"
DO_EAST_SLOPE_BOXPLOT="${DO_EAST_SLOPE_BOXPLOT:-1}"
EAST_SLOPE_MASK_VAR="${EAST_SLOPE_MASK_VAR:-pen_east_slope}"

# Absolute paths (no symlinks)
DATA_SLICE_DIR="${DATA_SLICE_DIR:-${DATA_DIR}/F01_preprocess/era5/t2m_daily_max}"
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"
PENINSULA_DOMAIN_MASKS="${PENINSULA_DOMAIN_MASKS:-${CONST_DIR}/peninsula_domain_masks.nc}"

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

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "t2m_boxplot: T2m Box-and-Whisker by Lead Time"
echo "============================================================"
echo "Event:      $EVENT"
echo "Dataset:    $DATASET"
echo "Data dir:   $DATA_SLICE_DIR"
echo "Top N:      $NTOP"
echo "East slope: $DO_EAST_SLOPE_BOXPLOT"
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

BOXPLOT_ARGS=(--data-dir "$DATA_SLICE_DIR" --analogues "$ANALOGUES_FILE" --events-yaml "$EVENTS_FILE" --event "$EVENT" --outdir "$OUTPUT_DIR" --ntop "$NTOP")
# Optional: skip land mask (default: use land only)
[ "${NO_LAND_MASK:-0}" = "1" ] && BOXPLOT_ARGS+=(--no-land-mask)

run_poetry run python3 "$PY_SCRIPT" "${BOXPLOT_ARGS[@]}"

if [ "$DO_EAST_SLOPE_BOXPLOT" = "1" ]; then
    if [ ! -f "$PENINSULA_DOMAIN_MASKS" ]; then
        echo "[WARN] Peninsula domain masks not found; skipping east-slope boxplot: $PENINSULA_DOMAIN_MASKS"
    else
        EAST_SLOPE_ARGS=("${BOXPLOT_ARGS[@]}" \
            --domain-mask "$PENINSULA_DOMAIN_MASKS" \
            --domain-var "$EAST_SLOPE_MASK_VAR" \
            --domain-label "East Peninsula slope" \
            --output-suffix "east_slope")
        run_poetry run python3 "$PY_SCRIPT" "${EAST_SLOPE_ARGS[@]}"
    fi
fi

echo ""
echo "============================================================"
echo "t2m_boxplot complete."
echo "============================================================"
