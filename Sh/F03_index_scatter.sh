#!/bin/bash
# =============================================================================
# F03_index_scatter: Climate Index Scatter Plots for Analogue Members
# =============================================================================
# Downloads climate index data (Nino3.4, GISTEMP, PDO) if needed, then
# calls Python/make_index_scatter.py to produce scatter plots showing
# where each analogue sits in index space.
#
# Usage:
#   ./F03_index_scatter.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#   - Remote climate indices (downloaded to Const/)
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/scatter_*.png
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

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)  DATASET="$2"; shift 2 ;;
        --event)    EVENT="$2";   shift 2 ;;
        --help|-h)
            echo "Usage: $0 --dataset DATASET --event EVENT"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$EVENT" ]; then
    echo "ERROR: --event is required"
    exit 1
fi

# -----------------------------------------------------------------------------
# Read event config from YAML
# -----------------------------------------------------------------------------
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"

SNAPSHOT=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "snapshot_date:" | head -1 | sed 's/.*: *"\?\([^"]*\)"\?.*/\1/')

if [ -z "$SNAPSHOT" ]; then
    echo "ERROR: Event '$EVENT' not found or missing snapshot_date"
    exit 1
fi

# -----------------------------------------------------------------------------
# Check analogues file
# -----------------------------------------------------------------------------
ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"

if [ ! -f "$ANALOGUES_FILE" ]; then
    echo "ERROR: Analogues file not found: $ANALOGUES_FILE"
    echo "Please run F02_analogue_search first."
    exit 1
fi

OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"
mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "F03_index_scatter: Climate Index Scatter Plots"
echo "============================================================"
echo "Event:    $EVENT"
echo "Snapshot: $SNAPSHOT"
echo "Output:   $OUTPUT_DIR"
echo "============================================================"

# -----------------------------------------------------------------------------
# Remote index URLs
# -----------------------------------------------------------------------------
NINA34_URL="https://psl.noaa.gov/data/correlation/nina34.anom.data"
GISTEMP_GLB_URL="https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt"
GISTEMP_SH_URL="https://data.giss.nasa.gov/gistemp/tabledata_v4/T_AIRS/SH.Ts+dSST.txt"
PDO_URL="https://psl.noaa.gov/pdo/data/pdo.timeseries.sstens.data"

# Local targets
NINA34_FILE="${CONST_DIR}/nina34.anom.data"
GISTEMP_GLB_FILE="${CONST_DIR}/GLB.Ts+dSST.txt"
GISTEMP_SH_FILE="${CONST_DIR}/SH.Ts+dSST.txt"
PDO_FILE="${CONST_DIR}/pdo.timeseries.sstens.data"

# Download if missing or older than 1 day
download_if_needed() {
    local url="$1"; local out="$2"
    if [ ! -f "$out" ]; then
        echo "Downloading $(basename "$out") ..."
        wget -q -O "$out" "$url" || { echo "Warning: wget failed for $url"; }
    else
        if [ "$(find "$out" -mtime +1 -print)" ]; then
            echo "Refreshing $(basename "$out") ..."
            wget -q -O "$out" "$url" || echo "Warning: refresh failed for $url"
        fi
    fi
}

download_if_needed "$NINA34_URL" "$NINA34_FILE"
download_if_needed "$GISTEMP_GLB_URL" "$GISTEMP_GLB_FILE"
download_if_needed "$GISTEMP_SH_URL" "$GISTEMP_SH_FILE"
download_if_needed "$PDO_URL" "$PDO_FILE"

# -----------------------------------------------------------------------------
# Run Python scatter-plot script
# -----------------------------------------------------------------------------
PY_SCRIPT="${PYTHON_DIR}/make_index_scatter.py"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: ${PY_SCRIPT} not found. Please add 'make_index_scatter.py' to ${PYTHON_DIR}."
    exit 1
fi

echo "Running index scatter plots..."
python3 "$PY_SCRIPT" \
    --analogues "$ANALOGUES_FILE" \
    --nina "$NINA34_FILE" \
    --pdo "$PDO_FILE" \
    --glb "$GISTEMP_GLB_FILE" \
    --original-date "$SNAPSHOT" \
    --outdir "$OUTPUT_DIR"

echo ""
echo "============================================================"
echo "F03_index_scatter complete."
echo "============================================================"
