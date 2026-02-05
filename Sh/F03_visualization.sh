#!/bin/bash
# =============================================================================
# F03: Visualization Pipeline for Extreme Weather Events
# =============================================================================
# Generates synoptic visualizations using GrADS.
#
# Usage:
#   ./F03_visualization.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv (analogue dates)
#
# Output: Figs/F03_visualization/{event}/{dataset}/{period}_{yyyymmdd}_synoptic.png
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

# SOOO UGLY! Let's move these to env_setting.sh. along with ROOT_DIR.
GRADS_DIR="${ROOT_DIR}/GrADS"
CONST_DIR="${ROOT_DIR}/Const"
PYTHON_DIR="${ROOT_DIR}/Python"


FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"
ERA5_DAILY="${ERA5_DIR}/daily"
ERA5_INVARIANT="${ERA5_DIR}/invariant"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
GRADS="${GRADS_CMD:-grads}"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset) DATASET="$2"; shift 2 ;;
        --event) EVENT="$2"; shift 2 ;;
        --help|-h) echo "Usage: $0 --dataset DATASET --event EVENT"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$EVENT" ]; then
    echo "ERROR: --event is required"
    exit 1
fi

# -----------------------------------------------------------------------------
# Read event config from YAML (simple grep-based extraction)
# -----------------------------------------------------------------------------
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"

# Extract event block and parse fields
SNAPSHOT=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "snapshot_date:" | head -1 | sed 's/.*: *"\?\([^"]*\)"\?.*/\1/')
DESCRIPTION=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "description:" | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/')
LON1=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_min:" | head -1 | awk '{print $2}')
LON2=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_max:" | head -1 | awk '{print $2}')
LAT1=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_min:" | head -1 | awk '{print $2}')
LAT2=$(grep -A20 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_max:" | head -1 | awk '{print $2}')

if [ -z "$SNAPSHOT" ]; then
    echo "ERROR: Event '$EVENT' not found or missing snapshot_date"
    exit 1
fi

# -----------------------------------------------------------------------------
# Read analogue dates from CSV
# -----------------------------------------------------------------------------
ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"

if [ ! -f "$ANALOGUES_FILE" ]; then
    echo "ERROR: Analogues file not found: $ANALOGUES_FILE"
    echo "Please run F02_analogue_search first."
    exit 1
fi

# Parse CSV (skip header, extract date and period)
PAST_DATES=($(awk -F',' 'NR>1 && $7=="past" {print $1}' "$ANALOGUES_FILE"))
PRESENT_DATES=($(awk -F',' 'NR>1 && $7=="present" {print $1}' "$ANALOGUES_FILE"))

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"
mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "F03: Visualization"
echo "============================================================"
echo "Event: $EVENT"
echo "Description: $DESCRIPTION"
echo "Snapshot: $SNAPSHOT"
echo "Region: lon[$LON1, $LON2] lat[$LAT1, $LAT2]"
echo "Output: $OUTPUT_DIR"
echo "Analogues: ${#PAST_DATES[@]} past, ${#PRESENT_DATES[@]} present"
echo "============================================================"

# -----------------------------------------------------------------------------
# Plot function
# -----------------------------------------------------------------------------
plot_date() {
    local date_str="$1"
    local period="$2"
    local year="${date_str:0:4}"
    
    local T2M="${ERA5_DAILY}/2m_temperature/nc/era5_daily_2m_temperature_${year}.nc"
    local MSLP="${ERA5_DAILY}/mean_sea_level_pressure/nc/era5_daily_mean_sea_level_pressure_${year}.nc"
    local UWIND="${ERA5_DAILY}/u_component_of_wind/nc/era5_daily_u_component_of_wind_${year}.nc"
    local VWIND="${ERA5_DAILY}/v_component_of_wind/nc/era5_daily_v_component_of_wind_${year}.nc"
    local TOPO="${ERA5_INVARIANT}/geopotential/nc/era5_invariant_geopotential_20000101.nc"
    
    # Check required files
    [ ! -f "$T2M" ] && echo "  [SKIP] $date_str: T2M not found" && return 1
    [ ! -f "$MSLP" ] && echo "  [SKIP] $date_str: MSLP not found" && return 1
    
    # Optional wind files
    [ ! -f "$UWIND" ] && UWIND="NONE"
    [ ! -f "$VWIND" ] && VWIND="NONE"
    [ ! -f "$TOPO" ] && TOPO="NONE"
    
    local date_fmt="${date_str//-/}"
    local output="${OUTPUT_DIR}/${period}_${date_fmt}_synoptic.png"
    
    echo "  $period: $date_str"
    
    cd "$GRADS_DIR"
    $GRADS -blcx "run plot_synoptic.gs $T2M $MSLP $UWIND $VWIND $TOPO $date_str $LON1 $LON2 $LAT1 $LAT2 $output $period '$DESCRIPTION'" 2>&1 | grep -E "^(Saved|ERROR)" || true

    # TODOs: add more figures.
    
    [ -f "$output" ] && echo "    -> $(basename $output)" || echo "    -> FAILED"
}

# -----------------------------------------------------------------------------
# Generate plots
# -----------------------------------------------------------------------------
echo ""
echo "Plotting original event..."
plot_date "$SNAPSHOT" "original"

echo ""
echo "Plotting ${#PAST_DATES[@]} past analogues..."
for date in "${PAST_DATES[@]}"; do
    plot_date "$date" "past"
done

echo ""
echo "Plotting ${#PRESENT_DATES[@]} present analogues..."
for date in "${PRESENT_DATES[@]}"; do
    plot_date "$date" "present"
done

# remote URLs (probably more suitable if they are in Const/env_setting.sh)
NINA34_URL="https://psl.noaa.gov/data/correlation/nina34.anom.data"
GISTEMP_GLB_URL="https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt"
GISTEMP_SH_URL="https://data.giss.nasa.gov/gistemp/tabledata_v4/T_AIRS/SH.Ts+dSST.txt"
PDO_URL="https://psl.noaa.gov/pdo/data/pdo.timeseries.sstens.data"

# local targets
NINA34_FILE="${CONST_DIR}/nina34.anom.data"
GISTEMP_GLB_FILE="${CONST_DIR}/GLB.Ts+dSST.txt"
GISTEMP_SH_FILE="${CONST_DIR}/SH.Ts+dSST.txt"
PDO_FILE="${CONST_DIR}/pdo.timeseries.sstens.data"

# Download if missing or older than 1 day (keeps network-lite)
download_if_needed() {
  local url="$1"; local out="$2"
  if [ ! -f "$out" ]; then
    echo "Downloading $(basename "$out") ..."
    wget -q -O "$out" "$url" || { echo "Warning: wget failed for $url"; }
  else
    # quick freshness check: redownload if file older than 1 day
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

# ---------------------------------------------------------------------
# Call python script to build the index scatter plots
# ---------------------------------------------------------------------
PY_SCRIPT="${PYTHON_DIR}/make_index_scatter.py"

# If python script doesn't exist, write it from the repository scripts (assume it's checked in).
# Otherwise use the one present — this prevents overwriting local edits.
if [ ! -f "$PY_SCRIPT" ]; then
  echo "ERROR: ${PY_SCRIPT} not found. Please add the script 'make_index_scatter.py' to ${SCRIPTS_DIR}."
  echo "I provided the script contents separately; save it exactly as that filename and re-run."
  # non-fatal: continue (the plotting step will be skipped)
else
  echo "Running index scatter plots..."
  # Arguments: analogues_file, nina34_file, pdo_file, glb_file, output_dir
  ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"
  python3 "$PY_SCRIPT" \
    --analogues "$ANALOGUES_FILE" \
    --nina "$NINA34_FILE" \
    --pdo "$PDO_FILE" \
    --glb "$GISTEMP_GLB_FILE" \
    --outdir "$OUTPUT_DIR"
fi


echo ""
echo "============================================================"
echo "Complete. Files: $(ls "$OUTPUT_DIR"/*.png 2>/dev/null | wc -l)"
echo "============================================================"
