#!/bin/bash
# =============================================================================
# F03: Visualization Pipeline for Extreme Weather Events
# =============================================================================
# This script generates comprehensive synoptic visualizations using GrADS.
#
# For each event:
#   1. Plot original event snapshot
#   2. Plot past analogues (pre-1988)
#   3. Plot present analogues (1988+)
#
# Each plot includes:
#   - 2m temperature shading
#   - Mean sea level pressure contours
#   - 0°C isotherm line
#   - Surface wind vectors (when available)
#   - Terrain elevation contours
#
# Usage:
#   ./F03_visualization.sh [OPTIONS]
#
# Options:
#   --dataset DATASET   Dataset: era5 (default)
#   --event EVENT       Specific event name (default: all events)
#   --help              Show this help message
#
# Output: Figs/F03_visualization/{event}/{dataset}/{period}_{yyyymmdd}_synoptic.png
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment settings
if [ -f "${ROOT_DIR}/Const/env_setting.sh" ]; then
    source "${ROOT_DIR}/Const/env_setting.sh"
fi

# GrADS scripts directory
GRADS_DIR="${ROOT_DIR}/GrADS"

# Output directory base
FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"

# ERA5 data location (use lustre path for SLURM compatibility)
ERA5_BASE="/lustre/soge1/data/analysis/era5/0.28125x0.28125"
ERA5_DAILY="${ERA5_BASE}/daily"
ERA5_INVARIANT="${ERA5_BASE}/invariant"

# Default dataset
DATASET="${DATASET:-era5}"

# GrADS executable
GRADS="${GRADS_CMD:-grads}"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
EVENT_FILTER=""

show_help() {
    echo "F03: Visualization Pipeline for Extreme Weather Events"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dataset DATASET   Dataset: era5 (default)"
    echo "  --event EVENT       Specific event name"
    echo "  --help              Show this help"
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
            EVENT_FILTER="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# -----------------------------------------------------------------------------
# Analogue dates for antarctica_peninsula_2020
# These are the dates identified from the analogue search
# -----------------------------------------------------------------------------

# Original event snapshot date
ORIGINAL_DATE="2020-02-08"

# Past analogues (1948-1987)
PAST_DATES=(
    "1960-03-10"
    "1972-01-04"
    "1979-03-22"
    "1970-01-23"
    "1977-01-29"
    "1974-01-22"
    "1971-01-09"
    "1957-01-12"
    "1956-01-12"
    "1967-01-16"
    "1986-03-01"
    "1972-03-24"
    "1966-02-10"
    "1959-01-04"
    "1970-03-12"
)

# Present analogues (1988-2026)
PRESENT_DATES=(
    "2006-02-23"
    "1989-03-23"
    "2012-12-25"
    "2013-02-23"
    "2017-03-08"
    "2015-01-01"
    "2020-03-10"
    "2012-12-31"
    "1997-03-15"
    "2011-03-14"
    "1988-01-30"
    "2010-03-05"
    "2020-02-21"
    "1989-02-07"
    "2004-02-11"
)

# Event region (from extreme_events.yaml)
# antarctica_peninsula_2020
LON1=225.0
LON2=325.0
LAT1=-85.0
LAT2=-55.0

echo "============================================================"
echo "F03: Visualization Pipeline"
echo "============================================================"
echo "Dataset: $DATASET"
echo "ERA5 data: $ERA5_BASE"
echo "Output: $FIGS_BASE"
echo "============================================================"

# -----------------------------------------------------------------------------
# Function to get data file paths for a given date
# -----------------------------------------------------------------------------
get_era5_files() {
    local date_str="$1"
    local year="${date_str:0:4}"
    
    # Temperature
    T2M_FILE="${ERA5_DAILY}/2m_temperature/nc/era5_daily_2m_temperature_${year}.nc"
    
    # Mean sea level pressure
    MSLP_FILE="${ERA5_DAILY}/mean_sea_level_pressure/nc/era5_daily_mean_sea_level_pressure_${year}.nc"
    
    # Wind components (may not exist for years before 1979)
    UWIND_FILE="${ERA5_DAILY}/u_component_of_wind/nc/era5_daily_u_component_of_wind_${year}.nc"
    VWIND_FILE="${ERA5_DAILY}/v_component_of_wind/nc/era5_daily_v_component_of_wind_${year}.nc"
    
    # Topography (invariant)
    TOPO_FILE="${ERA5_INVARIANT}/geopotential/nc/era5_invariant_geopotential_20000101.nc"
    
    # Check if wind files exist
    if [ ! -f "$UWIND_FILE" ] || [ ! -f "$VWIND_FILE" ]; then
        UWIND_FILE="NONE"
        VWIND_FILE="NONE"
    fi
}

# -----------------------------------------------------------------------------
# Function to plot a single date
# -----------------------------------------------------------------------------
plot_date() {
    local date_str="$1"
    local period="$2"  # "original", "past", or "present"
    local output_dir="$3"
    local event_title="$4"
    
    # Get file paths
    get_era5_files "$date_str"
    
    # Check required files exist
    if [ ! -f "$T2M_FILE" ]; then
        echo "  [SKIP] Temperature file not found: $T2M_FILE"
        return 1
    fi
    
    if [ ! -f "$MSLP_FILE" ]; then
        echo "  [SKIP] MSLP file not found: $MSLP_FILE"
        return 1
    fi
    
    # Format output filename: {period}_{yyyymmdd}_synoptic.png
    local date_fmt="${date_str//-/}"  # Remove dashes
    local output_file="${output_dir}/${period}_${date_fmt}_synoptic.png"
    
    echo "  Plotting: $date_str ($period) -> $(basename $output_file)"
    
    # Check if wind is available
    local wind_status="with wind"
    if [ "$UWIND_FILE" = "NONE" ]; then
        wind_status="no wind (pre-1979)"
    fi
    echo "    Files: T2M, MSLP, $wind_status"
    
    # Run GrADS
    cd "$GRADS_DIR"
    $GRADS -blc "run plot_synoptic.gs $T2M_FILE $MSLP_FILE $UWIND_FILE $VWIND_FILE $TOPO_FILE $date_str $LON1 $LON2 $LAT1 $LAT2 $output_file $period $event_title" 2>&1 | grep -E "^(Saved:|ERROR:|WARNING:)" || true
    
    if [ -f "$output_file" ]; then
        echo "    -> Created: $(basename $output_file)"
        return 0
    else
        echo "    -> FAILED to create output"
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Main processing
# -----------------------------------------------------------------------------

EVENT_NAME="antarctica_peninsula_2020"
EVENT_TITLE="Antarctica Peninsula 2020 Heatwave"

# Skip if event filter doesn't match
if [ -n "$EVENT_FILTER" ] && [ "$EVENT_NAME" != "$EVENT_FILTER" ]; then
    echo "Event '$EVENT_NAME' doesn't match filter '$EVENT_FILTER', skipping."
    exit 0
fi

echo ""
echo "------------------------------------------------------------"
echo "Event: $EVENT_NAME"
echo "------------------------------------------------------------"
echo "Title: $EVENT_TITLE"
echo "Region: lon[$LON1, $LON2] lat[$LAT1, $LAT2]"

# Create output directory: {event}/{dataset}/
OUTPUT_DIR="${FIGS_BASE}/${EVENT_NAME}/${DATASET}"
mkdir -p "$OUTPUT_DIR"

echo "Output directory: $OUTPUT_DIR"

# -----------------------------------------------------------------------------
# Plot original event
# -----------------------------------------------------------------------------
echo ""
echo "=== Plotting Original Event ==="
plot_date "$ORIGINAL_DATE" "original" "$OUTPUT_DIR" "$EVENT_TITLE"

# -----------------------------------------------------------------------------
# Plot past analogues
# -----------------------------------------------------------------------------
echo ""
echo "=== Plotting Past Analogues (${#PAST_DATES[@]} dates) ==="
for date in "${PAST_DATES[@]}"; do
    plot_date "$date" "past" "$OUTPUT_DIR" "$EVENT_TITLE"
done

# -----------------------------------------------------------------------------
# Plot present analogues
# -----------------------------------------------------------------------------
echo ""
echo "=== Plotting Present Analogues (${#PRESENT_DATES[@]} dates) ==="
for date in "${PRESENT_DATES[@]}"; do
    plot_date "$date" "present" "$OUTPUT_DIR" "$EVENT_TITLE"
done

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "F03 Visualization Complete"
echo "============================================================"
echo "Event: $EVENT_NAME"
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Files created:"
ls -la "$OUTPUT_DIR"/*.png 2>/dev/null | wc -l
echo ""
ls -la "$OUTPUT_DIR"/*.png 2>/dev/null | head -10 || echo "(no PNGs found)"
echo "============================================================"
