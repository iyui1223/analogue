#!/bin/bash
# =============================================================================
# F03: Visualization Pipeline
# =============================================================================
# This script generates visualizations for analogue events using GrADS.
#
# For each event with a snapshot_date:
#   1. Plot surface pressure (contour) + anomaly (BWR shading) for snapshot date
#   2. Plot same for top analogues from past and present periods
#
# Usage:
#   ./F03_visualization.sh [OPTIONS]
#
# Options:
#   --dataset DATASET   Dataset to visualize: era5 (default)
#   --event EVENT       Specific event name (default: all events)
#   --help              Show this help message
#
# Output: Figs/F03_visualization/{dataset}/{event}/
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

GRADS_DIR="${ROOT_DIR}/GrADS"
FIGS_DIR="${ROOT_DIR}/Figs/F03_visualization"

# Default dataset
DATASET="${DATASET:-era5}"

# GrADS executable (from env_setting.sh)
GRADS="${GRADS_CMD:-grads}"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
EVENT_FILTER=""

show_help() {
    echo "F03: Visualization Pipeline"
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
# Data paths
# -----------------------------------------------------------------------------
YEARLY_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/yearly"
ANOM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/anomaly"
ANALOGUE_DIR="${DATA_DIR}/F02_analogue_search/${DATASET}"
OUTPUT_DIR="${FIGS_DIR}/${DATASET}"

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "F03: Visualization Pipeline"
echo "============================================================"
echo "Dataset: $DATASET"
echo "Yearly data: $YEARLY_DIR"
echo "Anomaly data: $ANOM_DIR"
echo "Output: $OUTPUT_DIR"
echo "============================================================"

# -----------------------------------------------------------------------------
# Parse events from YAML (simple grep-based parsing)
# -----------------------------------------------------------------------------
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"

if [ ! -f "$EVENTS_FILE" ]; then
    echo "ERROR: Events file not found: $EVENTS_FILE"
    exit 1
fi

# Function to plot a single date
plot_date() {
    local event_name="$1"
    local date_str="$2"
    local lon1="$3"
    local lon2="$4"
    local lat1="$5"
    local lat2="$6"
    local output_prefix="$7"
    local title="$8"
    
    # Extract year from date
    local year="${date_str:0:4}"
    
    # Find data files
    local yearly_file="${YEARLY_DIR}/psurf_${year}.nc"
    local anom_file="${ANOM_DIR}/anomaly_psurf_${year}.nc"
    
    if [ ! -f "$yearly_file" ]; then
        echo "  [SKIP] Yearly file not found: $yearly_file"
        return 1
    fi
    
    if [ ! -f "$anom_file" ]; then
        echo "  [SKIP] Anomaly file not found: $anom_file"
        return 1
    fi
    
    local output_file="${output_prefix}.png"
    
    echo "  Plotting: $date_str -> $output_file"
    
    # Run GrADS
    cd "$GRADS_DIR"
    $GRADS -blc "run plot_psurf_anomaly.gs $yearly_file $anom_file $date_str $lon1 $lon2 $lat1 $lat2 $output_file $title" 2>/dev/null
    
    if [ -f "$output_file" ]; then
        echo "    -> Created: $output_file"
        return 0
    else
        echo "    -> FAILED"
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Process each event
# -----------------------------------------------------------------------------

# Simple YAML parsing: extract events with snapshot_date
# Format: name, snapshot_date, lat_min, lat_max, lon_min, lon_max

# Read events using Python helper
python3 > /tmp/events_list.txt << 'PYEOF'
import yaml
import os

root_dir = os.environ.get('ROOT_DIR', '/home/yi260/rds/hpc-work/analogue')
events_file = f"{root_dir}/Const/extreme_events.yaml"

with open(events_file) as f:
    config = yaml.safe_load(f)

events = config.get('events', [])

for event in events:
    if 'snapshot_date' not in event:
        continue
    
    name = event['name']
    snapshot = event['snapshot_date']
    region = event.get('region', {})
    
    lat_min = region.get('lat_min', -90)
    lat_max = region.get('lat_max', 90)
    lon_min = region.get('lon_min', 0)
    lon_max = region.get('lon_max', 360)
    
    # Output in tab-separated format
    print(f"{name}\t{snapshot}\t{lon_min}\t{lon_max}\t{lat_min}\t{lat_max}")
PYEOF

# Read events and process
while IFS=$'\t' read -r event_name snapshot_date lon1 lon2 lat1 lat2; do
    # Skip if event filter is set and doesn't match
    if [ -n "$EVENT_FILTER" ] && [ "$event_name" != "$EVENT_FILTER" ]; then
        continue
    fi
    
    echo ""
    echo "------------------------------------------------------------"
    echo "Event: $event_name"
    echo "------------------------------------------------------------"
    echo "Snapshot: $snapshot_date"
    echo "Region: lon[$lon1, $lon2] lat[$lat1, $lat2]"
    
    # Create event output directory
    event_output_dir="${OUTPUT_DIR}/${event_name}"
    mkdir -p "$event_output_dir"
    
    # Plot snapshot date
    echo ""
    echo "Plotting snapshot date..."
    plot_date "$event_name" "$snapshot_date" "$lon1" "$lon2" "$lat1" "$lat2" \
        "${event_output_dir}/snapshot_${snapshot_date}" \
        "${event_name} - Snapshot"
    
    # Plot analogues from CSV files
    past_file="${ANALOGUE_DIR}/${event_name}/past_analogues.csv"
    present_file="${ANALOGUE_DIR}/${event_name}/present_analogues.csv"
    
    # Plot past analogues
    if [ -f "$past_file" ]; then
        echo ""
        echo "Plotting past analogues..."
        # Read CSV (skip header), extract dates
        tail -n +2 "$past_file" | while IFS=',' read -r date distance year month day rank period; do
            # Extract just the date part (first 10 chars)
            date_only="${date:0:10}"
            plot_date "$event_name" "$date_only" "$lon1" "$lon2" "$lat1" "$lat2" \
                "${event_output_dir}/past_rank${rank}_${date_only}" \
                "Past Analogue #${rank}"
        done
    else
        echo "  [SKIP] No past analogues file: $past_file"
    fi
    
    # Plot present analogues
    if [ -f "$present_file" ]; then
        echo ""
        echo "Plotting present analogues..."
        tail -n +2 "$present_file" | while IFS=',' read -r date distance year month day rank period; do
            date_only="${date:0:10}"
            plot_date "$event_name" "$date_only" "$lon1" "$lon2" "$lat1" "$lat2" \
                "${event_output_dir}/present_rank${rank}_${date_only}" \
                "Present Analogue #${rank}"
        done
    else
        echo "  [SKIP] No present analogues file: $present_file"
    fi
    
    echo ""
    echo "Event $event_name complete."
    echo "Output directory: $event_output_dir"
    ls -la "$event_output_dir"/*.png 2>/dev/null | head -5 || echo "  (no PNGs found)"

done < /tmp/events_list.txt

# Cleanup
rm -f /tmp/events_list.txt

echo ""
echo "============================================================"
echo "F03 Visualization Complete"
echo "============================================================"
echo "Output: $OUTPUT_DIR"
echo "============================================================"
