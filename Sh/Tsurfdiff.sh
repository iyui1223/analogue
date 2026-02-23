#!/bin/bash
# =============================================================================
# F03_Tsurfdiff: Temperature Difference Maps (analogue minus original)
# =============================================================================
# Generates differential synoptic visualizations using GrADS.
# For each analogue (past/present) at each time offset, plots:
#   - Shaded fill: T2m difference (analogue - original) in deg C
#   - Black dashed contours: original MSLP
#   - Blue (past) / Red (present) solid contours: analogue MSLP
#
# Usage:
#   ./F03_Tsurfdiff.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv (analogue dates)
#   - GrADS .ctl templates via GRADS_CTL_DIR
#
# Output: Figs/F03_visualization/{event}/{dataset}/Tsurfdiff_{period}_{idx}_{offset}.png
# =============================================================================

set -eox

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

GRADS_DIR="${ROOT_DIR}/GrADS"
FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
GRADS="${GRADS_CMD:-grads}"
CTL_DIR="${GRADS_CTL_DIR}"

# -----------------------------------------------------------------------------
# Parameters: temporal half-window in days (default 7 -> creates -7..+7 -> 15 frames)
# -----------------------------------------------------------------------------
WINDOW_DAYS="${WINDOW_DAYS:-7}"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset) DATASET="$2"; shift 2 ;;
        --event)   EVENT="$2";   shift 2 ;;
        --window)  WINDOW_DAYS="$2"; shift 2 ;;
        --help|-h) echo "Usage: $0 --dataset DATASET --event EVENT [--window N]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$EVENT" ]; then
    echo "ERROR: --event is required"
    exit 1
fi

if [ -z "$CTL_DIR" ] || [ ! -d "$CTL_DIR" ]; then
    echo "ERROR: GRADS_CTL_DIR not set or directory not found: $CTL_DIR"
    echo "Please set GRADS_CTL_DIR in Const/env_setting.sh"
    exit 1
fi

# -----------------------------------------------------------------------------
# Read event config from YAML (simple grep-based extraction)
# -----------------------------------------------------------------------------
EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"

SNAPSHOT=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "snapshot_date:" | head -1 | sed 's/.*: *"\?\([^"]*\)"\?.*/\1/')
DESCRIPTION=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "description:" | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/')
LON1=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_min:" | head -1 | awk '{print $2}')
LON2=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_max:" | head -1 | awk '{print $2}')
LAT1=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_min:" | head -1 | awk '{print $2}')
LAT2=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_max:" | head -1 | awk '{print $2}')

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

mapfile -t PAST_DATES < <(awk -F',' 'NR>1 && $7=="past" {print $1}' "$ANALOGUES_FILE")
mapfile -t PRESENT_DATES < <(awk -F',' 'NR>1 && $7=="present" {print $1}' "$ANALOGUES_FILE")

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"
mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "F03_Tsurfdiff: Temperature Difference Maps"
echo "============================================================"
echo "Event:       $EVENT"
echo "Description: $DESCRIPTION"
echo "Snapshot:    $SNAPSHOT"
echo "Region:      lon[$LON1, $LON2] lat[$LAT1, $LAT2]"
echo "Output:      $OUTPUT_DIR"
echo "CTL dir:     $CTL_DIR"
echo "Analogues:   ${#PAST_DATES[@]} past, ${#PRESENT_DATES[@]} present"
echo "Window days: +/-${WINDOW_DAYS} (total frames per date: $((WINDOW_DAYS*2+1)))"
echo "============================================================"

# -----------------------------------------------------------------------------
# offset_sortable: convert signed offset to alphabetically-sortable string
#   e.g. with WINDOW=7: -7->a-7, -6->b-6, ..., 0->h0, 1->i1, ..., 7->o7
# -----------------------------------------------------------------------------
offset_sortable() {
    local offset=$1
    local window=${2:-7}
    local idx=$((offset + window))
    local letter
    letter=$(printf "\\$(printf '%03o' $((idx + 97)))")
    printf "%s%d" "$letter" "$offset"
}

# -----------------------------------------------------------------------------
# date_shift: compute date +/- N days
# -----------------------------------------------------------------------------
date_shift() {
    local base="$1"
    local offset="$2"
    if date -d "${base} ${offset} days" "+%Y-%m-%d" >/dev/null 2>&1; then
        date -d "${base} ${offset} days" "+%Y-%m-%d"
        return 0
    fi
    python3 -c "
from datetime import datetime, timedelta
dt = datetime.fromisoformat('${base}') + timedelta(days=${offset})
print(dt.strftime('%Y-%m-%d'))
"
}

# -----------------------------------------------------------------------------
# Plot function: generate a single Tsurfdiff image
# -----------------------------------------------------------------------------
plot_diff() {
    local analogue_date="$1"  # analogue base date (e.g. 1960-03-10)
    local period="$2"         # past | present
    local idx="$3"            # integer index (1..N)
    local offset="$4"         # signed integer, -7..0..7

    # Compute target dates at this offset
    local orig_target anal_target
    orig_target=$(date_shift "$SNAPSHOT" "$offset") || { echo "  [SKIP] date_shift failed for SNAPSHOT offset $offset"; return 1; }
    anal_target=$(date_shift "$analogue_date" "$offset") || { echo "  [SKIP] date_shift failed for $analogue_date offset $offset"; return 1; }

    # Build output filename
    printf -v idx_padded "%03d" "$idx"
    local offset_str
    offset_str=$(offset_sortable "$offset" "$WINDOW_DAYS")
    local output="${OUTPUT_DIR}/Tsurfdiff_${period}_${idx_padded}_${offset_str}.png"

    echo "  ${period}_${idx_padded}_${offset_str}: orig=${orig_target} anal=${anal_target} (base ${analogue_date}, offset ${offset})"

    # Safely quote the description for GrADS
    local safe_desc="${DESCRIPTION//\'/\'\\\'\'}"

    cd "$GRADS_DIR"
    $GRADS -blcx "run plot_Tsurfdiff.gs $CTL_DIR $orig_target $anal_target $period $LON1 $LON2 $LAT1 $LAT2 $output '$safe_desc'" 2>&1 | grep -E "^(Saved|ERROR)" || true

    [ -f "$output" ] && echo "    -> $(basename "$output")" || echo "    -> FAILED"
}

# -----------------------------------------------------------------------------
# Generate Tsurfdiff plots for all analogues
# -----------------------------------------------------------------------------

echo ""
echo "Plotting ${#PAST_DATES[@]} past analogues (Tsurfdiff)..."
i=0
for date in "${PAST_DATES[@]}"; do
    i=$((i+1))
    for off in $(seq -$WINDOW_DAYS $WINDOW_DAYS); do
        plot_diff "$date" "past" "$i" "$off"
    done
done

echo ""
echo "Plotting ${#PRESENT_DATES[@]} present analogues (Tsurfdiff)..."
j=0
for date in "${PRESENT_DATES[@]}"; do
    j=$((j+1))
    for off in $(seq -$WINDOW_DAYS $WINDOW_DAYS); do
        plot_diff "$date" "present" "$j" "$off"
    done
done

echo ""
echo "============================================================"
echo "F03_Tsurfdiff complete."
echo "Files: $(ls "$OUTPUT_DIR"/Tsurfdiff_*.png 2>/dev/null | wc -l)"
echo "============================================================"
