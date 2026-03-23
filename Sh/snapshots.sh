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
# Output: Figs/F03_visualization/{event}/{dataset}/daily_snapshots/Tsurf_*.png
# where:
#   period = past | present | target
#   index  = integer index of analogue in the list (starting at 1). target uses 0
#   offset = signed integer indicating days difference from analogue date (-7 .. 0 .. 7)
# =============================================================================

set -eox

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

GRADS_DIR="${ROOT_DIR}/GrADS"
CONST_DIR="${ROOT_DIR}/Const"
PYTHON_DIR="${ROOT_DIR}/Python"


FIGS_BASE="${ROOT_DIR}/Figs/F03_visualization"
# F01 domain-sliced monthly files (primary data source)
F01_DATA_DIR="${F01_ERA5_DAILY_MEAN:-${DATA_DIR}/F01_preprocess/era5/daily_mean}"
F01_INVARIANT="${F01_ERA5_INVARIANT:-${DATA_DIR}/F01_preprocess/era5/invariant}"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
GRADS="${GRADS_CMD:-grads}"

# -----------------------------------------------------------------------------
# Parameters: temporal half-window in days (default 7 -> creates -7..+7 -> 15 frames)
# -----------------------------------------------------------------------------
WINDOW_DAYS="${WINDOW_DAYS:-7}" # Best to read from start_date and end_date in the event config.

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset) DATASET="$2"; shift 2 ;;
        --event) EVENT="$2"; shift 2 ;;
        --window) WINDOW_DAYS="$2"; shift 2 ;;    # optional override
        --help|-h) echo "Usage: $0 --dataset DATASET --event EVENT [--window N]"; exit 0 ;;
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

# Parse CSV (skip header, extract date and period)
# Collect dates in arrays preserving order
mapfile -t PAST_DATES < <(awk -F',' 'NR>1 && $7=="past" {print $1}' "$ANALOGUES_FILE")
mapfile -t PRESENT_DATES < <(awk -F',' 'NR>1 && $7=="present" {print $1}' "$ANALOGUES_FILE")

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}/daily_snapshots"
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
echo "Window days: ±${WINDOW_DAYS} (total frames per date: $((WINDOW_DAYS*2+1)))"
echo "============================================================"

# -----------------------------------------------------------------------------
# date_shift: compute date +/- N days robustly (works if GNU date not available)
# usage: date_shift DATE_STR OFFSET  -> prints YYYY-MM-DD
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# offset_sortable: convert signed offset to alphabetically-sortable string
#   Maps offset to letter prefix: -WINDOW→a, -(WINDOW-1)→b, ..., 0→letter, ..., +WINDOW→last
#   e.g. with WINDOW=7: -7→a-7, -6→b-6, ..., 0→h0, 1→i1, ..., 7→o7
# usage: offset_sortable OFFSET WINDOW_DAYS  -> prints e.g. "h0" or "a-7"
# -----------------------------------------------------------------------------
offset_sortable() {
    local offset=$1
    local window=${2:-7}
    local idx=$((offset + window))  # 0-based index into alphabet
    local letter
    letter=$(printf "\\$(printf '%03o' $((idx + 97)))")  # a=97 in ASCII
    printf "%s%d" "$letter" "$offset"
}

date_shift() {
    local base="$1"
    local offset="$2"  # signed integer, e.g. -7, 0, 5
    # try GNU date first
    if date -d "${base} ${offset} days" "+%Y-%m-%d" >/dev/null 2>&1; then
        date -d "${base} ${offset} days" "+%Y-%m-%d"
        return 0
    fi
    # fallback: python
    python3 - <<PYCODE
from datetime import datetime, timedelta
import sys
try:
    base = sys.argv[1]
    offset = int(sys.argv[2])
    dt = datetime.fromisoformat(base) + timedelta(days=offset)
    print(dt.strftime("%Y-%m-%d"))
except Exception as e:
    sys.exit(2)
PYCODE
}

# -----------------------------------------------------------------------------
# Plot function: accepts base date, period label, analogue index, and offset
# -----------------------------------------------------------------------------
plot_date() {
    local base_date="$1"   # e.g. 2020-02-08 (the analogue date around which offsets are taken)
    local period="$2"      # past | present | target
    local idx="$3"         # integer index for analogue (1..N), '0' for target
    local offset="$4"      # signed integer, -7..0..7

    # compute target date
    local target_date
    target_date=$(date_shift "$base_date" "$offset") || { echo "  [SKIP] date_shift failed for $base_date offset $offset"; return 1; }

    local year="${target_date:0:4}"
    local month="${target_date:5:2}"

    # F01 monthly multi-variable file (t2m, msl, u10, v10, tp in one file)
    local DATA_FILE="${F01_DATA_DIR}/${year}${month}.nc"
    local TOPO="${F01_INVARIANT}/geopotential.nc"

    if [ ! -f "$DATA_FILE" ]; then
        echo "  [SKIP] ${period}_${idx}_${offset}: data file not found for ${target_date}: ${DATA_FILE}" && return 1
    fi

    local T2M="$DATA_FILE"
    local MSLP="$DATA_FILE"
    local UWIND="$DATA_FILE"
    local VWIND="$DATA_FILE"
    [ ! -f "$TOPO" ] && TOPO="NONE"

    # Create filename using analogue index and offset (offset written as e.g. -7, 0, 7)
    # Use zero-padded index to keep sort order (3 digits)
    printf -v idx_padded "%03d" "$idx"
    # Alphabetical prefix for file-system sort order:
    # a-7, b-6, c-5, d-4, e-3, f-2, g-1, h0, i1, j2, k3, l4, m5, n6, o7
    offset_str=$(offset_sortable "$offset" "$WINDOW_DAYS")

    local output_basename="Tsurf_${period}_${idx_padded}_${offset_str}.png"
    local output="${OUTPUT_DIR}/${output_basename}"

    echo "  ${period}_${idx_padded}_${offset_str}: ${target_date} (base ${base_date}, offset ${offset})"

    # Run GrADS from OUTPUT_DIR with only the basename so printim writes directly
    # into daily_snapshots/. GASCRP lets plot_Tsurf.gs find colors.gs.
    # Ensure description is safely quoted for grads call (escape single quotes)
    safe_desc="${DESCRIPTION//\'/\'\\\'\'}"
    (cd "$OUTPUT_DIR" && GASCRP="${GRADS_DIR}" $GRADS -blcx "run ${GRADS_DIR}/plot_Tsurf.gs $T2M $MSLP $UWIND $VWIND $TOPO $target_date $LON1 $LON2 $LAT1 $LAT2 $output_basename $period '$safe_desc'") 2>&1 | grep -E "^(Saved|ERROR)" || true

    [ -f "$output" ] && echo "    -> $(basename $output)" || echo "    -> FAILED"
}

# -----------------------------------------------------------------------------
# Generate plots
# -----------------------------------------------------------------------------
echo ""
echo "Plotting target event (index 000)..."
# target uses index 0
for off in $(seq -$WINDOW_DAYS $WINDOW_DAYS); do
    plot_date "$SNAPSHOT" "target" 0 "$off"
done

# link target_000 images to 001, 002, ..., 015 for the analogue comparison
(
 cd ${OUTPUT_DIR}
 for src in Tsurf_target_000_*.png; do
   [ -e "$src" ] || continue
   suffix=${src#Tsurf_target_000_}
   for i in $(seq -f "%03g" 1 15); do
     tgt="Tsurf_target_${i}_${suffix}"
     if [ -e "$tgt" ]; then
       [ -L "$tgt" ] && [ "$(readlink "$tgt")" = "$src" ] && continue
       echo "Skipping existing: $tgt"
       continue
     fi
     ln -s "$src" "$tgt"
   done
 done
)

echo ""
echo "Plotting ${#PAST_DATES[@]} past analogues..."
# enumerate past analogues starting at 1 (keeps consistent indexing)
i=0
for date in "${PAST_DATES[@]}"; do
    i=$((i+1))
    for off in $(seq -$WINDOW_DAYS $WINDOW_DAYS); do
        plot_date "$date" "past" "$i" "$off"
    done
done

echo ""
echo "Plotting ${#PRESENT_DATES[@]} present analogues..."
j=0
for date in "${PRESENT_DATES[@]}"; do
    j=$((j+1))
    for off in $(seq -$WINDOW_DAYS $WINDOW_DAYS); do
        plot_date "$date" "present" "$j" "$off"
    done
done

# ---------------------------------------------------------------------
# NOTE: Index scatter plots have been moved to Sh/F03_index_scatter.sh
# Run separately or via F03_visualization_slurm.sh (master dispatcher).
# ---------------------------------------------------------------------

echo ""
echo "============================================================"
echo "F03_visualization (Tsurf snapshots) complete."
echo "Files: $(ls "$OUTPUT_DIR"/Tsurf_*.png 2>/dev/null | wc -l)"
echo "============================================================"
======"
