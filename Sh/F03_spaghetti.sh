#!/bin/bash
# =============================================================================
# F03_spaghetti: Z500/T850 Spaghetti Plots for Analogue Members
# =============================================================================
# Generates spaghetti-style synoptic visualizations using GrADS.
# Overlays contours from the original event + top 3 past/present analogues
# for days 0..7 on a single two-panel map (Z500 left, T850 right).
#
# Colour scheme:
#   Original event   : black (day 0) → light gray (day 7)
#   Past analogues    : dark blue → pale blue
#   Present analogues : dark red  → pale red
#
# Usage:
#   ./F03_spaghetti.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/spaghetti_Z500.png
#   Figs/F03_visualization/{event}/{dataset}/spaghetti_T850.png
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
ERA5_DAILY="${ERA5_DIR}/daily"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
GRADS="${GRADS_CMD:-grads}"

# Number of top analogues to display per period (past/present)
N_TOP=3
# Forward window: days 0..FORWARD_DAYS
FORWARD_DAYS=7

# Z500 contour levels (gpm) -- two widely-spaced values keep the plot readable
Z500_CLEVS="5000 5400"
# T850 contour levels (K) -- two widely-spaced values
T850_CLEVS="255 275"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)  DATASET="$2";       shift 2 ;;
        --event)    EVENT="$2";         shift 2 ;;
        --ntop)     N_TOP="$2";         shift 2 ;;
        --forward)  FORWARD_DAYS="$2";  shift 2 ;;
        --help|-h)
            echo "Usage: $0 --dataset DATASET --event EVENT [--ntop N] [--forward DAYS]"
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
DESCRIPTION=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "description:" | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/')
LON1=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_min:" | head -1 | awk '{print $2}')
LON2=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lon_max:" | head -1 | awk '{print $2}')
LAT1=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_min:" | head -1 | awk '{print $2}')
LAT2=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "lat_max:" | head -1 | awk '{print $2}')
PROJECTION=$(grep -A40 "name: \"${EVENT}\"" "$EVENTS_FILE" | grep "projection:" | head -1 | awk '{print $2}' | tr -d '"')

if [ -z "$SNAPSHOT" ]; then
    echo "ERROR: Event '$EVENT' not found or missing snapshot_date"
    exit 1
fi

# Default projection for Antarctic events
PROJECTION="${PROJECTION:-sps}"

# -----------------------------------------------------------------------------
# Read analogue dates from CSV (top N_TOP per period)
# -----------------------------------------------------------------------------
ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${EVENT}/analogues.csv"

if [ ! -f "$ANALOGUES_FILE" ]; then
    echo "ERROR: Analogues file not found: $ANALOGUES_FILE"
    echo "Please run F02_analogue_search first."
    exit 1
fi

mapfile -t PAST_DATES   < <(awk -F',' 'NR>1 && $7=="past"    {print $1}' "$ANALOGUES_FILE" | head -n "$N_TOP")
mapfile -t PRESENT_DATES < <(awk -F',' 'NR>1 && $7=="present" {print $1}' "$ANALOGUES_FILE" | head -n "$N_TOP")

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
OUTPUT_DIR="${FIGS_BASE}/${EVENT}/${DATASET}"
WORK_DIR="${ROOT_DIR}/Work/F03_spaghetti"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

echo "============================================================"
echo "F03_spaghetti: Z500/T850 Spaghetti Plots"
echo "============================================================"
echo "Event:       $EVENT"
echo "Description: $DESCRIPTION"
echo "Snapshot:    $SNAPSHOT"
echo "Region:      lon[$LON1, $LON2] lat[$LAT1, $LAT2]"
echo "Projection:  $PROJECTION"
echo "Output:      $OUTPUT_DIR"
echo "Top N:       $N_TOP per period"
echo "Past dates:  ${PAST_DATES[*]}"
echo "Present dates: ${PRESENT_DATES[*]}"
echo "Forward days:  0..$FORWARD_DAYS"
echo "============================================================"

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
import sys
dt = datetime.fromisoformat('${base}') + timedelta(days=${offset})
print(dt.strftime('%Y-%m-%d'))
"
}

# -----------------------------------------------------------------------------
# grads_time_str: convert YYYY-MM-DD to GrADS time format DDmonYYYY
# -----------------------------------------------------------------------------
grads_time_str() {
    local d="$1"
    local year="${d:0:4}"
    local month="${d:5:2}"
    local day="${d:8:2}"
    local mon_names=("jan" "feb" "mar" "apr" "may" "jun"
                     "jul" "aug" "sep" "oct" "nov" "dec")
    # Remove leading zero from month for array indexing
    local mon_idx=$((10#$month - 1))
    echo "${day}${mon_names[$mon_idx]}${year}"
}

# -----------------------------------------------------------------------------
# era5_file: return ERA5 monthly file path for a given variable and date
# e.g. era5_file "geopotential" "2020-02-08"
# -----------------------------------------------------------------------------
era5_file() {
    local varname="$1"
    local d="$2"
    local year="${d:0:4}"
    local month="${d:5:2}"
    echo "${ERA5_DAILY}/${varname}/nc/era5_daily_${varname}_${year}_${month}.nc"
}

# =============================================================================
# Generate GrADS script for spaghetti plot
# =============================================================================
# The shell dynamically writes a .gs file with all sdfopen/draw commands,
# then executes it with grads -blcx.  This avoids the GrADS argument-passing
# limitations when overlaying many members.
# =============================================================================

generate_spaghetti_gs() {
    local vartype="$1"      # "Z500" or "T850"
    local outpng="$2"
    local gs_file="$3"

    # --- Variable-specific settings ---
    local ncvar level clevs expr panel_title
    if [ "$vartype" = "Z500" ]; then
        ncvar="geopotential"
        level=500
        clevs="$Z500_CLEVS"
        expr="z/9.80665"   # geopotential (m²/s²) → geopotential height (gpm)
        panel_title="Z500 (gpm) spaghetti"
    else
        ncvar="temperature"
        level=850
        clevs="$T850_CLEVS"
        expr="t"            # temperature in K
        panel_title="T850 (K) spaghetti"
    fi

    # --- Colour definitions (RGB) ---
    # Original: 8 shades from black (day 0) to light gray (day 7)
    local orig_r=(  0  36  72 108 144 170 195 220)
    local orig_g=(  0  36  72 108 144 170 195 220)
    local orig_b=(  0  36  72 108 144 170 195 220)
    # Past analogues: dark blue (day 0) to pale blue (day 7)
    local past_r=(  0   0  30  60  90 120 150 180)
    local past_g=(  0  30  60  90 120 150 180 210)
    local past_b=(180 200 210 220 230 235 240 245)
    # Present analogues: dark red (day 0) to pale red (day 7)
    local pres_r=(180 200 210 220 230 235 240 245)
    local pres_g=(  0  30  60  90 120 150 180 210)
    local pres_b=(  0   0  30  60  90 120 150 180)

    # Line thickness: thicker at day 0, thinner at day 7
    local orig_thick=(8 7 6 5 4 4 3 3)
    local memb_thick=(5 5 4 4 3 3 2 2)

    # Linestyle: solid=1 for original, dashed=3 for analogues (optional)
    # Keeping all solid as per plan
    local orig_style=(1 1 1 1 1 1 1 1)
    local memb_style=(1 1 1 1 1 1 1 1)

    # --- Start writing the GrADS script ---
    cat > "$gs_file" <<'GSHEADER'
* Auto-generated spaghetti plot script
* Do not edit by hand — regenerated by F03_spaghetti.sh

'reinit'
'set display color white'
'c'

GSHEADER

    # Define custom RGB colours
    local cidx=20
    for d in $(seq 0 $FORWARD_DAYS); do
        echo "'set rgb $((cidx + d)) ${orig_r[$d]} ${orig_g[$d]} ${orig_b[$d]}'" >> "$gs_file"
    done
    cidx=30
    for d in $(seq 0 $FORWARD_DAYS); do
        echo "'set rgb $((cidx + d)) ${past_r[$d]} ${past_g[$d]} ${past_b[$d]}'" >> "$gs_file"
    done
    cidx=40
    for d in $(seq 0 $FORWARD_DAYS); do
        echo "'set rgb $((cidx + d)) ${pres_r[$d]} ${pres_g[$d]} ${pres_b[$d]}'" >> "$gs_file"
    done

    # Projection and region
    cat >> "$gs_file" <<GSREGION

* Set up region and projection
'set lat ${LAT1} ${LAT2}'
'set lon ${LON1} ${LON2}'
GSREGION

    if [ "$PROJECTION" = "sps" ]; then
        echo "'set mproj sps'" >> "$gs_file"
    elif [ "$PROJECTION" = "nps" ]; then
        echo "'set mproj nps'" >> "$gs_file"
    fi

    cat >> "$gs_file" <<'GSPAREA'

'set parea 0.8 10.2 1.8 7.8'
'set gxout contour'
'set clab off'

GSPAREA

    # --- Helper: emit GrADS block to open file, draw contour, close ---
    # After close, GrADS resets dimension settings to defaults, so we
    # re-apply lat/lon/projection within each block for safety.
    emit_draw_block() {
        local fpath="$1"
        local grads_time="$2"
        local color_idx="$3"
        local thick="$4"
        local style="$5"
        local draw_expr="$6"
        local lev="$7"

        # After close, next sdfopen always becomes file 1
        cat >> "$gs_file" <<GSDRAW
'sdfopen ${fpath}'
'set dfile 1'
'set lat ${LAT1} ${LAT2}'
'set lon ${LON1} ${LON2}'
'set time ${grads_time}'
'set lev ${lev}'
'set gxout contour'
'set clevs ${clevs}'
'set ccolor ${color_idx}'
'set cthick ${thick}'
'set cstyle ${style}'
'set clab off'
'd ${draw_expr}'
'close 1'

GSDRAW
    }

    # --- Draw original event (days 0..FORWARD_DAYS) ---
    echo "* ===== Original event =====" >> "$gs_file"
    for d in $(seq 0 "$FORWARD_DAYS"); do
        local target
        target=$(date_shift "$SNAPSHOT" "$d") || continue
        local fpath
        fpath=$(era5_file "$ncvar" "$target")
        if [ ! -f "$fpath" ]; then
            echo "  [WARN] Original day+$d: $fpath not found"
            continue
        fi
        local gtime
        gtime=$(grads_time_str "$target")
        emit_draw_block "$fpath" "$gtime" "$((20 + d))" "${orig_thick[$d]}" "${orig_style[$d]}" "$expr" "$level"
    done

    # --- Draw past analogues ---
    echo "* ===== Past analogues =====" >> "$gs_file"
    for ((m=0; m<${#PAST_DATES[@]}; m++)); do
        echo "* --- Past member $((m+1)): ${PAST_DATES[$m]} ---" >> "$gs_file"
        for d in $(seq 0 "$FORWARD_DAYS"); do
            local target
            target=$(date_shift "${PAST_DATES[$m]}" "$d") || continue
            local fpath
            fpath=$(era5_file "$ncvar" "$target")
            if [ ! -f "$fpath" ]; then
                echo "  [WARN] Past[$((m+1))] day+$d: $fpath not found"
                continue
            fi
            local gtime
            gtime=$(grads_time_str "$target")
            emit_draw_block "$fpath" "$gtime" "$((30 + d))" "${memb_thick[$d]}" "${memb_style[$d]}" "$expr" "$level"
        done
    done

    # --- Draw present analogues ---
    echo "* ===== Present analogues =====" >> "$gs_file"
    for ((m=0; m<${#PRESENT_DATES[@]}; m++)); do
        echo "* --- Present member $((m+1)): ${PRESENT_DATES[$m]} ---" >> "$gs_file"
        for d in $(seq 0 "$FORWARD_DAYS"); do
            local target
            target=$(date_shift "${PRESENT_DATES[$m]}" "$d") || continue
            local fpath
            fpath=$(era5_file "$ncvar" "$target")
            if [ ! -f "$fpath" ]; then
                echo "  [WARN] Present[$((m+1))] day+$d: $fpath not found"
                continue
            fi
            local gtime
            gtime=$(grads_time_str "$target")
            emit_draw_block "$fpath" "$gtime" "$((40 + d))" "${memb_thick[$d]}" "${memb_style[$d]}" "$expr" "$level"
        done
    done

    # --- Map, title, legend, and save ---
    cat >> "$gs_file" <<GSFOOTER

* Map boundaries
'set map 1 1 4'
'draw map'

* Title
'set string 1 c 6'
'set strsiz 0.18'
'draw string 5.5 8.3 ${panel_title}  |  ${DESCRIPTION}'
'set strsiz 0.13'
'draw string 5.5 7.95 Original: ${SNAPSHOT} (day 0-${FORWARD_DAYS})  |  Contours: ${clevs}'

* Legend (colour key)
'set strsiz 0.10'
'set string 1 l 4'
'draw string 0.5 1.35 Black/gray: original  |  Blue: past top-${N_TOP}  |  Red: present top-${N_TOP}  |  Bolder = day 0, paler = day ${FORWARD_DAYS}'

* Save PNG
'printim ${outpng} white x1400 y1000'
'say Saved: ${outpng}'

'quit'
GSFOOTER

    echo "  Generated: $(basename "$gs_file")"
}

# =============================================================================
# Generate and run spaghetti plots
# =============================================================================

echo ""
echo "Generating Z500 spaghetti plot..."
Z500_GS="${WORK_DIR}/spaghetti_Z500_${EVENT}.gs"
Z500_PNG="${OUTPUT_DIR}/spaghetti_Z500.png"
generate_spaghetti_gs "Z500" "$Z500_PNG" "$Z500_GS"

cd "$GRADS_DIR"
$GRADS -blcx "run ${Z500_GS}" 2>&1 || echo "[WARN] GrADS Z500 spaghetti may have encountered issues"
[ -f "$Z500_PNG" ] && echo "  -> $(basename $Z500_PNG)" || echo "  -> Z500 spaghetti FAILED"

echo ""
echo "Generating T850 spaghetti plot..."
T850_GS="${WORK_DIR}/spaghetti_T850_${EVENT}.gs"
T850_PNG="${OUTPUT_DIR}/spaghetti_T850.png"
generate_spaghetti_gs "T850" "$T850_PNG" "$T850_GS"

cd "$GRADS_DIR"
$GRADS -blcx "run ${T850_GS}" 2>&1 || echo "[WARN] GrADS T850 spaghetti may have encountered issues"
[ -f "$T850_PNG" ] && echo "  -> $(basename $T850_PNG)" || echo "  -> T850 spaghetti FAILED"

echo ""
echo "============================================================"
echo "F03_spaghetti complete."
echo "============================================================"
