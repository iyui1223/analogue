#!/bin/bash
# =============================================================================
# spaghetti: Z500 Spaghetti Plots for Analogue Members
# =============================================================================
# Generates spaghetti-style Z500 synoptic visualizations using GrADS.
# Overlays contours from the original event (snapshot date) + top 5 past and
# top 5 present analogues, all at day 0 (snapshot date). Full Antarctic
# domain (south of 50°S) for sps projection.
#
# Colour scheme:
#   Original event   : black
#   Past analogues   : dark blue
#   Present analogues: dark red
#
# Usage:
#   ./spaghetti.sh --dataset era5 --event antarctica_peninsula_2020
#
# Reads:
#   - Const/extreme_events.yaml (event definitions)
#   - Data/F02_analogue_search/{dataset}/{event}/analogues.csv
#
# Output:
#   Figs/F03_visualization/{event}/{dataset}/spaghetti_Z500.png
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
N_TOP=5

# Z500 contour levels (gpm) -- two widely-spaced values keep the plot readable
Z500_CLEVS="5000 5400"

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dataset)  DATASET="$2";       shift 2 ;;
        --event)    EVENT="$2";         shift 2 ;;
        --ntop)     N_TOP="$2";         shift 2 ;;
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

# Full Antarctic domain for spaghetti (south of 50°S) when using sps projection
if [ "$PROJECTION" = "sps" ]; then
    LAT1=-90
    LAT2=-50
    LON1=0
    LON2=360
fi

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
echo "spaghetti: Z500 (snapshot date only)"
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
    local outpng="$1"
    local gs_file="$2"

    # Z500 settings
    local ncvar="geopotential"
    local level=500
    local clevs="$Z500_CLEVS"
    local expr="z/9.80665"   # geopotential (m²/s²) → geopotential height (gpm)
    local panel_title="Z500 (gpm) spaghetti"

    # Colour indices: 20=original, 30=past, 40=present (single shade per category)
    local orig_color=20   # black
    local past_color=30   # dark blue
    local pres_color=40   # dark red
    local orig_thick=6
    local memb_thick=4
    local line_style=1

    # --- Start writing the GrADS script ---
    cat > "$gs_file" <<'GSHEADER'
* Auto-generated spaghetti plot script
* Do not edit by hand — regenerated by spaghetti.sh

'reinit'
'set display color white'
'c'

GSHEADER

    # Define custom RGB colours (one per category)
    echo "'set rgb 20 0 0 0'" >> "$gs_file"       # original: black
    echo "'set rgb 30 0 0 180'" >> "$gs_file"     # past: dark blue
    echo "'set rgb 40 180 0 0'" >> "$gs_file"     # present: dark red

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

    # --- Draw original event (snapshot date only) ---
    echo "* ===== Original event =====" >> "$gs_file"
    local target
    target=$(date_shift "$SNAPSHOT" 0) || true
    if [ -n "$target" ]; then
        local fpath
        fpath=$(era5_file "$ncvar" "$target")
        if [ -f "$fpath" ]; then
            local gtime
            gtime=$(grads_time_str "$target")
            emit_draw_block "$fpath" "$gtime" "$orig_color" "$orig_thick" "$line_style" "$expr" "$level"
        else
            echo "  [WARN] Original: $fpath not found"
        fi
    fi

    # --- Draw past analogues (snapshot date only) ---
    echo "* ===== Past analogues =====" >> "$gs_file"
    for ((m=0; m<${#PAST_DATES[@]}; m++)); do
        echo "* --- Past member $((m+1)): ${PAST_DATES[$m]} ---" >> "$gs_file"
        target=$(date_shift "${PAST_DATES[$m]}" 0) || continue
        fpath=$(era5_file "$ncvar" "$target")
        if [ ! -f "$fpath" ]; then
            echo "  [WARN] Past[$((m+1))]: $fpath not found"
            continue
        fi
        gtime=$(grads_time_str "$target")
        emit_draw_block "$fpath" "$gtime" "$past_color" "$memb_thick" "$line_style" "$expr" "$level"
    done

    # --- Draw present analogues (snapshot date only) ---
    echo "* ===== Present analogues =====" >> "$gs_file"
    for ((m=0; m<${#PRESENT_DATES[@]}; m++)); do
        echo "* --- Present member $((m+1)): ${PRESENT_DATES[$m]} ---" >> "$gs_file"
        target=$(date_shift "${PRESENT_DATES[$m]}" 0) || continue
        fpath=$(era5_file "$ncvar" "$target")
        if [ ! -f "$fpath" ]; then
            echo "  [WARN] Present[$((m+1))]: $fpath not found"
            continue
        fi
        gtime=$(grads_time_str "$target")
        emit_draw_block "$fpath" "$gtime" "$pres_color" "$memb_thick" "$line_style" "$expr" "$level"
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
'draw string 5.5 7.95 Snapshot: ${SNAPSHOT}  |  Contours: ${clevs}'

* Legend (colour key)
'set strsiz 0.10'
'set string 1 l 4'
'draw string 0.5 1.35 Black: original  |  Blue: past top-${N_TOP}  |  Red: present top-${N_TOP}  |  Snapshot date only'

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
generate_spaghetti_gs "$Z500_PNG" "$Z500_GS"

cd "$GRADS_DIR"
$GRADS -blcx "run ${Z500_GS}" 2>&1 || echo "[WARN] GrADS Z500 spaghetti may have encountered issues"
[ -f "$Z500_PNG" ] && echo "  -> $(basename $Z500_PNG)" || echo "  -> Z500 spaghetti FAILED"

echo ""
echo "============================================================"
echo "spaghetti complete."
echo "============================================================"
