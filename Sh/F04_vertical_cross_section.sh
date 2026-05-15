#!/bin/bash
# =============================================================================
# F04: Vertical cross sections
# =============================================================================
# 1) Keep existing curated event scripts untouched (optional execution).
# 2) Generate additional generic 2015-style cross sections for top-N analogues
#    (past + present) from F02 outputs.
#
# Usage:
#   ./Sh/F04_vertical_cross_section.sh
#   ./Sh/F04_vertical_cross_section.sh --dataset jra3q --ntop 30
#   ./Sh/F04_vertical_cross_section.sh --dataset jra3q --event antarctica_peninsula_2020 --ntop 30
#   ./Sh/F04_vertical_cross_section.sh --dataset jra3q --event antarctica_peninsula_2020 --with-curated
#
# Optional environment variables:
#   DATASET=era5|jra3q|mswx  (default: era5)
#   EVENT=<event_name>       (default: all events in extreme_events.yaml)
#   NTOP=30                  (default: 30)
#   TIME_UTC=19:00           (default: 19:00)
#   USE_CDS=true|false       (default: true)
#   FORCE=false|true         (default: false; rerun plots even if PNG exists)
#   RUN_CURATED=false|true   (default: false; run tuned per-event scripts)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

# F04 intentionally stays OUTSIDE the project Poetry env (F01-F03).
# Some conda activate hooks reference unset vars; relax nounset only here.
set +u
source /soge-home/users/cenv1201/miniconda3/etc/profile.d/conda.sh
conda activate maproom
set -u

# Pin the interpreter to the active conda env to avoid PATH/hash drift.
PYTHON_CMD="${CONDA_PREFIX:-}/bin/python"
if [[ -z "${PYTHON_CMD}" || ! -x "${PYTHON_CMD}" ]]; then
    PYTHON_CMD="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON_CMD}" || ! -x "${PYTHON_CMD}" ]]; then
    echo "ERROR: Could not resolve a Python interpreter for F04."
    exit 1
fi
if ! "${PYTHON_CMD}" -c "import metview" >/dev/null 2>&1; then
    echo "ERROR: Selected Python cannot import metview."
    echo "Python: ${PYTHON_CMD}"
    "${PYTHON_CMD}" -c "import sys; print('sys.executable =', sys.executable)" || true
    echo "Please verify the maproom conda env has metview installed."
    exit 1
fi

export ROOT_DIR
export FIGS_DIR="${FIGS_DIR:-${ROOT_DIR}/Figs}"
export WORK_DIR="${WORK_DIR:-${ROOT_DIR}/Work}"

PYTHON_DIR="${ROOT_DIR}/Python"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/Log}"
mkdir -p "${LOG_DIR}"

DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
NTOP="${NTOP:-30}"
TIME_UTC="${TIME_UTC:-19:00}"
USE_CDS="${USE_CDS:-true}"
FORCE="${FORCE:-false}"
RUN_CURATED="${RUN_CURATED:-false}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)
            DATASET="$2"
            shift 2
            ;;
        --event)
            EVENT="$2"
            shift 2
            ;;
        --ntop)
            NTOP="$2"
            shift 2
            ;;
        --time)
            TIME_UTC="$2"
            shift 2
            ;;
        --no-cds)
            USE_CDS="false"
            shift
            ;;
        --force)
            FORCE="true"
            shift
            ;;
        --with-curated)
            RUN_CURATED="true"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--dataset DATASET] [--event EVENT] [--ntop N] [--time HH:MM] [--no-cds] [--force] [--with-curated]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

case "$DATASET" in
    era5|jra3q|mswx)
        ;;
    *)
        echo "ERROR: Invalid dataset '$DATASET'. Must be one of: era5, jra3q, mswx"
        exit 1
        ;;
esac

EVENTS_FILE="${ROOT_DIR}/Const/extreme_events.yaml"
if [[ ! -f "$EVENTS_FILE" ]]; then
    echo "ERROR: Missing events file: $EVENTS_FILE"
    exit 1
fi

mapfile -t ALL_EVENTS < <(awk '$1=="-" && $2=="name:" {gsub(/"/, "", $3); print $3}' "$EVENTS_FILE")

if [[ -n "$EVENT" ]]; then
    EVENTS=("$EVENT")
else
    EVENTS=("${ALL_EVENTS[@]}")
fi

if [[ ${#EVENTS[@]} -eq 0 ]]; then
    echo "ERROR: No events found in $EVENTS_FILE"
    exit 1
fi

get_event_field() {
    local event_name="$1"
    local key="$2"

    awk -v ev="$event_name" -v wanted="$key" '
        $1=="-" && $2=="name:" {
            name=$3
            gsub(/"/, "", name)
            in_event=(name==ev)
            next
        }
        in_event && $1==wanted":" {
            val=$2
            gsub(/"/, "", val)
            print val
            exit
        }
    ' "$EVENTS_FILE"
}

run_curated_event() {
    local event_name="$1"
    local script_name="$2"
    local log_name="$3"

    if [[ -n "$EVENT" && "$EVENT" != "$event_name" ]]; then
        return 0
    fi

    echo ""
    echo ">>> Running curated ${script_name} (unchanged) ..."
    echo "------------------------------------------------------------"
    "${PYTHON_CMD}" "${PYTHON_DIR}/${script_name}" 2>&1 | tee "${LOG_DIR}/${log_name}"
}

normalize_generic_outputs() {
    local event_id="$1"
    local output_base="$2"
    local output_dir="${FIGS_DIR}/cross_section/${event_id}"
    local wanted_png="${output_dir}/${output_base}.png"
    local page1_png="${output_dir}/${output_base}.1.png"

    if [[ -f "$page1_png" ]]; then
        mv -f "$page1_png" "$wanted_png"
    fi

    rm -f \
        "${output_dir}/${output_base}.pdf" \
        "${output_dir}/${output_base}.1.pdf" \
        "${output_dir}/${output_base}.eps" \
        "${output_dir}/${output_base}.1.eps"
}

run_generic_plot() {
    local date_str="$1"
    local event_id="$2"
    local output_base="$3"
    local log_file="$4"

    local output_png="${FIGS_DIR}/cross_section/${event_id}/${output_base}.png"

    if [[ "$FORCE" != "true" && ( -f "$output_png" || -f "${FIGS_DIR}/cross_section/${event_id}/${output_base}.1.png" ) ]]; then
        normalize_generic_outputs "$event_id" "$output_base"
        echo "  [SKIP] $(basename "$output_png")"
        return 0
    fi

    if CROSS_DATE="$date_str" \
       CROSS_TIME="$TIME_UTC" \
       CROSS_USE_CDS="$USE_CDS" \
       CROSS_EVENT_ID="$event_id" \
       CROSS_OUTPUT_BASE="$output_base" \
       CROSS_WIND_ARROW_UNIT_VELOCITY="90" \
       CROSS_PRECIP_YMAX="4.0" \
       "${PYTHON_CMD}" "${PYTHON_DIR}/vertical_cross_section_generic.py" >>"$log_file" 2>&1; then
        normalize_generic_outputs "$event_id" "$output_base"
        echo "  [OK]   $(basename "$output_png")"
        return 0
    fi

    echo "  [FAIL] ${event_id}/${output_base}"
    return 1
}

echo "============================================================"
echo "F04: Vertical Cross Section"
echo "============================================================"
echo "Dataset for analogue expansion: $DATASET"
echo "Events: ${EVENTS[*]}"
echo "Top N analogues per period: $NTOP"
echo "Analogue plot time (UTC): $TIME_UTC"
echo "Use CDS retrieval: $USE_CDS"
echo "Force rerun existing generic plots: $FORCE"
echo "Run curated scripts: $RUN_CURATED"
echo "Python interpreter: $PYTHON_CMD"
echo "============================================================"

# -----------------------------------------------------------------------------
# 1) Keep current curated images intact
# -----------------------------------------------------------------------------
if [[ "$RUN_CURATED" == "true" ]]; then
    run_curated_event "antarctica_peninsula_2015" "vertical_cross_section_2015.py" "cross_section_2015.log"
    run_curated_event "antarctica_peninsula_2020" "vertical_cross_section_2020.py" "cross_section_2020.log"
    run_curated_event "antarctica_peninsula_2022" "vertical_cross_section_2022.py" "cross_section_2022.log"
else
    echo ""
    echo "Skipping curated scripts (RUN_CURATED=false). Existing tuned images remain untouched."
fi

# -----------------------------------------------------------------------------
# 2) New generic (2015-style) analogue plots
# -----------------------------------------------------------------------------

global_success=0
global_fail=0

echo ""
echo "============================================================"
echo "Generating additional analogue plots (2015 style)"
echo "============================================================"

for evt in "${EVENTS[@]}"; do
    SNAPSHOT_DATE="$(get_event_field "$evt" "snapshot_date")"
    if [[ -z "$SNAPSHOT_DATE" ]]; then
        echo ""
        echo "[SKIP] $evt has no snapshot_date in $EVENTS_FILE"
        continue
    fi

    ANALOGUES_FILE="${ROOT_DIR}/Data/F02_analogue_search/${DATASET}/${evt}/analogues.csv"
    if [[ ! -f "$ANALOGUES_FILE" ]]; then
        echo ""
        echo "[SKIP] Analogues file not found for $evt: $ANALOGUES_FILE"
        continue
    fi

    EVENT_LOG="${LOG_DIR}/cross_section_analogues_${DATASET}_${evt}.log"
    {
        echo "============================================================"
        echo "Event: $evt"
        echo "Snapshot: $SNAPSHOT_DATE"
        echo "Dataset: $DATASET"
        echo "Top N: $NTOP"
        echo "Time: $TIME_UTC"
        echo "============================================================"
    } > "$EVENT_LOG"

    mapfile -t PAST_ROWS < <(
        awk -F',' -v n="$NTOP" 'NR>1 && $7=="past" && ($6+0)<=n {print $1 "," $6}' "$ANALOGUES_FILE" | sort -t',' -k2,2n
    )
    mapfile -t PRESENT_ROWS < <(
        awk -F',' -v n="$NTOP" 'NR>1 && $7=="present" && ($6+0)<=n {print $1 "," $6}' "$ANALOGUES_FILE" | sort -t',' -k2,2n
    )

    echo ""
    echo "Event: $evt"
    echo "  Analogues selected: ${#PAST_ROWS[@]} past, ${#PRESENT_ROWS[@]} present"

    event_success=0
    event_fail=0
    generic_event_id="${evt}/analogues_2015style/${DATASET}"

    for row in "${PAST_ROWS[@]}"; do
        analogue_dt="${row%,*}"
        rank="${row##*,}"
        analogue_date="${analogue_dt%% *}"
        printf -v rank_padded "%02d" "$rank"
        output_base="cross_section_past_rank${rank_padded}"

        if run_generic_plot "$analogue_date" "$generic_event_id" "$output_base" "$EVENT_LOG"; then
            event_success=$((event_success + 1))
            global_success=$((global_success + 1))
        else
            event_fail=$((event_fail + 1))
            global_fail=$((global_fail + 1))
        fi
    done

    for row in "${PRESENT_ROWS[@]}"; do
        analogue_dt="${row%,*}"
        rank="${row##*,}"
        analogue_date="${analogue_dt%% *}"
        printf -v rank_padded "%02d" "$rank"
        output_base="cross_section_present_rank${rank_padded}"

        if run_generic_plot "$analogue_date" "$generic_event_id" "$output_base" "$EVENT_LOG"; then
            event_success=$((event_success + 1))
            global_success=$((global_success + 1))
        else
            event_fail=$((event_fail + 1))
            global_fail=$((global_fail + 1))
        fi
    done

    echo "  Generic analogue plots done for $evt: success=${event_success}, fail=${event_fail}"
    echo "  Output directory: ${FIGS_DIR}/cross_section/${generic_event_id}"
    echo "  Log file: $EVENT_LOG"
done

echo ""
echo "============================================================"
echo "F04 complete."
echo "Curated outputs (unchanged): ${FIGS_DIR}/cross_section/antarctica_peninsula_{2015,2020,2022}/"
echo "Generic analogue outputs: ${FIGS_DIR}/cross_section/<event>/analogues_2015style/${DATASET}/"
echo "Generic plot summary: success=${global_success}, fail=${global_fail}"
echo "============================================================"
