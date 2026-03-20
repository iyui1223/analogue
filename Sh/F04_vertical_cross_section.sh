#!/bin/bash
# =============================================================================
# Run vertical cross sections for 2015 Mar 24, 2020 Feb 08, 2022 Feb 08 (19 UTC)
# =============================================================================
# Generates ERA5 cross-section figures (T, cloud, EPT, wind, precip) for
# Antarctic Peninsula events. Outputs: PNG (quick check), PDF, EPS (publication).
#
# Usage:
#   ./run_vertical_cross_section.sh
#   source Const/env_setting.sh && ./run_vertical_cross_section.sh
#
# Output:
#   Figs/cross_section/antarctica_peninsula_2015/cross_section_wind3d_height_era5.{png,pdf,eps}
#   Figs/cross_section/antarctica_peninsula_2020/cross_section_wind3d_height_era5.{png,pdf,eps}
#   Figs/cross_section/antarctica_peninsula_2022/cross_section_wind3d_height_era5.{png,pdf,eps}
#
# Requires: Metview >= 5.16.0, ~/.cdsapirc for CDS API
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

source /soge-home/users/cenv1201/miniconda3/etc/profile.d/conda.sh 
conda activate maproom # needs separate environment for Metview

export ROOT_DIR
export FIGS_DIR="${FIGS_DIR:-${ROOT_DIR}/Figs}"
export WORK_DIR="${WORK_DIR:-${ROOT_DIR}/Work}"

PYTHON_DIR="${ROOT_DIR}/Python"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/Log}"
mkdir -p "${LOG_DIR}"

echo "============================================================"
echo "Vertical Cross Section: 2015 Mar 24 & 2020/2022 Feb 08 19 UTC"
echo "============================================================"
echo "ROOT_DIR:   $ROOT_DIR"
echo "FIGS_DIR:   $FIGS_DIR"
echo "WORK_DIR:   $WORK_DIR"
echo "============================================================"

# ---- 2015 event (March warm spell; snapshot_date in extreme_events.yaml) ----
echo ""
echo ">>> Running vertical_cross_section_2015.py ..."
echo "------------------------------------------------------------"
python3 "${PYTHON_DIR}/vertical_cross_section_2015.py" 2>&1 | tee "${LOG_DIR}/cross_section_2015.log"

# ---- 2020 event ----
echo ""
echo ">>> Running vertical_cross_section_2020.py ..."
echo "------------------------------------------------------------"
python3 "${PYTHON_DIR}/vertical_cross_section_2020.py" 2>&1 | tee "${LOG_DIR}/cross_section_2020.log"

# ---- 2022 event ----
echo ""
echo ">>> Running vertical_cross_section_2022.py ..."
echo "------------------------------------------------------------"
python3 "${PYTHON_DIR}/vertical_cross_section_2022.py" 2>&1 | tee "${LOG_DIR}/cross_section_2022.log"

echo ""
echo "============================================================"
echo "Vertical cross sections complete."
echo "Outputs: ${FIGS_DIR}/cross_section/antarctica_peninsula_{2015,2020,2022}/"
echo "============================================================"
