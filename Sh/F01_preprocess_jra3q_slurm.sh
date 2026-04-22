#!/bin/bash
#SBATCH --job-name=F01_preprocess_jra3q
#SBATCH --output=../Log/F01_preprocess_jra3q_%j.out
#SBATCH --error=../Log/F01_preprocess_jra3q_%j.err
#SBATCH --partition=Long
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=36:00:00

# =============================================================================
# F01: JRA-3Q preprocessing for analogue search
# =============================================================================
# Source:  /soge-home/data/analysis/jra-q3/anl_surf125/anl_surf125.YYYYMMDDHH
# Output:  Data/F01_preprocess/jra3q/
#   - daily_mean/YYYYMM.nc
#   - yearly/psurf_YYYY.nc
#   - climatology/climatology_psurf.nc
#   - anomaly/anomaly_psurf_YYYY.nc
#
# Usage:
#   sbatch F01_preprocess_jra3q_slurm.sh
#   FORCE=1 sbatch F01_preprocess_jra3q_slurm.sh
#   START_YEAR=1948 END_YEAR=2025 MONTHS="12,1,2,3,4" sbatch F01_preprocess_jra3q_slurm.sh
# =============================================================================

set -euo pipefail

# Do not use SCRIPT_DIR in SLURM jobs. Support submission from Sh/ or project root.
if [ -f ../Const/env_setting.sh ]; then
    source ../Const/env_setting.sh
elif [ -f Const/env_setting.sh ]; then
    source Const/env_setting.sh
else
    echo "ERROR: Could not locate Const/env_setting.sh from current working directory: $(pwd)"
    exit 1
fi

if [ -z "${ROOT_DIR:-}" ]; then
    echo "ERROR: ROOT_DIR is not set after sourcing env_setting.sh"
    exit 1
fi

cd "${ROOT_DIR}"

if ! ensure_poetry_env "${ROOT_DIR}"; then
    echo "ERROR: Failed to prepare Poetry environment."
    exit 1
fi

CMD=(
    run_poetry run python3
    Python/preprocess_jra3q.py
    --start-year "${START_YEAR:-$JRA3Q_START_YEAR}"
    --end-year "${END_YEAR:-$END_YEAR}"
)

if [ -n "${MONTHS:-}" ]; then
    CMD+=(--months "${MONTHS}")
fi

if [ "${FORCE:-0}" = "1" ]; then
    CMD+=(--force)
fi

echo "================================================================"
echo "F01: JRA-3Q preprocess"
echo "================================================================"
echo "ROOT_DIR:     ${ROOT_DIR}"
echo "Source:       ${JRA3Q_DIR}"
echo "Anomaly out:  ${F01_JRA3Q_ANOMALY:-${DATA_DIR}/F01_preprocess/jra3q/anomaly}"
echo "START_YEAR:   ${START_YEAR:-$JRA3Q_START_YEAR}"
echo "END_YEAR:     ${END_YEAR:-$END_YEAR}"
echo "MONTHS:       ${MONTHS:-12,1,2,3,4}"
echo "FORCE:        ${FORCE:-0}"
echo "Command:      ${CMD[*]}"
echo "================================================================"

"${CMD[@]}"

echo ""
echo "================================================================"
echo "F01 JRA-3Q preprocess complete."
echo "================================================================"
