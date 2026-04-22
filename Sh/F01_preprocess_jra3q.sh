#!/bin/bash
# =============================================================================
# F01: JRA-3Q preprocessing (interactive/local)
# =============================================================================
# Usage:
#   ./F01_preprocess_jra3q.sh
#   FORCE=1 ./F01_preprocess_jra3q.sh
#   START_YEAR=1948 END_YEAR=2025 MONTHS="12,1,2,3,4" ./F01_preprocess_jra3q.sh
# =============================================================================

set -euo pipefail

if [ -f ../Const/env_setting.sh ]; then
    source ../Const/env_setting.sh
elif [ -f Const/env_setting.sh ]; then
    source Const/env_setting.sh
else
    echo "ERROR: Could not locate Const/env_setting.sh from current working directory: $(pwd)"
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

echo "Running: ${CMD[*]}"
"${CMD[@]}"
