#!/bin/bash
# =============================================================================
# F01: ERA5 Daily-Mean Slice Download (local/interactive)
# =============================================================================
# Calls Python/download_era5_slices.py. For batch, use F01_download_era5_slurm.sh.
#
# Usage:
#   ./F01_download_era5.sh
#   START_YEAR=1950 END_YEAR=2020 ./F01_download_era5.sh
#   MONTHS="1,2,3,4,12" ./F01_download_era5.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

source "${ROOT_DIR}/Const/env_setting.sh"

OUTPUT_DIR="${F01_ERA5_SLICES:-${DATA_DIR}/F01_preprocess/era5/slices}"
mkdir -p "$OUTPUT_DIR"

export OUTPUT_DIR
export START_YEAR="${START_YEAR:-$ERA5_START_YEAR}"
export END_YEAR="${END_YEAR:-$END_YEAR}"

cd "$ROOT_DIR"
python3 "${ROOT_DIR}/Python/download_era5_slices.py"
