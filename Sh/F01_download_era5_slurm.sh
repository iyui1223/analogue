#!/bin/bash
# DEPRECATED: Use Sh/F01_preprocess_era5_slurm.sh instead.
# This script downloaded global ERA5 from CDS. The new pipeline
# sources from heavy/ and domain-slices locally.
#SBATCH --job-name=F01_download_era5
#SBATCH --output=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F01_download_era5_%j.out
#SBATCH --error=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F01_download_era5_%j.err
#SBATCH --partition=Long
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=72:00:00

# =============================================================================
# F01: ERA5 Daily-Mean Slice Download
# =============================================================================
# Downloads pre-sliced ERA5 daily statistics from CDS (ECMWF) to monthly files.
# Variables: msl, t2m, d2m, sst, u10, v10 (daily_mean) + tp (daily_sum)
# Output: Data/F01_preprocess/era5/slices/YYYYMM.nc
#
# Keep Data/data_slice/ (daily max t2m) unchanged - used by box plot.
#
# Usage:
#   sbatch F01_download_era5_slurm.sh
#   START_YEAR=1950 END_YEAR=2020 sbatch F01_download_era5_slurm.sh
#   MONTHS="1,2,3,4,12" sbatch F01_download_era5_slurm.sh  # DJFM only
#
# Env: START_YEAR, END_YEAR, MONTHS, OUTPUT_DIR, FORCE, CDSAPI_RC
# =============================================================================

set -e

ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

OUTPUT_DIR="${F01_ERA5_SLICES:-${DATA_DIR}/F01_preprocess/era5}"
mkdir -p "$OUTPUT_DIR"

export CDSAPI_RC="/hn01-home/cenv1201/.cdsapirc"

cd "$ROOT_DIR"

echo "============================================================"
echo "F01: ERA5 Daily-Mean Slice Download"
echo "============================================================"
echo "Output:      $OUTPUT_DIR"
echo "START_YEAR:  ${START_YEAR:-$ERA5_START_YEAR}"
echo "END_YEAR:    ${END_YEAR:-$END_YEAR}"
echo "MONTHS:      ${MONTHS:-all}"
echo "============================================================"

export OUTPUT_DIR
export START_YEAR="${START_YEAR:-$ERA5_START_YEAR}"
export END_YEAR="${END_YEAR:-$END_YEAR}"

python3 "${ROOT_DIR}/Python/download_era5_slices.py"

echo ""
echo "============================================================"
echo "F01 download complete. Files: $(ls "${OUTPUT_DIR}"/*.nc 2>/dev/null | wc -l)"
echo "============================================================"
