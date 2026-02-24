#!/bin/bash
#SBATCH -J era5-peninsula-2022
#SBATCH -o era5-peninsula-2022_%j.out
#SBATCH --time=12:00:00
#SBATCH --partition=Long

# === Antarctic Peninsula: DJFM (Dec, Jan, Feb, Mar) for 2022 event + analogues ===
# Shares era5_daily_peninsula with 2020; adds months 12,1,2,3 for analogue dates
OUTPUT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/quick_event_analysis/antarctica_peninsula_2022/dataslices/era5_daily_peninsula"

source /lustre/soge1/projects/andante/cenv1201/venvs/template/bin/activate

# Slurm may set different $HOME; point cdsapi at credentials explicitly
export CDSAPI_RC="/hn01-home/cenv1201/.cdsapirc"

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"

export OUTPUT_DIR="${OUTPUT_DIR}"
export START_YEAR=1947
export MONTHS="12,1,2,3,4"
export DAILY_STAT="daily_maximum"
export TIME_ZONE="utc+00:00"
export FREQUENCY="6_hourly"
export VARIABLES="2m_temperature"
# export FORCE=1

python "/lustre/soge1/projects/andante/cenv1201/proj/quick_event_analysis/antarctica_peninsula_2022/dataslices/download.py"

