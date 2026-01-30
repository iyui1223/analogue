#!/bin/bash
#SBATCH --job-name=preprocess_era5
#SBATCH --output=../Log/preprocess_era5.out
#SBATCH --error=../Log/preprocess_era5.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=12:00:00

# =============================================================================
# ERA5 Preprocessing Pipeline
# =============================================================================
# This script processes ERA5 reanalysis data:
#   1. Merge monthly ERA5 files -> yearly files
#   2. Compute daily climatology
#   3. Compute anomalies = data - climatology
#   4. Extract event bounding boxes and apply smoothing (via Python)
#
# All steps are conditional: skip if output already exists.
#
# ERA5 file structure: Monthly files with pattern YYYYMM.nc
# Each monthly file contains daily data (valid_time dimension)
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Load environment settings
# -----------------------------------------------------------------------------
ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

# Load CDO module
module load cdo/2.0.5 2>/dev/null || module load cdo 2>/dev/null || echo "Warning: CDO module not found"

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DATASET="era5"
YEARLY_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/yearly"
CLIM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/climatology"
ANOM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/anomaly"

mkdir -p "$YEARLY_DIR" "$CLIM_DIR" "$ANOM_DIR"

# ERA5 data paths
ERA5_PSURF_DIR="${ERA5_DIR}/Psurf/daily"

# -----------------------------------------------------------------------------
# Variable Configuration for ERA5
# -----------------------------------------------------------------------------
# Map variable names to their netCDF variable names
declare -A VAR_CONFIG
VAR_CONFIG[psurf]="sp"  # ERA5 surface pressure

# Variables to process (focus on psurf only for now)
VARS_ALL="psurf"
VARS_NEED_ANOMALY="psurf"

# #region agent log - H1: Verify VAR_CONFIG is properly declared
DEBUG_LOG="/home/yi260/rds/hpc-work/analogue/.cursor/debug.log"
echo "{\"hypothesisId\":\"H1\",\"location\":\"preprocess_era5_slurm.sh:config\",\"message\":\"VAR_CONFIG check\",\"data\":{\"VAR_CONFIG_psurf\":\"${VAR_CONFIG[psurf]}\",\"VARS_ALL\":\"${VARS_ALL}\"},\"timestamp\":$(date +%s)}" >> "$DEBUG_LOG"
# #endregion

# =============================================================================
# Year range from env_setting.sh (1948-2026 for production)
# =============================================================================
START_YEAR=${ERA5_START_YEAR}
# END_YEAR is already set in env_setting.sh
# =============================================================================

echo "============================================================"
echo "ERA5 Preprocessing Pipeline"
echo "============================================================"
echo "Dataset: ERA5"
echo "ROOT_DIR: $ROOT_DIR"
echo "ERA5_PSURF_DIR: $ERA5_PSURF_DIR"
echo "Year range: $START_YEAR - $END_YEAR"
echo "Variables: $VARS_ALL"
echo "============================================================"

# #region agent log - H2: Log ERA5 paths before directory check
echo "{\"hypothesisId\":\"H2\",\"location\":\"preprocess_era5_slurm.sh:paths\",\"message\":\"Path verification\",\"data\":{\"ERA5_DIR\":\"${ERA5_DIR}\",\"ERA5_PSURF_DIR\":\"${ERA5_PSURF_DIR}\",\"DATA_DIR\":\"${DATA_DIR}\",\"YEARLY_DIR\":\"${YEARLY_DIR}\"},\"timestamp\":$(date +%s)}" >> "$DEBUG_LOG"
# #endregion

# Check if ERA5 directory exists
if [ ! -d "$ERA5_PSURF_DIR" ]; then
    echo "ERROR: ERA5 data directory not found: $ERA5_PSURF_DIR"
    echo "Please set ERA5_PSURF_DIR in env_setting.sh or ensure data is downloaded."
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 1: Merge monthly files to yearly files
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 1: Merging monthly files to yearly bundles"
echo "============================================================"

for var in $VARS_ALL; do
    nc_varname="${VAR_CONFIG[$var]}"
    
    # #region agent log - H1: Log VAR_CONFIG lookup result in loop
    echo "{\"hypothesisId\":\"H1\",\"location\":\"preprocess_era5_slurm.sh:step1_loop\",\"message\":\"VAR_CONFIG lookup\",\"data\":{\"var\":\"${var}\",\"nc_varname\":\"${nc_varname}\",\"is_empty\":\"$([ -z \"$nc_varname\" ] && echo 'true' || echo 'false')\"},\"timestamp\":$(date +%s)}" >> "$DEBUG_LOG"
    # #endregion
    
    echo ""
    echo "[${var}] NC variable: ${nc_varname}"
    
    for year in $(seq $START_YEAR $END_YEAR); do
        output_file="${YEARLY_DIR}/${var}_${year}.nc"
        
        # Skip if exists
        if [ -f "$output_file" ] && [ -s "$output_file" ]; then
            echo "[${var}] Year ${year}: exists, skipping"
            continue
        fi
        
        echo "[${var}] Year ${year}: merging monthly files..."
        
        # Find monthly files for this year
        # ERA5 pattern: YYYYMM.nc (e.g., 194001.nc, 194002.nc, ...)
        input_pattern="${ERA5_PSURF_DIR}/${year}??.nc"
        
        # Check if files exist
        file_count=$(ls ${input_pattern} 2>/dev/null | wc -l)
        
        if [ "$file_count" -eq 0 ]; then
            echo "[${var}] Warning: No files found for ${year} with pattern: ${input_pattern}"
            continue
        fi
        
        echo "[${var}] Found ${file_count} monthly files for ${year}"
        
        # Merge monthly files using CDO
        # Each monthly file contains daily data, so mergetime concatenates along time
        cdo -O mergetime ${input_pattern} "${output_file}"
        
        if [ $? -eq 0 ]; then
            # Verify the merged file
            n_timesteps=$(cdo -s ntime "${output_file}" 2>/dev/null || echo "0")
            echo "[${var}] Year ${year}: done -> ${output_file} (${n_timesteps} timesteps)"
        else
            echo "[${var}] Year ${year}: ERROR in mergetime"
            rm -f "${output_file}"
        fi
    done
done

# -----------------------------------------------------------------------------
# Step 2: Compute daily climatology
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 2: Computing daily climatology (366 days)"
echo "============================================================"

for var in $VARS_NEED_ANOMALY; do
    clim_file="${CLIM_DIR}/climatology_${var}.nc"
    
    # Skip if exists
    if [ -f "$clim_file" ] && [ -s "$clim_file" ]; then
        echo "[${var}] Climatology exists, skipping: ${clim_file}"
        continue
    fi
    
    echo "[${var}] Computing climatology..."
    
    # Build list of yearly files
    yearly_files=""
    for year in $(seq $START_YEAR $END_YEAR); do
        yf="${YEARLY_DIR}/${var}_${year}.nc"
        if [ -f "$yf" ]; then
            yearly_files="${yearly_files} ${yf}"
        fi
    done
    
    if [ -z "$yearly_files" ]; then
        echo "[${var}] Error: No yearly files found for climatology!"
        continue
    fi
    
    # Count available years
    n_years=$(echo $yearly_files | wc -w)
    echo "[${var}] Using ${n_years} years for climatology"
    
    # Compute daily climatology using ydaymean
    # ydaymean computes mean for each day-of-year (366 values for leap years)
    cdo -O ydaymean -cat "${yearly_files}" "${clim_file}"
    
    if [ $? -eq 0 ]; then
        echo "[${var}] Climatology done: ${clim_file}"
    else
        echo "[${var}] ERROR computing climatology"
        rm -f "${clim_file}"
    fi
done

# -----------------------------------------------------------------------------
# Step 3: Compute anomalies
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 3: Computing anomalies (data - climatology)"
echo "============================================================"

for var in $VARS_NEED_ANOMALY; do
    clim_file="${CLIM_DIR}/climatology_${var}.nc"
    
    if [ ! -f "$clim_file" ]; then
        echo "[${var}] Error: Climatology file not found: ${clim_file}"
        continue
    fi
    
    for year in $(seq $START_YEAR $END_YEAR); do
        yearly_file="${YEARLY_DIR}/${var}_${year}.nc"
        anom_file="${ANOM_DIR}/anomaly_${var}_${year}.nc"
        
        # Skip if exists
        if [ -f "$anom_file" ] && [ -s "$anom_file" ]; then
            echo "[${var}] Year ${year}: anomaly exists, skipping"
            continue
        fi
        
        if [ ! -f "$yearly_file" ]; then
            echo "[${var}] Year ${year}: yearly file not found, skipping"
            continue
        fi
        
        echo "[${var}] Year ${year}: computing anomaly..."
        
        # Subtract day-of-year climatology from each day
        # ydaysub subtracts the corresponding day-of-year value
        cdo -O ydaysub "${yearly_file}" "${clim_file}" "${anom_file}"
        
        if [ $? -eq 0 ]; then
            echo "[${var}] Year ${year}: done -> ${anom_file}"
        else
            echo "[${var}] Year ${year}: ERROR computing anomaly"
            rm -f "${anom_file}"
        fi
    done
done

# -----------------------------------------------------------------------------
# Step 4: Extract event bounding boxes (via Python)
# -----------------------------------------------------------------------------
# NOTE: Skipping Python step for now during testing
# Uncomment when ready for full processing
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 4: Extracting event bounding boxes (SKIPPED for testing)"
echo "============================================================"
echo "[INFO] Python event bbox extraction is skipped during testing."
echo "[INFO] Uncomment Step 4 in the script when ready for full processing."

# cd "$ROOT_DIR"
# poetry run python3 Python/preprocess.py --extract-bbox --dataset era5
# 
# if [ $? -eq 0 ]; then
#     echo "Event bbox extraction completed successfully"
# else
#     echo "ERROR in event bbox extraction"
#     exit 1
# fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "ERA5 Preprocessing Complete"
echo "============================================================"
echo "Yearly files:    ${YEARLY_DIR}"
echo "Climatology:     ${CLIM_DIR}"
echo "Anomalies:       ${ANOM_DIR}"
echo ""
echo "Output files:"
ls -lh "${YEARLY_DIR}"/*.nc 2>/dev/null || echo "  No yearly files found"
ls -lh "${CLIM_DIR}"/*.nc 2>/dev/null || echo "  No climatology files found"
ls -lh "${ANOM_DIR}"/*.nc 2>/dev/null || echo "  No anomaly files found"
echo "============================================================"

# #region agent log - H3: Log completion status
YEARLY_COUNT=$(ls "${YEARLY_DIR}"/*.nc 2>/dev/null | wc -l)
CLIM_COUNT=$(ls "${CLIM_DIR}"/*.nc 2>/dev/null | wc -l)
ANOM_COUNT=$(ls "${ANOM_DIR}"/*.nc 2>/dev/null | wc -l)
echo "{\"hypothesisId\":\"H3\",\"location\":\"preprocess_era5_slurm.sh:completion\",\"message\":\"Pipeline completed\",\"data\":{\"yearly_files\":${YEARLY_COUNT},\"clim_files\":${CLIM_COUNT},\"anom_files\":${ANOM_COUNT}},\"timestamp\":$(date +%s)}" >> "$DEBUG_LOG"
# #endregion
