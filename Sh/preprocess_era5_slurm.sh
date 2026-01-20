#!/bin/bash
#SBATCH --job-name=preprocess_era5
#SBATCH --output=../Log/preprocess_era5.out
#SBATCH --error=../Log/preprocess_era5.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=01:00:00

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
# Psurf directory contains monthly files with t2m variable
ERA5_PSURF_DIR="${ERA5_PSURF_DIR:-${ERA5_DIR}/Psurf/daily/work}"

# Variable configurations for ERA5
# Using Psurf (surface pressure proxy - actually contains t2m in the files)
declare -A VAR_CONFIG
VAR_CONFIG[psurf]="t2m"       # Variable name inside the NetCDF files

# All variables to process
VARS_ALL="psurf"

# Variables that need anomaly calculation (all for ERA5)
VARS_NEED_ANOMALY="psurf"

# =============================================================================
# TEST MODE: Override year range for small-scale testing
# Comment out these lines to use full year range from env_setting.sh
# =============================================================================
START_YEAR=1940
END_YEAR=1943
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
# python Python/preprocess.py --extract-bbox --dataset era5
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
