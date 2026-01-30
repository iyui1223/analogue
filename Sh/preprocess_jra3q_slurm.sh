#!/bin/bash
#SBATCH --job-name=preprocess_jra3q
#SBATCH --output=../Log/preprocess_jra3q.out
#SBATCH --error=../Log/preprocess_jra3q.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=12:00:00

# =============================================================================
# JRA-3Q Preprocessing Pipeline (TEMPLATE)
# =============================================================================
# This script processes JRA-3Q reanalysis data:
#   1. Merge JRA-3Q files -> yearly files
#   2. Compute daily climatology
#   3. Compute anomalies = data - climatology
#   4. Extract event bounding boxes and apply smoothing (via Python)
#
# NOTE: This is a template script. JRA-3Q data path must be configured
#       in Const/env_setting.sh before running.
#
# JRA-3Q file structure: TBD (adjust patterns when data is available)
# Expected patterns from config:
#   - anl_p125.007_hgt.{date}.nc for geopotential height
#   - anl_surf125.002_prmsl.{date}.nc for mean sea level pressure
#   - anl_surf125.011_tmp.{date}.nc for 2m temperature
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
# Check if JRA-3Q is configured
# -----------------------------------------------------------------------------
if [ "$JRA3Q_DIR" = "/path/to/JRA-3Q" ] || [ -z "$JRA3Q_DIR" ] || [ ! -d "$JRA3Q_DIR" ]; then
    echo "============================================================"
    echo "ERROR: JRA-3Q data directory not configured"
    echo "============================================================"
    echo ""
    echo "Please set JRA3Q_DIR in Const/env_setting.sh to point to"
    echo "your JRA-3Q data directory."
    echo ""
    echo "Current value: ${JRA3Q_DIR:-'not set'}"
    echo ""
    echo "Expected JRA-3Q file patterns (adjust as needed):"
    echo "  - anl_surf125.011_tmp.{date}.nc  (2m temperature)"
    echo "  - anl_surf125.002_prmsl.{date}.nc (mean sea level pressure)"
    echo "  - anl_p125.007_hgt.{date}.nc (geopotential height)"
    echo ""
    echo "============================================================"
    exit 1
fi

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DATASET="jra3q"
YEARLY_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/yearly"
CLIM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/climatology"
ANOM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/anomaly"

mkdir -p "$YEARLY_DIR" "$CLIM_DIR" "$ANOM_DIR"

# Variable configurations for JRA-3Q
# Adjust these patterns based on your actual JRA-3Q data structure
declare -A VAR_CONFIG
declare -A VAR_PATTERN
declare -A VAR_NCNAME

# Surface pressure (mean sea level pressure)
VAR_CONFIG[pres]="prmsl"
VAR_PATTERN[pres]="anl_surf125.002_prmsl"
VAR_NCNAME[pres]="PRMSL_GDS0_MSL"

# 2m temperature
VAR_CONFIG[t2m]="tmp"
VAR_PATTERN[t2m]="anl_surf125.011_tmp"
VAR_NCNAME[t2m]="TMP_GDS0_HTGL"

# All variables to process
VARS_ALL="pres t2m"

# Variables that need anomaly calculation
VARS_NEED_ANOMALY="pres t2m"

# Year range for JRA-3Q (1947-present for JRA-3Q)
START_YEAR="${JRA3Q_START_YEAR:-1958}"
END_YEAR="${JRA3Q_END_YEAR:-2022}"

echo "============================================================"
echo "JRA-3Q Preprocessing Pipeline"
echo "============================================================"
echo "Dataset: JRA-3Q"
echo "ROOT_DIR: $ROOT_DIR"
echo "JRA3Q_DIR: $JRA3Q_DIR"
echo "Year range: $START_YEAR - $END_YEAR"
echo "Variables: $VARS_ALL"
echo "============================================================"

# -----------------------------------------------------------------------------
# Step 1: Merge files to yearly files
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 1: Merging files to yearly bundles"
echo "============================================================"

for var in $VARS_ALL; do
    file_pattern="${VAR_PATTERN[$var]}"
    nc_varname="${VAR_NCNAME[$var]}"
    
    echo ""
    echo "[${var}] File pattern: ${file_pattern}"
    echo "[${var}] NC variable: ${nc_varname}"
    
    for year in $(seq $START_YEAR $END_YEAR); do
        output_file="${YEARLY_DIR}/${var}_${year}.nc"
        
        # Skip if exists
        if [ -f "$output_file" ] && [ -s "$output_file" ]; then
            echo "[${var}] Year ${year}: exists, skipping"
            continue
        fi
        
        echo "[${var}] Year ${year}: merging files..."
        
        # Find files for this year
        # JRA-3Q pattern: ${file_pattern}.YYYYMMDD.nc or similar
        # Adjust this pattern based on actual JRA-3Q file naming
        input_pattern="${JRA3Q_DIR}/${file_pattern}.${year}*.nc"
        
        # Check if files exist
        file_count=$(ls ${input_pattern} 2>/dev/null | wc -l)
        
        if [ "$file_count" -eq 0 ]; then
            echo "[${var}] Warning: No files found for ${year} with pattern: ${input_pattern}"
            # Try alternative patterns
            input_pattern="${JRA3Q_DIR}/${year}/${file_pattern}.*.nc"
            file_count=$(ls ${input_pattern} 2>/dev/null | wc -l)
            
            if [ "$file_count" -eq 0 ]; then
                echo "[${var}] Error: Still no files found. Skipping year ${year}."
                continue
            fi
        fi
        
        echo "[${var}] Found ${file_count} files for ${year}"
        
        # Merge using CDO
        cdo -O mergetime ${input_pattern} "${output_file}"
        
        if [ $? -eq 0 ]; then
            echo "[${var}] Year ${year}: done -> ${output_file}"
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
echo ""
echo "============================================================"
echo "Step 4: Extracting event bounding boxes"
echo "============================================================"

cd "$ROOT_DIR"
poetry run python3 Python/preprocess.py --extract-bbox --dataset jra3q

if [ $? -eq 0 ]; then
    echo "Event bbox extraction completed successfully"
else
    echo "ERROR in event bbox extraction"
    exit 1
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "JRA-3Q Preprocessing Complete"
echo "============================================================"
echo "Yearly files:    ${YEARLY_DIR}"
echo "Climatology:     ${CLIM_DIR}"
echo "Anomalies:       ${ANOM_DIR}"
echo "Event data:      ${DATA_DIR}/F01_preprocess/${DATASET}/events/"
echo "============================================================"
