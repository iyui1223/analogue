#!/bin/bash
#SBATCH --job-name=preprocess_mswx
#SBATCH --output=../Log/F01_preprocess_mswx.out
#SBATCH --error=../Log/F01_preprocess_mswx.err
#SBATCH --partition=icelake
#SBATCH --account=CRANMER-SL3-CPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=12:00:00

# =============================================================================
# MSWX Preprocessing Pipeline
# =============================================================================
# This script processes MSWX reanalysis data:
#   1. Merge daily MSWX files -> yearly files (all 4 variables)
#   2. Compute daily climatology (for pres, t2m only)
#   3. Compute anomalies = data - climatology (for pres, t2m only)
#   4. Extract event bounding boxes and apply smoothing (via Python)
#
# All steps are conditional: skip if output already exists.
#
# MSWX file structure: ${MSWX_DIR}/${VAR}/${VAR}_YYYYMMDD.nc
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Load environment settings
# -----------------------------------------------------------------------------
ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

# Load CDO module
module load cdo/2.0.5 2>/dev/null || module load cdo 2>/dev/null || echo "Warning: CDO module not found"

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DATASET="mswx"
YEARLY_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/yearly"
CLIM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/climatology"
ANOM_DIR="${DATA_DIR}/F01_preprocess/${DATASET}/anomaly"

mkdir -p "$YEARLY_DIR" "$CLIM_DIR" "$ANOM_DIR"

# Variable configurations for MSWX
# Format: internal_name -> MSWX prefix
# MSWX files: ${MSWX_DIR}/${prefix}/${prefix}_YYYYMMDD.nc
declare -A VAR_CONFIG
VAR_CONFIG[pres]="Pres"      # MSWX surface pressure
VAR_CONFIG[t2m]="Tair"       # MSWX 2m air temperature  
VAR_CONFIG[precip]="Prec"    # MSWX precipitation
VAR_CONFIG[wind10m]="Wind"   # MSWX 10m wind speed

# All variables to process
VARS_ALL="pres t2m precip wind10m"

# Variables that need anomaly calculation
VARS_NEED_ANOMALY="pres t2m"

# Year range (from env_setting.sh or defaults)
START_YEAR="${START_YEAR:-1979}"
END_YEAR="${END_YEAR:-2022}"

echo "============================================================"
echo "MSWX Preprocessing Pipeline"
echo "============================================================"
echo "Dataset: MSWX"
echo "ROOT_DIR: $ROOT_DIR"
echo "MSWX_DIR: $MSWX_DIR"
echo "Year range: $START_YEAR - $END_YEAR"
echo "Variables: $VARS_ALL"
echo "Anomaly vars: $VARS_NEED_ANOMALY"
echo "============================================================"

# -----------------------------------------------------------------------------
# Step 1: Merge daily files to yearly files (ALL variables)
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Step 1: Merging daily files to yearly bundles"
echo "============================================================"

for var in $VARS_ALL; do
    mswx_prefix="${VAR_CONFIG[$var]}"
    
    echo ""
    echo "[${var}] MSWX prefix: ${mswx_prefix}"
    
    for year in $(seq $START_YEAR $END_YEAR); do
        output_file="${YEARLY_DIR}/${var}_${year}.nc"
        
        # Skip if exists
        if [ -f "$output_file" ] && [ -s "$output_file" ]; then
            echo "[${var}] Year ${year}: exists, skipping"
            continue
        fi
        
        echo "[${var}] Year ${year}: merging daily files..."
        
        # Find daily files for this year
        # Pattern: ${MSWX_DIR}/${mswx_prefix}/${mswx_prefix}_YYYYMMDD.nc
        input_pattern="${MSWX_DIR}/${mswx_prefix}/${mswx_prefix}_${year}????.nc"
        
        # Check if files exist
        file_count=$(ls ${input_pattern} 2>/dev/null | wc -l)
        
        if [ "$file_count" -eq 0 ]; then
            echo "[${var}] Warning: No files found for ${year} with pattern: ${input_pattern}"
            # Try alternative pattern (flat structure)
            input_pattern="${MSWX_DIR}/${mswx_prefix}_${year}????.nc"
            file_count=$(ls ${input_pattern} 2>/dev/null | wc -l)
            
            if [ "$file_count" -eq 0 ]; then
                echo "[${var}] Error: Still no files found. Skipping year ${year}."
                continue
            fi
        fi
        
        echo "[${var}] Found ${file_count} daily files for ${year}"
        
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
# Step 2: Compute daily climatology (anomaly variables only)
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
# Step 3: Compute anomalies (anomaly variables only)
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
echo ""
echo "============================================================"
echo "Step 4: Extracting event bounding boxes"
echo "============================================================"

cd "$ROOT_DIR"
poetry run python3 Python/preprocess.py --extract-bbox --dataset mswx

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
echo "MSWX Preprocessing Complete"
echo "============================================================"
echo "Yearly files:    ${YEARLY_DIR}"
echo "Climatology:     ${CLIM_DIR}"
echo "Anomalies:       ${ANOM_DIR}"
echo "Event data:      ${DATA_DIR}/F01_preprocess/${DATASET}/events/"
echo "============================================================"
