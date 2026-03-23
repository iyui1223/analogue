#!/bin/bash

# =============================================================================
# Environment settings for Antarctic Circulation Analogue Attribution Study
# =============================================================================
# Updated for SOGE environment (cenv1201)
# =============================================================================

# --------------------- EDIT ---------------------
# Use actual lustre path (not symlink) for SLURM compatibility
export ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"

# Python environment (update path as needed)
# source ~/venvs/c1coursework/bin/activate

# GrADS executable (globally available on SOGE)
export GRADS_CMD="grads"

# -----------------------------------------------------------------------------
# Data Source Directories (use lustre paths for SLURM compatibility)
# -----------------------------------------------------------------------------
# User-managed ERA5 data store (global monthly files, CDS downloads)
export ERA5_HEAVY_DIR="/lustre/soge1/projects/andante/cenv1201/heavy/ERA5/daily/Surf/slices"

# ERA5 invariant fields (project-local copy)
export F01_ERA5_INVARIANT="${ROOT_DIR}/Data/F01_preprocess/era5/invariant"
export ERA5_LSM_PATH="${F01_ERA5_INVARIANT}/land_sea_mask.nc"

# GrADS .ctl template directory (for template-based multi-file access)
export GRADS_CTL_DIR="/lustre/soge1/projects/andante/cenv1201/scripts/data_handling/grads_ctl"

# JRA-3Q reanalysis (6-hourly GRIB2 surface analysis, 1.25° grid)
export JRA3Q_DIR="/lustre/soge1/data/analysis/jra-q3/anl_surf125"
export MSWX_DIR="/lustre/soge1/data/analysis/gloh2o-mswx"

# -----------------------------------------------------------------------------
# Year Ranges for Each Dataset
# -----------------------------------------------------------------------------
export MSWX_START_YEAR=1979
export ERA5_START_YEAR=1948   # Aligned with JRA3Q availability (Jan 1948 onwards)
export JRA3Q_START_YEAR=1948

export END_YEAR=2026

# -----------------------------------------------------------------------------
# Past / Present Period Separation (for attribution analysis)
# -----------------------------------------------------------------------------
export PAST_START_YEAR=1948
export PAST_END_YEAR=1987
export PRESENT_START_YEAR=1988
export PRESENT_END_YEAR=2026

# All variables to process
export VARS_ALL="pres t2m precip wind10m"

# ------------------------------------------------

# Configuration files (parameters defined in YAML, not here)
export PREPROCESS_CONFIG="${ROOT_DIR}/Const/preprocess_config.yaml"
export ANALOGUE_CONFIG="${ROOT_DIR}/Const/analogue_config.yaml"
export EVENTS_CONFIG="${ROOT_DIR}/Const/extreme_events.yaml"

# Output directories
export DATA_DIR="${ROOT_DIR}/Data"
# F01: Domain-sliced daily MEAN (msl, t2m, u10, v10, tp) for visualization & analogue
export F01_ERA5_DAILY_MEAN="${DATA_DIR}/F01_preprocess/era5/daily_mean"
# ERA5 daily MAXIMUM t2m (domain-sliced, for box plots / CvM test)
export DATA_SLICE_DIR="${DATA_DIR}/F01_preprocess/era5/t2m_daily_max"
# export INTERMEDIATE_DIR="${ROOT_DIR}/Data/Intermediate"
# export PROCESSED_DIR="${ROOT_DIR}/Data/Processed"
export LOG_DIR="${ROOT_DIR}/Log"
export FIGS_DIR="${ROOT_DIR}/Figs"
export RESULTS_DIR="${ROOT_DIR}/Results"

# Create output directories if they don't exist
#mkdir -p "${INTERMEDIATE_DIR}" # not being used anymore
#mkdir -p "${PROCESSED_DIR}" #not being used anymore
mkdir -p "${LOG_DIR}"
mkdir -p "${FIGS_DIR}"
mkdir -p "${RESULTS_DIR}"
