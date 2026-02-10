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
# ERA5 data on SOGE shared storage
export ERA5_DIR="/lustre/soge1/data/analysis/era5/0.28125x0.28125"
export ERA5_DAILY="${ERA5_DIR}/daily"
export ERA5_INVARIANT="${ERA5_DIR}/invariant"

# GrADS .ctl template directory (for template-based multi-file access)
export GRADS_CTL_DIR="/lustre/soge1/projects/andante/cenv1201/scripts/data_handling/grads_ctl"

# Other reanalysis datasets (TODO: Set when available)
export JRA3Q_DIR="/path/to/JRA-3Q"
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
export INTERMEDIATE_DIR="${ROOT_DIR}/Data/Intermediate"
export PROCESSED_DIR="${ROOT_DIR}/Data/Processed"
export LOG_DIR="${ROOT_DIR}/Log"
export FIGS_DIR="${ROOT_DIR}/Figs"
export RESULTS_DIR="${ROOT_DIR}/Results"

# Create output directories if they don't exist
mkdir -p "${INTERMEDIATE_DIR}"
mkdir -p "${PROCESSED_DIR}"
mkdir -p "${LOG_DIR}"
mkdir -p "${FIGS_DIR}"
mkdir -p "${RESULTS_DIR}"
