#!/bin/bash

module purge

# =============================================================================
# Environment settings for Antarctic Circulation Analogue Attribution Study
# =============================================================================

# --------------------- EDIT ---------------------
export ROOT_DIR="/home/yi260/rds/hpc-work/analogue"

# Python environment
source ~/venvs/c1coursework/bin/activate
# Note: Use 'poetry run python3' in scripts, or 'poetry shell' for interactive use

# -----------------------------------------------------------------------------
# Data Source Directories
# -----------------------------------------------------------------------------
export ERA5_DIR="/home/yi260/rds/hpc-work/Download/ERA5"
export ERA5_PSURF_DIR="${ERA5_DIR}/Psurf/daily/work"  # ERA5 surface pressure data
export JRA3Q_DIR="/path/to/JRA-3Q"  # TODO: Set when available
export MSWX_DIR="/home/yi260/rds/hpc-work/Download/MSWX_V100/Past"

# -----------------------------------------------------------------------------
# Year Ranges for Each Dataset
# -----------------------------------------------------------------------------
# MSWX year range
export START_YEAR=1979
export END_YEAR=2022

# ERA5 year range (data available from 1940)
export ERA5_START_YEAR=1940
export ERA5_END_YEAR=2022

# JRA-3Q year range (JRA-3Q available from 1947)
export JRA3Q_START_YEAR=1958
export JRA3Q_END_YEAR=2022

# All variables to process (for MSWX)
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

# Create symlinks to data sources
ln -rsf ${ERA5_DIR} ${ROOT_DIR}/Data/ERA5
