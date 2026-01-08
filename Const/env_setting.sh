#!/bin/bash
# =============================================================================
# Environment settings for Analogue Weather Analysis Pipeline
# User should edit this file to configure paths and resources
# =============================================================================

# -----------------------------------------------------------------------------
# Directory Paths
# -----------------------------------------------------------------------------
export ROOT_DIR="/home/yi260/rds/hpc-work/analogue"
export DATA_DIR="${ROOT_DIR}/Data"
export LOG_DIR="${ROOT_DIR}/Log"

# MSWX data location (daily files: {var}_{YYYYMMDD}.nc)
# TODO: User must set this to actual MSWX data path
export MSWX_DIR="/path/to/mswx/daily"

# -----------------------------------------------------------------------------
# SLURM Configuration
# -----------------------------------------------------------------------------
export SLURM_PARTITION="icelake"
export SLURM_ACCOUNT=""  # Set if required by your HPC
export SLURM_TIME="12:00:00"
export SLURM_MEM="32G"
export SLURM_CPUS="4"

# -----------------------------------------------------------------------------
# Python Environment
# -----------------------------------------------------------------------------
# Conda environment activation command
export PYTHON_ENV_CMD="conda activate base"  # TODO: Change to your env name

# -----------------------------------------------------------------------------
# Data Processing Period
# -----------------------------------------------------------------------------
export START_YEAR=1979
export END_YEAR=2022

# -----------------------------------------------------------------------------
# Variables Configuration
# Variables requiring anomaly calculation (climatology subtracted)
export VARS_ANOMALY="pres t2m"
# Variables kept as raw values (no climatology removal)
export VARS_RAW="precip wind10m"
# All variables (for unified processing)
export VARS_ALL="pres t2m precip wind10m"
