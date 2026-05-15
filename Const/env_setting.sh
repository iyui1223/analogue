#!/bin/bash

# =============================================================================
# Environment settings for Antarctic Circulation Analogue Attribution Study
# =============================================================================
# Updated for SOGE environment (cenv1201)
# =============================================================================

# --------------------- EDIT ---------------------
# Use actual lustre path (not symlink) for SLURM compatibility
export ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"

# Base Python/Poetry bootstrap environment.
# Keep this optional so scripts can still run even if starter venv is absent.
if [ -f ~/venvs/starter/bin/activate ]; then
    # shellcheck disable=SC1090
    source ~/venvs/starter/bin/activate
fi

# Use the currently activated environment for poetry commands in SLURM jobs.
# This avoids creating empty per-project venvs on compute nodes.
export POETRY_VIRTUALENVS_CREATE="${POETRY_VIRTUALENVS_CREATE:-false}"
export POETRY_NO_INTERACTION=1

# -----------------------------------------------------------------------------
# Poetry helpers
# -----------------------------------------------------------------------------
# run_poetry <args...>
#   Executes poetry even when the launcher script exists but lacks execute bit.
run_poetry() {
    local poetry_path poetry_shebang poetry_python

    poetry_path="$(command -v poetry 2>/dev/null || true)"
    if [ -z "$poetry_path" ]; then
        echo "ERROR: poetry not found in PATH." >&2
        return 1
    fi

    if [ -x "$poetry_path" ]; then
        "$poetry_path" "$@"
        return $?
    fi

    if [ -r "$poetry_path" ]; then
        poetry_shebang="$(head -n 1 "$poetry_path" 2>/dev/null || true)"
        if [[ "$poetry_shebang" == '#!'* ]]; then
            poetry_python="${poetry_shebang#\#!}"
            if [ -x "$poetry_python" ]; then
                "$poetry_python" "$poetry_path" "$@"
                return $?
            fi
        fi
    fi

    echo "ERROR: poetry launcher exists but is not executable: $poetry_path" >&2
    return 1
}

# ensure_poetry_env [project_dir]
#   Ensures runtime dependencies for F02/F03 are importable.
#   If missing, prints install commands instead of auto-installing during jobs.
ensure_poetry_env() {
    local project_dir
    project_dir="${1:-$ROOT_DIR}"

    if [ ! -f "${project_dir}/pyproject.toml" ]; then
        echo "ERROR: pyproject.toml not found in ${project_dir}" >&2
        return 1
    fi

    if ! (cd "$project_dir" && run_poetry run python3 -c "import numpy, scipy, pandas, xarray, dask, netCDF4, yaml, matplotlib, cdsapi" >/dev/null 2>&1); then
        echo "ERROR: Runtime dependencies are missing in the active Python environment." >&2
        echo "Run once on login node:" >&2
        echo "  cd ${project_dir} && run_poetry install --no-root" >&2
        echo "Note: F04 (vertical cross sections) uses Metview and stays on the" >&2
        echo "      separate conda env 'maproom' -- not covered by this Poetry env." >&2
        return 1
    fi
}

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
export JRA3Q_DIR="/soge-home/data/analysis/jra-q3/anl_surf125"
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
export PENINSULA_DOMAIN_MASKS="${ROOT_DIR}/Const/peninsula_domain_masks.nc"

# Output directories
export DATA_DIR="${ROOT_DIR}/Data"
# F01: Domain-sliced daily MEAN (msl, t2m, u10, v10, tp) for visualization & analogue
export F01_ERA5_DAILY_MEAN="${DATA_DIR}/F01_preprocess/era5/daily_mean"
# JRA-3Q F01 outputs for analogue search compatibility
export F01_JRA3Q_BASE="${DATA_DIR}/F01_preprocess/jra3q"
export F01_JRA3Q_DAILY_MEAN="${F01_JRA3Q_BASE}/daily_mean"
export F01_JRA3Q_YEARLY="${F01_JRA3Q_BASE}/yearly"
export F01_JRA3Q_CLIMATOLOGY="${F01_JRA3Q_BASE}/climatology"
export F01_JRA3Q_ANOMALY="${F01_JRA3Q_BASE}/anomaly"
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
