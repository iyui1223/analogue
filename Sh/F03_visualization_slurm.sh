#!/bin/bash
#SBATCH --job-name=F03_visualization
#SBATCH --output=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.out
#SBATCH --error=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.err
#SBATCH --partition=GPU
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00

# =============================================================================
# F03: Visualization Pipeline — Master Dispatcher (SLURM batch version)
# =============================================================================
# Runs all F03 sub-scripts in sequence:
#   1. F03_snapshots.sh        — Tsurf snapshot maps (GrADS)
#   2. F03_Tsurfdiff.sh        — Tsurf difference maps: analogue minus original (GrADS)
#   3. F03_spaghetti.sh        — Z500/T850 spaghetti plots (GrADS)
#   4. F03_index_scatter.sh    — Climate index scatter plots (Python)
#
# Usage:
#   sbatch F03_visualization_slurm.sh                                  # defaults
#   DATASET=era5 EVENT=antarctica_peninsula_2020 sbatch F03_visualization_slurm.sh
#
# Selective plotting (set SKIP_*=1 to skip a sub-script):
#   SKIP_TSURF=1 sbatch F03_visualization_slurm.sh    # skip Tsurf snapshots
#   SKIP_TSURFDIFF=1 sbatch ...                        # skip Tsurf difference maps
#   SKIP_SPAGHETTI=1 sbatch ...                        # skip spaghetti plots
#   SKIP_SCATTER=1 sbatch ...                           # skip index scatter
# =============================================================================

set -eox

# Use actual lustre path (not symlink) for SLURM compatibility
ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

echo "Using GrADS: ${GRADS_CMD}"

# Dataset and event can be overridden via environment variables
DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"
SKIP_TSURF="${SKIP_TSURF:-0}"
SKIP_TSURFDIFF="${SKIP_TSURFDIFF:-0}"
SKIP_SPAGHETTI="${SKIP_SPAGHETTI:-0}"
SKIP_SCATTER="${SKIP_SCATTER:-0}"

echo "============================================================"
echo "F03: Visualization Pipeline — Master Dispatcher"
echo "============================================================"
echo "Dataset:          $DATASET"
echo "Event:            ${EVENT:-all}"
echo "Skip Tsurf:       $SKIP_TSURF"
echo "Skip Tsurfdiff:   $SKIP_TSURFDIFF"
echo "Skip Spaghetti:   $SKIP_SPAGHETTI"
echo "Skip Scatter:     $SKIP_SCATTER"
echo "============================================================"

cd "${ROOT_DIR}/Sh"

# Build common arguments
ARGS=""
[ -n "$DATASET" ] && ARGS="$ARGS --dataset $DATASET"
[ -n "$EVENT" ]   && ARGS="$ARGS --event $EVENT"

# -------------------------------------------------------
# 1. Tsurf snapshot maps
# -------------------------------------------------------
if [ "$SKIP_TSURF" != "1" ]; then
    echo ""
    echo ">>> Running F03_snapshots.sh (Tsurf snapshots)..."
    echo "------------------------------------------------------------"
    ./F03_snapshots.sh $ARGS || echo "[WARN] F03_snapshots.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf snapshots (SKIP_TSURF=1)"
fi

# -------------------------------------------------------
# 2. Tsurf difference maps (analogue minus original)
# -------------------------------------------------------
if [ "$SKIP_TSURFDIFF" != "1" ]; then
    echo ""
    echo ">>> Running F03_Tsurfdiff.sh (Tsurf difference maps)..."
    echo "------------------------------------------------------------"
    ./F03_Tsurfdiff.sh $ARGS || echo "[WARN] F03_Tsurfdiff.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf difference maps (SKIP_TSURFDIFF=1)"
fi

# -------------------------------------------------------
# 3. Z500/T850 spaghetti plots
# -------------------------------------------------------
if [ "$SKIP_SPAGHETTI" != "1" ]; then
    echo ""
    echo ">>> Running F03_spaghetti.sh (Z500/T850 spaghetti)..."
    echo "------------------------------------------------------------"
    ./F03_spaghetti.sh $ARGS || echo "[WARN] F03_spaghetti.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping spaghetti plots (SKIP_SPAGHETTI=1)"
fi

# -------------------------------------------------------
# 4. Climate index scatter plots
# -------------------------------------------------------
if [ "$SKIP_SCATTER" != "1" ]; then
    echo ""
    echo ">>> Running F03_index_scatter.sh (index scatter)..."
    echo "------------------------------------------------------------"
    ./F03_index_scatter.sh $ARGS || echo "[WARN] F03_index_scatter.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping index scatter plots (SKIP_SCATTER=1)"
fi

echo ""
echo "============================================================"
echo "F03 Visualization Pipeline Complete"
echo "============================================================"
