#!/bin/bash
#SBATCH --job-name=F03_visualization
#SBATCH --output=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.out
#SBATCH --error=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.err
#SBATCH --partition=Short
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00

# =============================================================================
# F03: Visualization Pipeline — Master Dispatcher (SLURM batch version)
# =============================================================================
# Runs all F03 sub-scripts in sequence:
#   1. snapshots.sh        — Tsurf snapshot maps (GrADS)
#   2. Tsurfdiff.sh        — Tsurf difference maps: analogue minus original (GrADS)
#   3. spaghetti.sh        — Z500 spaghetti plots (GrADS)
#   4. t2m_boxplot.sh      — T2m box-and-whisker by lead time (Python)
#   5. cvm_test.sh         — Cramér–von Mises test (past vs present, Python)
#   6. index_scatter.sh    — Climate index scatter plots (Python)
#
# Usage:
#   sbatch F03_visualization_slurm.sh                                  # defaults
#   DATASET=era5 EVENT=antarctica_peninsula_2020 sbatch F03_visualization_slurm.sh
#
# Selective plotting (set SKIP_*=1 to skip a sub-script):
#   SKIP_TSURF=1 sbatch F03_visualization_slurm.sh    # skip Tsurf snapshots
#   SKIP_TSURFDIFF=1 sbatch ...                        # skip Tsurf difference maps
#   SKIP_SPAGHETTI=1 sbatch ...                        # skip spaghetti plots
#   SKIP_BOXPLOT=1 sbatch ...                          # skip T2m box plot
#   SKIP_CVM=1 sbatch ...                              # skip CvM test
#   SKIP_SCATTER=1 sbatch ...                          # skip index scatter
#
# Ensemble size: NTOP=7 (box plot, default 5), NMEMBERS=15 (CvM test, default 15)
# =============================================================================

set -eox

# Use actual lustre path (not symlink) for SLURM compatibility
ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

# override the default paths as those are not visible from CPU nodes

echo "Using GrADS: ${GRADS_CMD}"

# Dataset and event can be overridden via environment variables
DATASET="${DATASET:-era5}"
#EVENT="antarctica_peninsula_2022"
SKIP_TSURF="${SKIP_TSURF:-0}"
SKIP_TSURFDIFF="${SKIP_TSURFDIFF:-0}"
SKIP_SPAGHETTI="${SKIP_SPAGHETTI:-0}"
SKIP_BOXPLOT="${SKIP_BOXPLOT:-0}"
SKIP_CVM="${SKIP_CVM:-0}"
SKIP_SCATTER="${SKIP_SCATTER:-0}"
NTOP="${NTOP:-15}"
NMEMBERS="${NMEMBERS:-15}"

echo "============================================================"
echo "F03: Visualization Pipeline — Master Dispatcher"
echo "============================================================"
echo "Dataset:          $DATASET"
echo "Event:            ${EVENT:-all}"
echo "Skip Tsurf:       $SKIP_TSURF"
echo "Skip Tsurfdiff:   $SKIP_TSURFDIFF"
echo "Skip Spaghetti:   $SKIP_SPAGHETTI"
echo "Skip Boxplot:     $SKIP_BOXPLOT"
echo "Skip CvM:         $SKIP_CVM"
echo "Skip Scatter:     $SKIP_SCATTER"
echo "Boxplot N top:    $NTOP"
echo "CvM N members:    $NMEMBERS"
echo "============================================================"

cd "${ROOT_DIR}/Sh"

# Build common arguments
ARGS=""
[ -n "$DATASET" ] && ARGS="$ARGS --dataset $DATASET"
[ -n "$EVENT" ]   && ARGS="$ARGS --event $EVENT"

ARGS_BOXPLOT="$ARGS --ntop $NTOP"

# -------------------------------------------------------
# 1. Tsurf snapshot maps
# -------------------------------------------------------
if [ "$SKIP_TSURF" != "1" ]; then
    echo ""
    echo ">>> Running snapshots.sh (Tsurf snapshots)..."
    echo "------------------------------------------------------------"
    ./snapshots.sh $ARGS || echo "[WARN] snapshots.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf snapshots (SKIP_TSURF=1)"
fi

# -------------------------------------------------------
# 2. Tsurf difference maps (analogue minus original)
# -------------------------------------------------------
if [ "$SKIP_TSURFDIFF" != "1" ]; then
    echo ""
    echo ">>> Running Tsurfdiff.sh (Tsurf difference maps)..."
    echo "------------------------------------------------------------"
    ./Tsurfdiff.sh $ARGS || echo "[WARN] Tsurfdiff.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf difference maps (SKIP_TSURFDIFF=1)"
fi

# -------------------------------------------------------
# 3. Z500/T850 spaghetti plots
# -------------------------------------------------------
if [ "$SKIP_SPAGHETTI" != "1" ]; then
    echo ""
    echo ">>> Running spaghetti.sh (Z500/T850 spaghetti)..."
    echo "------------------------------------------------------------"
    ./spaghetti.sh $ARGS || echo "[WARN] spaghetti.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping spaghetti plots (SKIP_SPAGHETTI=1)"
fi

# -------------------------------------------------------
# 4. T2m box plot
# -------------------------------------------------------
if [ "$SKIP_BOXPLOT" != "1" ]; then
    echo ""
    echo ">>> Running t2m_boxplot.sh (T2m box-and-whisker)..."
    echo "------------------------------------------------------------"
    ./t2m_boxplot.sh $ARGS_BOXPLOT || echo "[WARN] t2m_boxplot.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping T2m box plot (SKIP_BOXPLOT=1)"
fi

# -------------------------------------------------------
# 5. Cramér–von Mises test (past vs present)
# -------------------------------------------------------
if [ "$SKIP_CVM" != "1" ]; then
    echo ""
    echo ">>> Running cvm_test.sh (CvM past vs present)..."
    echo "------------------------------------------------------------"
    ./cvm_test.sh $ARGS --nmembers $NMEMBERS || echo "[WARN] cvm_test.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping CvM test (SKIP_CVM=1)"
fi

# -------------------------------------------------------
# 6. Climate index scatter plots
# -------------------------------------------------------
if [ "$SKIP_SCATTER" != "1" ]; then
    echo ""
    echo ">>> Running index_scatter.sh (index scatter)..."
    echo "------------------------------------------------------------"
    ./index_scatter.sh $ARGS || echo "[WARN] index_scatter.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping index scatter plots (SKIP_SCATTER=1)"
fi

echo ""
echo "============================================================"
echo "F03 Visualization Pipeline Complete"
echo "============================================================"
