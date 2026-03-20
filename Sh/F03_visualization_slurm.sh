#!/bin/bash
#SBATCH --job-name=F03_visualization
#SBATCH --output=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.out
#SBATCH --error=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.err
#SBATCH --partition=Long
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
#   2. Tsurfdiff.sh        — Tsurf difference maps: analogue minus target (GrADS)
#   3. spaghetti.sh        — Z500 spaghetti plots (GrADS)
#   4. t2m_boxplot.sh      — T2m box-and-whisker by lead time (Python)
#   5. cvm_test.sh         — Cramér–von Mises test (past vs present, Python)
#   6. index_scatter.sh    — Climate index scatter plots (Python)
#
# Usage:
#   sbatch F03_visualization_slurm.sh                                  # defaults
#   DATASET=era5 EVENT=antarctica_peninsula_2020 sbatch F03_visualization_slurm.sh
#
# Selective plotting (set DO_*=1 to run a sub-script):
#   DO_TSURF=1 sbatch F03_visualization_slurm.sh      # run Tsurf snapshots
#   DO_TSURFDIFF=1 sbatch ...                          # run Tsurf difference maps
#   DO_SPAGHETTI=1 sbatch ...                          # run spaghetti plots
#   DO_BOXPLOT=1 sbatch ...                            # run T2m box plot (default on)
#   DO_CVM=1 sbatch ...                                # run CvM test (default on)
#   DO_SCATTER=1 sbatch ...                            # run index scatter (default on)
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
DO_TSURF="${DO_TSURF:-0}"
DO_TSURFDIFF="${DO_TSURFDIFF:-0}"
DO_SPAGHETTI="${DO_SPAGHETTI:-0}"
DO_BOXPLOT="${DO_BOXPLOT:-1}"
DO_CVM="${DO_CVM:-1}"
DO_SCATTER="${DO_SCATTER:-0}"
NTOP="${NTOP:-30}"
NMEMBERS="${NMEMBERS:-30}"

echo "============================================================"
echo "F03: Visualization Pipeline — Master Dispatcher"
echo "============================================================"
echo "Dataset:          $DATASET"
echo "Event:            ${EVENT:-all}"
echo "Do Tsurf:         $DO_TSURF"
echo "Do Tsurfdiff:     $DO_TSURFDIFF"
echo "Do Spaghetti:     $DO_SPAGHETTI"
echo "Do Boxplot:       $DO_BOXPLOT"
echo "Do CvM:           $DO_CVM"
echo "Do Scatter:       $DO_SCATTER"
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
if [ "$DO_TSURF" = "1" ]; then
    echo ""
    echo ">>> Running snapshots.sh (Tsurf snapshots)..."
    echo "------------------------------------------------------------"
    ./snapshots.sh $ARGS || echo "[WARN] snapshots.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf snapshots (DO_TSURF=0)"
fi

# -------------------------------------------------------
# 2. Tsurf difference maps (analogue minus target)
# -------------------------------------------------------
if [ "$DO_TSURFDIFF" = "1" ]; then
    echo ""
    echo ">>> Running Tsurfdiff.sh (Tsurf difference maps)..."
    echo "------------------------------------------------------------"
    ./Tsurfdiff.sh $ARGS || echo "[WARN] Tsurfdiff.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping Tsurf difference maps (DO_TSURFDIFF=0)"
fi

# -------------------------------------------------------
# 3. Z500/T850 spaghetti plots
# -------------------------------------------------------
if [ "$DO_SPAGHETTI" = "1" ]; then
    echo ""
    echo ">>> Running spaghetti.sh (Z500/T850 spaghetti)..."
    echo "------------------------------------------------------------"
    ./spaghetti.sh $ARGS || echo "[WARN] spaghetti.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping spaghetti plots (DO_SPAGHETTI=0)"
fi

# -------------------------------------------------------
# 4. T2m box plot
# -------------------------------------------------------
if [ "$DO_BOXPLOT" = "1" ]; then
    echo ""
    echo ">>> Running t2m_boxplot.sh (T2m box-and-whisker)..."
    echo "------------------------------------------------------------"
    ./t2m_boxplot.sh $ARGS_BOXPLOT || echo "[WARN] t2m_boxplot.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping T2m box plot (DO_BOXPLOT=0)"
fi

# -------------------------------------------------------
# 5. Cramér–von Mises test (past vs present)
# -------------------------------------------------------
if [ "$DO_CVM" = "1" ]; then
    echo ""
    echo ">>> Running cvm_test.sh (CvM past vs present)..."
    echo "------------------------------------------------------------"
    ./cvm_test.sh $ARGS --nmembers $NMEMBERS || echo "[WARN] cvm_test.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping CvM test (DO_CVM=0)"
fi

# -------------------------------------------------------
# 6. Climate index scatter plots
# -------------------------------------------------------
if [ "$DO_SCATTER" = "1" ]; then
    echo ""
    echo ">>> Running index_scatter.sh (index scatter)..."
    echo "------------------------------------------------------------"
    ./index_scatter.sh $ARGS || echo "[WARN] index_scatter.sh exited with non-zero status"
else
    echo ""
    echo ">>> Skipping index scatter plots (DO_SCATTER=0)"
fi

echo ""
echo "============================================================"
echo "F03 Visualization Pipeline Complete"
echo "============================================================"
