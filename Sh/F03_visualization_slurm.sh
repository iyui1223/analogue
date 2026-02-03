#!/bin/bash
#SBATCH --job-name=F03_visualization
#SBATCH --output=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.out
#SBATCH --error=/lustre/soge1/projects/andante/cenv1201/proj/analogue/Log/F03_visualization.err
#SBATCH --partition=Short
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=01:00:00

# =============================================================================
# F03: Visualization Pipeline (SLURM batch version)
# =============================================================================

set -eox

# Use actual lustre path (not symlink) for SLURM compatibility
ROOT_DIR="/lustre/soge1/projects/andante/cenv1201/proj/analogue"
source "${ROOT_DIR}/Const/env_setting.sh"

# GrADS is set via GRADS_CMD in env_setting.sh
echo "Using GrADS: ${GRADS_CMD}"

# Dataset and event can be overridden
DATASET="${DATASET:-era5}"
EVENT="${EVENT:-}"

echo "============================================================"
echo "F03: Visualization Pipeline"
echo "============================================================"
echo "Dataset: $DATASET"
echo "Event: ${EVENT:-all}"
echo "============================================================"

# Run the main script
cd "${ROOT_DIR}/Sh"

if [ -n "$EVENT" ]; then
    ./F03_visualization.sh --dataset "$DATASET" --event "$EVENT"
else
    ./F03_visualization.sh --dataset "$DATASET"
fi

echo ""
echo "F03 Visualization Complete"
echo "============================================================"
