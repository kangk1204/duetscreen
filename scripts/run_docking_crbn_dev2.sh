#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export CUDA_DEVICE_ORDER=PCI_BUS_ID
export CUDA_VISIBLE_DEVICES=MIG-ddf3ede4-ddbc-5f3a-9330-f15c9d174495
export OPENMM_DEFAULT_PLATFORM=CUDA

source "$(conda info --base)/etc/profile.d/conda.sh"
# Work around openjdk activation script expecting JAVA_LD_LIBRARY_PATH.
export JAVA_LD_LIBRARY_PATH="${JAVA_LD_LIBRARY_PATH:-}"
export JAVA_HOME="${JAVA_HOME:-}"
conda activate duetscreen-dock

export PATH="$ROOT_DIR/third_party/docking_tools/bin:$PATH"
export DIFFDOCK_DIR="$ROOT_DIR/third_party/DiffDock"
export DIFFDOCK_CONDA_ENV="${DIFFDOCK_ENV:-diffdock}"
echo "=== RUN $(date '+%Y-%m-%d %H:%M:%S') ==="

PYTHONPATH="$ROOT_DIR/src" python -m duetscreen dock \
  --uniprot Q96SW2 \
  --ligands "$ROOT_DIR/data/results/top_intersection_10000.csv" \
  --smiles-column smiles \
  --pockets 3 \
  --dockers gnina,diffdock \
  --run-mmgbsa \
  --mmgbsa-topk 200 \
  --out-prefix crbn_dock_dev2
