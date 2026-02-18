#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$(conda info --base)/etc/profile.d/conda.sh"
# Work around openjdk activation script expecting JAVA_* vars.
export JAVA_LD_LIBRARY_PATH="${JAVA_LD_LIBRARY_PATH:-}"
export JAVA_HOME="${JAVA_HOME:-}"
conda activate duetscreen-dock
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

export PATH="$ROOT_DIR/third_party/docking_tools/bin:$PATH"
export DIFFDOCK_DIR="$ROOT_DIR/third_party/DiffDock"
export DIFFDOCK_CONDA_ENV="${DIFFDOCK_ENV:-diffdock}"

PYTHONPATH="$ROOT_DIR/src" python -m duetscreen dock \
  --uniprot Q96SW2 \
  --ligands "$ROOT_DIR/data/results/top_intersection_10000.csv" \
  --smiles-column smiles \
  --pockets 3 \
  --dockers gnina,diffdock \
  --run-mmgbsa \
  --mmgbsa-topk 200 \
  --out-prefix crbn_dock
