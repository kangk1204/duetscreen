#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v conda >/dev/null 2>&1; then
  echo "conda not found. Please install Miniconda/Conda first." >&2
  exit 1
fi

ENV_NAME="${ENV_NAME:-}"
if [[ -z "${ENV_NAME}" ]]; then
  if conda env list | awk '{print $1}' | grep -qx "dl"; then
    ENV_NAME="dl"
  else
    ENV_NAME="duetscreen"
  fi
fi

if ! conda env list | awk '{print $1}' | grep -qx "${ENV_NAME}"; then
  conda create -y -n "${ENV_NAME}" python=3.10
fi

CONDA_RUN=(conda run -n "${ENV_NAME}")

# Ensure pip is available and up to date.
"${CONDA_RUN[@]}" python -m pip install --upgrade pip

# Install PyTorch if missing.
if ! "${CONDA_RUN[@]}" python - <<'PY' >/dev/null 2>&1
import importlib.util
print(bool(importlib.util.find_spec('torch')))
PY
then
  "${CONDA_RUN[@]}" python -m pip install \
    torch==2.6.0+cu124 torchvision==0.21.0+cu124 torchaudio==2.6.0+cu124 \
    --index-url https://download.pytorch.org/whl/cu124
fi

# Core scientific deps.
conda install -y -n "${ENV_NAME}" -c conda-forge rdkit
"${CONDA_RUN[@]}" python -m pip install \
  numpy pandas scikit-learn scipy tqdm pyyaml requests biopython networkx \
  yacs prettytable subword-nmt

# DGL + DGL-Life for DrugBAN.
"${CONDA_RUN[@]}" python -m pip install dgl -f https://data.dgl.ai/wheels/cu121/repo.html
"${CONDA_RUN[@]}" python -m pip install dgllife

# PyG stack for GraphDTA.
"${CONDA_RUN[@]}" python -m pip install \
  pyg-lib torch-scatter torch-sparse torch-cluster torch-spline-conv \
  -f https://data.pyg.org/whl/torch-2.6.0+cu124.html
"${CONDA_RUN[@]}" python -m pip install torch-geometric

# Clone third-party repos if missing.
mkdir -p "${ROOT_DIR}/third_party"
if [[ ! -d "${ROOT_DIR}/third_party/MolTrans" ]]; then
  git clone https://github.com/kexinhuang12345/MolTrans.git "${ROOT_DIR}/third_party/MolTrans"
fi
if [[ ! -d "${ROOT_DIR}/third_party/DrugBAN" ]]; then
  git clone https://github.com/peizhenbai/DrugBAN.git "${ROOT_DIR}/third_party/DrugBAN"
fi
if [[ ! -d "${ROOT_DIR}/third_party/GraphDTA" ]]; then
  git clone https://github.com/thinng/GraphDTA.git "${ROOT_DIR}/third_party/GraphDTA"
fi

# Install this repo as an editable package.
"${CONDA_RUN[@]}" python -m pip install -e "${ROOT_DIR}"

echo "Setup complete. Using conda env: ${ENV_NAME}"
