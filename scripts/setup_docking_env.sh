#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-duetscreen-dock}"

if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  conda install -y -n "$ENV_NAME" -c conda-forge \
    openmm \
    openmmforcefields \
    openff-toolkit \
    rdkit \
    pdbfixer \
    cudnn
else
  conda create -y -n "$ENV_NAME" python=3.10
  conda install -y -n "$ENV_NAME" -c conda-forge \
    openmm \
    openmmforcefields \
    openff-toolkit \
    rdkit \
    pdbfixer \
    cudnn
fi

echo "Conda env '$ENV_NAME' created. Activate with: conda activate $ENV_NAME"
