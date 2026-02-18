#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIN_RECALL="${MIN_RECALL:-0.95}"

# Resolve python runner (prefer conda env if available).
PYTHON_CMD="python"
if command -v conda >/dev/null 2>&1; then
  ENV_NAME="${ENV_NAME:-}"
  if [[ -z "${ENV_NAME}" ]]; then
    if conda env list | awk '{print $1}' | grep -qx "dl"; then
      ENV_NAME="dl"
    else
      ENV_NAME="duetscreen"
    fi
  fi
  PYTHON_CMD="conda run -n ${ENV_NAME} python"
fi

# Wait for at least one optimization output.
while [[ ! -f "${ROOT_DIR}/data/results/pruning_optimization.csv" && ! -f "${ROOT_DIR}/data/results/pruning_optimization_ml.csv" ]]; do
  sleep 300
  echo "[select-best] waiting for pruning optimization outputs..."
  done

echo "[select-best] selecting best configurations"
${PYTHON_CMD} "${ROOT_DIR}/scripts/select_best_pruning.py" \
  --min-recall "${MIN_RECALL}" \
  --out-best "${ROOT_DIR}/data/results/pruning_best.csv" \
  --out-pareto "${ROOT_DIR}/data/results/pruning_pareto.csv"

echo "[select-best] done: data/results/pruning_best.csv, data/results/pruning_pareto.csv"
