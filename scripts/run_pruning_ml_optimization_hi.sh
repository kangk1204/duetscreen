#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_merged2.smi}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/opt_search}"

L1_REPS="${L1_REPS:-10000}"
PER_PARENT="${PER_PARENT:-10}"

BITS_L1="${BITS_L1:-0}"
BITS_L2="${BITS_L2:-10}"
BITS_L3="${BITS_L3:-14}"

P1_LIST="${P1_LIST:-60,70,80,90,100}"
P2_LIST="${P2_LIST:-60,70,80,90,100}"
P3_LIST="${P3_LIST:-60,70,80,90,100}"

METHODS="${METHODS:-avg_rank,avg_score,logreg}"
GATE="${GATE:-relaxed}"
SCAFFOLD_MODE="${SCAFFOLD_MODE:-generic}"
OUT_FILE="${OUT_FILE:-${ROOT_DIR}/data/results/pruning_optimization_ml_60_100.csv}"
OUT_BEST="${OUT_BEST:-${ROOT_DIR}/data/results/pruning_best_60_100.csv}"
OUT_PARETO="${OUT_PARETO:-${ROOT_DIR}/data/results/pruning_pareto_60_100.csv}"

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

mkdir -p "${OUT_BASE}"

if [[ ! -f "${OUT_BASE}/layer1/rep_meta.tsv" ]]; then
  echo "[ml-opt-hi] build L1 reps"
  ${PYTHON_CMD} -m duetscreen hierarchy-build \
    --smiles-path "${SMILES_PATH}" \
    --layer 1 --bits "${BITS_L1}" --rep-target "${L1_REPS}" \
    --scaffold-mode "${SCAFFOLD_MODE}" \
    --out-dir "${OUT_BASE}/layer1"
fi

cut -f2 "${OUT_BASE}/layer1/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer1/all_keys.txt"
parents=$(wc -l < "${OUT_BASE}/layer1/all_keys.txt")
L2_REPS=$((parents * PER_PARENT))

if [[ ! -f "${OUT_BASE}/layer2/rep_meta.tsv" ]]; then
  echo "[ml-opt-hi] build L2 reps"
  ${PYTHON_CMD} -m duetscreen hierarchy-build \
    --smiles-path "${SMILES_PATH}" \
    --layer 2 --bits "${BITS_L2}" --rep-target "${L2_REPS}" \
    --per-parent "${PER_PARENT}" --parent-bits "${BITS_L1}" \
    --parent-keys "${OUT_BASE}/layer1/all_keys.txt" \
    --scaffold-mode "${SCAFFOLD_MODE}" \
    --out-dir "${OUT_BASE}/layer2"
fi

cut -f2 "${OUT_BASE}/layer2/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer2/all_keys.txt"
parents=$(wc -l < "${OUT_BASE}/layer2/all_keys.txt")
L3_REPS=$((parents * PER_PARENT))

if [[ ! -f "${OUT_BASE}/layer3/rep_meta.tsv" ]]; then
  echo "[ml-opt-hi] build L3 reps"
  ${PYTHON_CMD} -m duetscreen hierarchy-build \
    --smiles-path "${SMILES_PATH}" \
    --layer 3 --bits "${BITS_L3}" --rep-target "${L3_REPS}" \
    --per-parent "${PER_PARENT}" --parent-bits "${BITS_L2}" \
    --parent-keys "${OUT_BASE}/layer2/all_keys.txt" \
    --scaffold-mode "${SCAFFOLD_MODE}" \
    --out-dir "${OUT_BASE}/layer3"
fi

echo "[ml-opt-hi] run ML pruning optimization"
${PYTHON_CMD} "${ROOT_DIR}/scripts/optimize_pruning_ml.py" \
  --layer-dir "${OUT_BASE}/layer1" \
  --layer-dir "${OUT_BASE}/layer2" \
  --layer-dir "${OUT_BASE}/layer3" \
  --bits "${BITS_L1}" --bits "${BITS_L2}" --bits "${BITS_L3}" \
  --methods "${METHODS}" \
  --gate "${GATE}" \
  --scaffold-mode "${SCAFFOLD_MODE}" \
  --p1 "${P1_LIST}" --p2 "${P2_LIST}" --p3 "${P3_LIST}" \
  --out "${OUT_FILE}"

echo "[ml-opt-hi] select best (recall>=0.95)"
${PYTHON_CMD} "${ROOT_DIR}/scripts/select_best_pruning.py" \
  --in "${OUT_FILE}" \
  --min-recall 0.95 \
  --out-best "${OUT_BEST}" \
  --out-pareto "${OUT_PARETO}"

echo "[ml-opt-hi] done"
