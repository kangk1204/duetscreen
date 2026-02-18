#!/usr/bin/env bash
set -euo pipefail

# Ideal 5-layer hierarchy: 10k -> 100k -> 1M -> 10M -> 100M
# Uses fixed branching of 10 children per parent at each layer.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_merged2.smi}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/ideal_5layer}"
PER_PARENT="${PER_PARENT:-10}"

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

# Bits schedule (increase resolution at deeper layers)
BITS_L1=0
BITS_L2=${BITS_L2:-10}
BITS_L3=${BITS_L3:-14}
BITS_L4=${BITS_L4:-18}
BITS_L5=${BITS_L5:-22}

mkdir -p "${OUT_BASE}"

echo "[hierarchy] L1 build: 10k reps"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 1 --bits "${BITS_L1}" --rep-target 10000 \
  --out-dir "${OUT_BASE}/layer1"

cut -f2 "${OUT_BASE}/layer1/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer1/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer1/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[hierarchy] L2 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 2 --bits "${BITS_L2}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L1}" \
  --parent-keys "${OUT_BASE}/layer1/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer2"

cut -f2 "${OUT_BASE}/layer2/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer2/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer2/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[hierarchy] L3 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 3 --bits "${BITS_L3}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L2}" \
  --parent-keys "${OUT_BASE}/layer2/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer3"

cut -f2 "${OUT_BASE}/layer3/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer3/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer3/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[hierarchy] L4 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 4 --bits "${BITS_L4}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L3}" \
  --parent-keys "${OUT_BASE}/layer3/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer4"

cut -f2 "${OUT_BASE}/layer4/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer4/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer4/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[hierarchy] L5 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 5 --bits "${BITS_L5}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L4}" \
  --parent-keys "${OUT_BASE}/layer4/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer5"

echo "[hierarchy] done: ${OUT_BASE}"
