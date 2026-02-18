#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_merged2.smi}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/ideal_5layer}"
PROTEIN_FASTA="${PROTEIN_FASTA:-${ROOT_DIR}/data/targets/crbn.fasta}"

# Layer sizes (10k -> 100k -> 1M)
L1_REPS="${L1_REPS:-10000}"
PER_PARENT="${PER_PARENT:-10}"

# Bits schedule for 3 layers
BITS_L1="${BITS_L1:-0}"
BITS_L2="${BITS_L2:-10}"
BITS_L3="${BITS_L3:-14}"

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

echo "[layered] L1 build: ${L1_REPS} reps"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 1 --bits "${BITS_L1}" --rep-target "${L1_REPS}" \
  --out-dir "${OUT_BASE}/layer1"

cut -f2 "${OUT_BASE}/layer1/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer1/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer1/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[layered] L2 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 2 --bits "${BITS_L2}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L1}" \
  --parent-keys "${OUT_BASE}/layer1/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer2"

cut -f2 "${OUT_BASE}/layer2/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer2/all_keys.txt"

parents=$(wc -l < "${OUT_BASE}/layer2/all_keys.txt")
rep_target=$((parents * PER_PARENT))
echo "[layered] L3 build: parents=${parents} per_parent=${PER_PARENT} reps~${rep_target}"
${PYTHON_CMD} -m duetscreen hierarchy-build \
  --smiles-path "${SMILES_PATH}" \
  --layer 3 --bits "${BITS_L3}" --rep-target "${rep_target}" \
  --per-parent "${PER_PARENT}" --parent-bits "${BITS_L2}" \
  --parent-keys "${OUT_BASE}/layer2/all_keys.txt" \
  --out-dir "${OUT_BASE}/layer3"

for layer in 1 2 3; do
  rep_path="${OUT_BASE}/layer${layer}/rep_smiles.smi"
  prefix="crbn_layer${layer}"
  echo "[layered] screen ${rep_path} -> ${prefix}"
  ${PYTHON_CMD} -m duetscreen screen \
    --protein "${PROTEIN_FASTA}" \
    --zinc-path "${rep_path}" \
    --out-prefix "${prefix}"
  nreps=$(wc -l < "${rep_path}")
  echo "[layered] aggregate ${prefix} topk=${nreps}"
  ${PYTHON_CMD} -m duetscreen aggregate \
    --topk "${nreps}" --per-model-k "${nreps}" --prefix "${prefix}"
done

echo "[layered] done"
