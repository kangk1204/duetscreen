#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_merged2.smi}"
PROTEIN_FASTA="${PROTEIN_FASTA:-${ROOT_DIR}/data/targets/crbn.fasta}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/optimal_5layer}"

# Base sizes / branching
L1_REPS="${L1_REPS:-10000}"
PER_PARENT="${PER_PARENT:-10}"

# Bits schedule
BITS_L1="${BITS_L1:-0}"
BITS_L2="${BITS_L2:-10}"
BITS_L3="${BITS_L3:-14}"
BITS_L4="${BITS_L4:-18}"
BITS_L5="${BITS_L5:-22}"

# Pruning fractions after each layer (L1->L2, L2->L3, L3->L4)
P1="${P1:-50}"  # % of L1 reps to keep for L2
P2="${P2:-50}"  # % of L2 reps to keep for L3
P3="${P3:-40}"  # % of L3 reps to keep for L4
STOP_LAYER="${STOP_LAYER:-5}"

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

build_layer() {
  local layer=$1
  local bits=$2
  local reps=$3
  local parent_bits=$4
  local parent_keys=$5
  local out_dir="${OUT_BASE}/layer${layer}"

  if [[ -n "${parent_keys}" ]]; then
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --per-parent "${PER_PARENT}" --parent-bits "${parent_bits}" \
      --parent-keys "${parent_keys}" \
      --out-dir "${out_dir}"
  else
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --out-dir "${out_dir}"
  fi
}

screen_layer() {
  local layer=$1
  local rep_path="${OUT_BASE}/layer${layer}/rep_smiles.smi"
  local prefix="crbn_opt_layer${layer}"
  ${PYTHON_CMD} -m duetscreen screen \
    --protein "${PROTEIN_FASTA}" \
    --zinc-path "${rep_path}" \
    --out-prefix "${prefix}"
  local nreps
  nreps=$(wc -l < "${rep_path}")
  ${PYTHON_CMD} -m duetscreen aggregate \
    --topk "${nreps}" --per-model-k "${nreps}" --prefix "${prefix}"
}

select_keys() {
  local layer=$1
  local percent=$2
  local rep_meta="${OUT_BASE}/layer${layer}/rep_meta.tsv"
  local prefix="crbn_opt_layer${layer}"
  local scores="${ROOT_DIR}/data/results/${prefix}_top_intersection_$(wc -l < ${OUT_BASE}/layer${layer}/rep_smiles.smi).csv"
  local out_keys="${OUT_BASE}/layer${layer}/selected_keys.txt"
  ${PYTHON_CMD} -m duetscreen hierarchy-select \
    --scores "${scores}" \
    --rep-meta "${rep_meta}" \
    --out-keys "${out_keys}" \
    --top-percent "${percent}" --score-column avg_rank --ascending
  echo "${out_keys}"
}

echo "[optimal] L1 build (10k reps)"
build_layer 1 "${BITS_L1}" "${L1_REPS}" "" ""
screen_layer 1
L1_KEYS=$(select_keys 1 "${P1}")

L1_PARENTS=$(wc -l < "${L1_KEYS}")
L2_REPS=$((L1_PARENTS * PER_PARENT))
echo "[optimal] L2 build: parents=${L1_PARENTS} reps=${L2_REPS}"
build_layer 2 "${BITS_L2}" "${L2_REPS}" "${BITS_L1}" "${L1_KEYS}"
screen_layer 2
L2_KEYS=$(select_keys 2 "${P2}")

L2_PARENTS=$(wc -l < "${L2_KEYS}")
L3_REPS=$((L2_PARENTS * PER_PARENT))
echo "[optimal] L3 build: parents=${L2_PARENTS} reps=${L3_REPS}"
build_layer 3 "${BITS_L3}" "${L3_REPS}" "${BITS_L2}" "${L2_KEYS}"
screen_layer 3
L3_KEYS=$(select_keys 3 "${P3}")

if [[ "${STOP_LAYER}" -le 3 ]]; then
  echo "[optimal] stop at layer ${STOP_LAYER}"
  exit 0
fi

L3_PARENTS=$(wc -l < "${L3_KEYS}")
L4_REPS=$((L3_PARENTS * PER_PARENT))
echo "[optimal] L4 build: parents=${L3_PARENTS} reps=${L4_REPS}"
build_layer 4 "${BITS_L4}" "${L4_REPS}" "${BITS_L3}" "${L3_KEYS}"
screen_layer 4

if [[ "${STOP_LAYER}" -le 4 ]]; then
  echo "[optimal] stop at layer ${STOP_LAYER}"
  exit 0
fi

cut -f2 "${OUT_BASE}/layer4/rep_meta.tsv" | tail -n +2 > "${OUT_BASE}/layer4/all_keys.txt"
L4_PARENTS=$(wc -l < "${OUT_BASE}/layer4/all_keys.txt")
L5_REPS=$((L4_PARENTS * PER_PARENT))
echo "[optimal] L5 build: parents=${L4_PARENTS} reps=${L5_REPS}"
build_layer 5 "${BITS_L5}" "${L5_REPS}" "${BITS_L4}" "${OUT_BASE}/layer4/all_keys.txt"
screen_layer 5

echo "[optimal] done"
