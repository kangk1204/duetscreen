#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_100k.smi}"
PROTEIN_FASTA="${PROTEIN_FASTA:-${ROOT_DIR}/data/targets/crbn.fasta}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/100k_search}"

SCAFFOLD_MODE="${SCAFFOLD_MODE:-generic}"
P1_LIST="${P1_LIST:-10,20,30,40,50,60,70,80,90,100}"
P2_LIST="${P2_LIST:-10,20,30,40,50,60,70,80,90,100}"
P3_LIST="${P3_LIST:-10,20,30,40,50,60,70,80,90,100}"

BASE_PREFIX="${BASE_PREFIX:-crbn_full_100k}"
BASE_TOPK="${BASE_TOPK:-10000}"

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

if [[ ! -f "${SMILES_PATH}" ]]; then
  echo "[100k] sample file not found: ${SMILES_PATH}"
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/data/results/${BASE_PREFIX}_top_intersection_${BASE_TOPK}.csv" ]]; then
  echo "[100k] full screen baseline"
  ${PYTHON_CMD} -m duetscreen screen \
    --protein "${PROTEIN_FASTA}" \
    --zinc-path "${SMILES_PATH}" \
    --out-prefix "${BASE_PREFIX}"
  ${PYTHON_CMD} -m duetscreen aggregate \
    --topk "${BASE_TOPK}" --per-model-k "${BASE_TOPK}" --prefix "${BASE_PREFIX}"
fi

build_layer() {
  local layer=$1
  local bits=$2
  local reps=$3
  local parent_bits=$4
  local parent_keys=$5
  local out_dir=$6

  if [[ -n "${parent_keys}" ]]; then
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --per-parent "${PER_PARENT}" --parent-bits "${parent_bits}" \
      --parent-keys "${parent_keys}" \
      --scaffold-mode "${SCAFFOLD_MODE}" \
      --out-dir "${out_dir}"
  else
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --scaffold-mode "${SCAFFOLD_MODE}" \
      --out-dir "${out_dir}"
  fi
}

run_opt() {
  local name=$1
  local layers=$2
  local bits1=$3
  local bits2=$4
  local bits3=$5
  local l1=$6
  local per_parent=$7
  local base_dir="${OUT_BASE}/${name}"

  PER_PARENT="${per_parent}"
  mkdir -p "${base_dir}"

  echo "[100k] ${name} build"
  build_layer 1 "${bits1}" "${l1}" "" "" "${base_dir}/layer1"
  cut -f2 "${base_dir}/layer1/rep_meta.tsv" | tail -n +2 > "${base_dir}/layer1/all_keys.txt"
  parents=$(wc -l < "${base_dir}/layer1/all_keys.txt")
  l2=$((parents * per_parent))
  build_layer 2 "${bits2}" "${l2}" "${bits1}" "${base_dir}/layer1/all_keys.txt" "${base_dir}/layer2"

  if [[ "${layers}" -ge 3 ]]; then
    cut -f2 "${base_dir}/layer2/rep_meta.tsv" | tail -n +2 > "${base_dir}/layer2/all_keys.txt"
    parents2=$(wc -l < "${base_dir}/layer2/all_keys.txt")
    l3=$((parents2 * per_parent))
    build_layer 3 "${bits3}" "${l3}" "${bits2}" "${base_dir}/layer2/all_keys.txt" "${base_dir}/layer3"
  fi

  echo "[100k] ${name} optimize"
  local out_csv="${ROOT_DIR}/data/results/100k_layer_search_${name}_strict.csv"
  if [[ "${layers}" -ge 3 ]]; then
    ${PYTHON_CMD} "${ROOT_DIR}/scripts/optimize_pruning_ml.py" \
      --layer-dir "${base_dir}/layer1" \
      --layer-dir "${base_dir}/layer2" \
      --layer-dir "${base_dir}/layer3" \
      --bits "${bits1}" --bits "${bits2}" --bits "${bits3}" \
      --methods avg_rank,avg_score,logreg \
      --gate strict \
      --scaffold-mode "${SCAFFOLD_MODE}" \
      --baseline "${ROOT_DIR}/data/results/${BASE_PREFIX}_top_intersection_${BASE_TOPK}.csv" \
      --baseline-topk "${BASE_TOPK}" \
      --p1 "${P1_LIST}" --p2 "${P2_LIST}" --p3 "${P3_LIST}" \
      --out "${out_csv}"
  else
    ${PYTHON_CMD} "${ROOT_DIR}/scripts/optimize_pruning_ml.py" \
      --layer-dir "${base_dir}/layer1" \
      --layer-dir "${base_dir}/layer2" \
      --bits "${bits1}" --bits "${bits2}" \
      --methods avg_rank,avg_score,logreg \
      --gate strict \
      --scaffold-mode "${SCAFFOLD_MODE}" \
      --baseline "${ROOT_DIR}/data/results/${BASE_PREFIX}_top_intersection_${BASE_TOPK}.csv" \
      --baseline-topk "${BASE_TOPK}" \
      --p1 "${P1_LIST}" --p2 "${P2_LIST}" \
      --out "${out_csv}"
  fi
}

# Configs: (name layers bits1 bits2 bits3 L1 PER_PARENT)
run_opt "L2_10k_pp10" 2 0 10 14 10000 10
run_opt "L2_20k_pp5" 2 0 10 14 20000 5
run_opt "L3_10k_pp10" 3 0 10 14 10000 10
run_opt "L3_20k_pp5" 3 0 10 14 20000 5

echo "[100k] done"
