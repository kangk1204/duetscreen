#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMILES_PATH="${SMILES_PATH:-${ROOT_DIR}/data/zinc22/purchasable_druglike_100k.smi}"
PROTEIN_FASTA="${PROTEIN_FASTA:-${ROOT_DIR}/data/targets/crbn.fasta}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/data/hierarchy/100k_search_hi}"

BASE_PREFIX="${BASE_PREFIX:-crbn_full_100k}"
BASE_TOPK="${BASE_TOPK:-10000}"

SCAFFOLD_MODES="${SCAFFOLD_MODES:-generic,none}"
BITS_SETS="${BITS_SETS:-0,6,10;0,8,12}"
L1_REPS_LIST="${L1_REPS_LIST:-20000,50000,100000}"
PER_PARENT_LIST="${PER_PARENT_LIST:-5,10}"
P1_LIST="${P1_LIST:-70,80,90,100}"
P2_LIST="${P2_LIST:-70,80,90,100}"
P3_LIST="${P3_LIST:-70,80,90,100}"
GATE="${GATE:-relaxed}"

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
  echo "[100k-hi] sample file not found: ${SMILES_PATH}"
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/data/results/${BASE_PREFIX}_top_intersection_${BASE_TOPK}.csv" ]]; then
  echo "[100k-hi] full screen baseline"
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
  local scaffold_mode=$7

  if [[ -n "${parent_keys}" ]]; then
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --per-parent "${PER_PARENT}" --parent-bits "${parent_bits}" \
      --parent-keys "${parent_keys}" \
      --scaffold-mode "${scaffold_mode}" \
      --out-dir "${out_dir}"
  else
    ${PYTHON_CMD} -m duetscreen hierarchy-build \
      --smiles-path "${SMILES_PATH}" \
      --layer "${layer}" --bits "${bits}" --rep-target "${reps}" \
      --scaffold-mode "${scaffold_mode}" \
      --out-dir "${out_dir}"
  fi
}

optimize_config() {
  local name=$1
  local bits1=$2
  local bits2=$3
  local bits3=$4
  local l1=$5
  local per_parent=$6
  local scaffold_mode=$7
  local base_dir="${OUT_BASE}/${name}"

  PER_PARENT="${per_parent}"
  mkdir -p "${base_dir}"

  if [[ ! -f "${base_dir}/layer1/rep_meta.tsv" ]]; then
    build_layer 1 "${bits1}" "${l1}" "" "" "${base_dir}/layer1" "${scaffold_mode}"
  fi
  cut -f2 "${base_dir}/layer1/rep_meta.tsv" | tail -n +2 > "${base_dir}/layer1/all_keys.txt"
  parents=$(wc -l < "${base_dir}/layer1/all_keys.txt")
  l2=$((parents * per_parent))
  if [[ ! -f "${base_dir}/layer2/rep_meta.tsv" ]]; then
    build_layer 2 "${bits2}" "${l2}" "${bits1}" "${base_dir}/layer1/all_keys.txt" "${base_dir}/layer2" "${scaffold_mode}"
  fi
  cut -f2 "${base_dir}/layer2/rep_meta.tsv" | tail -n +2 > "${base_dir}/layer2/all_keys.txt"
  parents2=$(wc -l < "${base_dir}/layer2/all_keys.txt")
  l3=$((parents2 * per_parent))
  if [[ ! -f "${base_dir}/layer3/rep_meta.tsv" ]]; then
    build_layer 3 "${bits3}" "${l3}" "${bits2}" "${base_dir}/layer2/all_keys.txt" "${base_dir}/layer3" "${scaffold_mode}"
  fi

  local out_csv="${ROOT_DIR}/data/results/100k_layer_search_hi_${name}_${GATE}.csv"
  ${PYTHON_CMD} "${ROOT_DIR}/scripts/optimize_pruning_ml.py" \
    --layer-dir "${base_dir}/layer1" \
    --layer-dir "${base_dir}/layer2" \
    --layer-dir "${base_dir}/layer3" \
    --bits "${bits1}" --bits "${bits2}" --bits "${bits3}" \
    --methods avg_rank,avg_score,logreg \
    --gate "${GATE}" \
    --scaffold-mode "${scaffold_mode}" \
    --baseline "${ROOT_DIR}/data/results/${BASE_PREFIX}_top_intersection_${BASE_TOPK}.csv" \
    --baseline-topk "${BASE_TOPK}" \
    --p1 "${P1_LIST}" --p2 "${P2_LIST}" --p3 "${P3_LIST}" \
    --out "${out_csv}"
}

IFS=',' read -r -a scaffold_modes <<< "${SCAFFOLD_MODES}"
IFS=';' read -r -a bits_sets <<< "${BITS_SETS}"
IFS=',' read -r -a l1_list <<< "${L1_REPS_LIST}"
IFS=',' read -r -a per_parent_list <<< "${PER_PARENT_LIST}"

for scaffold_mode in "${scaffold_modes[@]}"; do
  for bits_set in "${bits_sets[@]}"; do
    IFS=',' read -r bits1 bits2 bits3 <<< "${bits_set}"
    for l1 in "${l1_list[@]}"; do
      for per_parent in "${per_parent_list[@]}"; do
        name="L3_${l1}_pp${per_parent}_b${bits1}-${bits2}-${bits3}_${scaffold_mode}"
        echo "[100k-hi] ${name}"
        optimize_config "${name}" "${bits1}" "${bits2}" "${bits3}" "${l1}" "${per_parent}" "${scaffold_mode}"
      done
    done
  done
done

echo "[100k-hi] done"
