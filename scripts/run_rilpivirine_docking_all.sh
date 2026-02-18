#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIGAND_CSV="${LIGAND_CSV:-${ROOT_DIR}/data/ligands/rilpivirine.csv}"
PDB_LIST="${PDB_LIST:-${ROOT_DIR}/data/targets/alphafold_human_available_pdbs.txt}"
OUT_PREFIX_BASE="${OUT_PREFIX_BASE:-rilpivirine_human_all}"
DOCKERS="${DOCKERS:-gnina}"
POCKETS="${POCKETS:-2}"
POCKET_TOOL="${POCKET_TOOL:-fpocket}"
PARALLEL="${PARALLEL:-1}"

if [[ ! -f "${LIGAND_CSV}" ]]; then
  echo "Ligand file not found: ${LIGAND_CSV}" >&2
  exit 1
fi
if [[ ! -f "${PDB_LIST}" ]]; then
  echo "PDB list not found: ${PDB_LIST}" >&2
  exit 1
fi

source "$(conda info --base)/etc/profile.d/conda.sh"
export JAVA_LD_LIBRARY_PATH="${JAVA_LD_LIBRARY_PATH:-}"
export JAVA_HOME="${JAVA_HOME:-}"
conda activate duetscreen-dock
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

export PATH="$ROOT_DIR/third_party/docking_tools/bin:$PATH"
export DIFFDOCK_DIR="$ROOT_DIR/third_party/DiffDock"
export DIFFDOCK_CONDA_ENV="${DIFFDOCK_ENV:-diffdock}"
export DUETSCREEN_POCKET_TOOL="${DUETSCREEN_POCKET_TOOL:-${POCKET_TOOL}}"

run_one() {
  local pdb_path="$1"
  [[ -z "${pdb_path}" ]] && return 0
  if [[ ! -f "${pdb_path}" ]]; then
    echo "[skip] missing pdb: ${pdb_path}"
    return 0
  fi
  local base
  base="$(basename "${pdb_path}")"
  local uniprot
  uniprot="${base#AF-}"
  uniprot="${uniprot%-F1.pdb}"
  local out_prefix
  out_prefix="${OUT_PREFIX_BASE}/${uniprot}"
  local out_dir
  out_dir="${ROOT_DIR}/data/docking/${out_prefix}"
  if [[ -f "${out_dir}/docking_ranked.csv" ]]; then
    echo "[skip] ${uniprot} (already done)"
    return 0
  fi
  echo "[dock] ${uniprot}"
  if ! PYTHONPATH="$ROOT_DIR/src" python -m duetscreen dock \
    --receptor-pdb "${pdb_path}" \
    --ligands "${LIGAND_CSV}" \
    --ligands-format csv \
    --smiles-column smiles \
    --id-column id \
    --pockets "${POCKETS}" \
    --dockers "${DOCKERS}" \
    --out-prefix "${out_prefix}"; then
    echo "[error] docking failed for ${uniprot}" >&2
  fi
  echo "[done] ${uniprot}"
}

if [[ "${PARALLEL}" -gt 1 ]]; then
  export ROOT_DIR LIGAND_CSV OUT_PREFIX_BASE DOCKERS POCKETS
  export -f run_one
  tr '\n' '\0' < "${PDB_LIST}" | xargs -0 -n 1 -P "${PARALLEL}" -I {} bash -c 'run_one "$@"' _ {}
else
  while IFS= read -r pdb_path; do
    run_one "${pdb_path}"
  done < "${PDB_LIST}"
fi

echo "[all done]"
