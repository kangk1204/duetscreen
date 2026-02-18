#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIGAND_CSV="${LIGAND_CSV:-${ROOT_DIR}/data/ligands/ligand.csv}"
PROTEIN_LIST="${PROTEIN_LIST:-${ROOT_DIR}/data/results/ligand_human_top_intersection_47.txt}"
OUT_PREFIX_BASE="${OUT_PREFIX_BASE:-ligand_47}"
DOCKERS="${DOCKERS:-gnina,diffdock}"
POCKETS="${POCKETS:-2}"
POCKET_TOOL="${POCKET_TOOL:-fpocket}"

if [[ ! -f "${LIGAND_CSV}" ]]; then
  echo "Ligand file not found: ${LIGAND_CSV}" >&2
  exit 1
fi
if [[ ! -f "${PROTEIN_LIST}" ]]; then
  echo "Protein list not found: ${PROTEIN_LIST}" >&2
  exit 1
fi

# Activate docking env (matches existing docking scripts).
source "$(conda info --base)/etc/profile.d/conda.sh"
# Work around openjdk activation script expecting JAVA_* vars.
export JAVA_LD_LIBRARY_PATH="${JAVA_LD_LIBRARY_PATH:-}"
export JAVA_HOME="${JAVA_HOME:-}"
conda activate duetscreen-dock
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

export PATH="$ROOT_DIR/third_party/docking_tools/bin:$PATH"
export DIFFDOCK_DIR="$ROOT_DIR/third_party/DiffDock"
export DIFFDOCK_CONDA_ENV="${DIFFDOCK_ENV:-diffdock}"
export DUETSCREEN_POCKET_TOOL="${DUETSCREEN_POCKET_TOOL:-${POCKET_TOOL}}"

while IFS= read -r uniprot; do
  [[ -z "${uniprot}" ]] && continue
  out_prefix="${OUT_PREFIX_BASE}/${uniprot}"
  out_dir="${ROOT_DIR}/data/docking/${out_prefix}"
  if [[ -f "${out_dir}/docking_ranked.csv" ]]; then
    echo "[skip] ${uniprot} (already done)"
    continue
  fi
  echo "[dock] ${uniprot}"
  if ! PYTHONPATH="$ROOT_DIR/src" python -m duetscreen dock \
    --uniprot "${uniprot}" \
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
  done < "${PROTEIN_LIST}"

echo "[all done]"
