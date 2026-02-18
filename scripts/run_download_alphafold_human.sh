#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/data/targets}"
FASTA="${FASTA:-${OUT_DIR}/UP000005640_9606.fasta}"
SLEEP="${SLEEP:-0.2}"
LOG_EVERY="${LOG_EVERY:-50}"
LIMIT="${LIMIT:-}"
START="${START:-}"

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

ARGS=("${ROOT_DIR}/scripts/download_alphafold_human.py" \
  --out-dir "${OUT_DIR}" \
  --fasta "${FASTA}" \
  --sleep "${SLEEP}" \
  --log-every "${LOG_EVERY}")

if [[ -n "${LIMIT}" ]]; then
  ARGS+=(--limit "${LIMIT}")
fi
if [[ -n "${START}" ]]; then
  ARGS+=(--start "${START}")
fi

${PYTHON_CMD} "${ARGS[@]}"
