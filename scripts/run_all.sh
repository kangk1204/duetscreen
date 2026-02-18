#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_NAME="${ENV_NAME:-}"
if [[ -z "${ENV_NAME}" ]]; then
  if conda env list | awk '{print $1}' | grep -qx "dl"; then
    ENV_NAME="dl"
  else
    ENV_NAME="duetscreen"
  fi
fi

ENV_NAME="${ENV_NAME}" bash "${ROOT_DIR}/scripts/setup.sh"

conda run -n "${ENV_NAME}" python -m duetscreen run-all \
  --bindingdb-split bindingdb \
  --zinc-target-count 200000 \
  --topk 10000
