#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 TRANCHE OUTPUT_DIR [RSYNC_EXTRA_OPTIONS...]" >&2
  exit 1
fi
tranche="$1"
shift
outdir="$1"
shift
mkdir -p "$outdir"
rsync -L -a --progress --prune-empty-dirs --delete-excluded \
  --include='*/' \
  --include="${tranche}P???-?.smi.gz" \
  --exclude='*' \
  "$@" \
  "rsync://files.docking.org/ZINC22-3D/${tranche}" \
  "$outdir"
