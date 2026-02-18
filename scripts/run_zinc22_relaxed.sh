#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
TARGET_TOTAL="${TARGET_TOTAL:-100000000}"
BASE_STATE="${BASE_STATE:-data/zinc22/purchasable_druglike.smi.state.json}"
OUT_PATH="${OUT_PATH:-data/zinc22/purchasable_druglike_relaxed.smi}"
TRANCHES="${TRANCHES:-}"
REACTIVE_MAX="${REACTIVE_MAX:-10}"
PURCH_MIN="${PURCH_MIN:-0}"
MW_MIN="${MW_MIN:-150}"
MW_MAX="${MW_MAX:-700}"
LOGP_MIN="${LOGP_MIN:--2}"
LOGP_MAX="${LOGP_MAX:-7}"
MW_BIN="${MW_BIN:-50}"
LOGP_BIN="${LOGP_BIN:-1}"
NUM_WORKERS="${NUM_WORKERS:-}"

export TARGET_TOTAL BASE_STATE OUT_PATH
export TRANCHES REACTIVE_MAX PURCH_MIN MW_MIN MW_MAX LOGP_MIN LOGP_MAX MW_BIN LOGP_BIN NUM_WORKERS

"$PYTHON_BIN" -u - <<'PY'
import json
import os
from pathlib import Path

from duetscreen.data.zinc22 import download_zinc22_stratified

target_total = int(os.environ.get("TARGET_TOTAL", "100000000"))
base_state = Path(os.environ.get("BASE_STATE", "data/zinc22/purchasable_druglike.smi.state.json"))
out_path = Path(os.environ.get("OUT_PATH", "data/zinc22/purchasable_druglike_relaxed.smi"))
tranches_env = os.environ.get("TRANCHES", "").strip()
reactive_max = int(os.environ.get("REACTIVE_MAX", "10"))
purch_min = int(os.environ.get("PURCH_MIN", "0"))
mw_min = float(os.environ.get("MW_MIN", "150"))
mw_max = float(os.environ.get("MW_MAX", "700"))
logp_min = float(os.environ.get("LOGP_MIN", "-2"))
logp_max = float(os.environ.get("LOGP_MAX", "7"))
mw_bin = float(os.environ.get("MW_BIN", "50"))
logp_bin = float(os.environ.get("LOGP_BIN", "1"))
num_workers_env = os.environ.get("NUM_WORKERS", "").strip()
num_workers = int(num_workers_env) if num_workers_env else None

selected = 0
tranches = [t.strip().upper() for t in tranches_env.split(",") if t.strip()] if tranches_env else ["ALL"]
if base_state.exists():
    try:
        base = json.loads(base_state.read_text())
        selected = base.get("selected_count", base.get("output_count", 0))
        tranches = base.get("tranches") or tranches
    except Exception:
        selected = 0

remaining = max(0, target_total - selected)
print(
    "base_selected=%s remaining=%s out=%s tranches=%s reactive_max=%s purch_min=%s mw=[%s,%s] logp=[%s,%s] bins=[%s,%s] workers=%s"
    % (
        selected,
        remaining,
        out_path,
        "ALL" if tranches == ["ALL"] else len(tranches),
        reactive_max,
        purch_min,
        mw_min,
        mw_max,
        logp_min,
        logp_max,
        mw_bin,
        logp_bin,
        num_workers if num_workers is not None else "auto",
    )
)

if remaining <= 0:
    print("No remaining target; exiting.")
else:
    download_zinc22_stratified(
        tranches=tranches,
        target_count=remaining,
        out_path=out_path,
        reactive_max=reactive_max,
        purchasable_min=purch_min,
        mw_min=mw_min,
        mw_max=mw_max,
        logp_min=logp_min,
        logp_max=logp_max,
        mw_bin=mw_bin,
        logp_bin=logp_bin,
        num_workers=num_workers,
    )
PY
