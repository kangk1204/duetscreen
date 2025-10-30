#!/usr/bin/env python3
"""Helper script to generate DUET-Screen configuration files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a DUET-Screen configuration file.")
    parser.add_argument("--output", required=True, help="Path to config file to write (json or yaml).")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Pipeline chunk size (default: 2000).")
    parser.add_argument("--dti-top-k", type=int, default=50, help="DTI top-k per input (default: 50).")
    parser.add_argument("--docking-top-k", type=int, default=50, help="Docking top-k per input (default: 50).")
    parser.add_argument("--mmgbsa-top-k", type=int, default=50, help="MM/GBSA top-k per input (default: 50).")
    parser.add_argument(
        "--devices",
        default="0",
        help="Comma-separated device list for the pipeline (default: '0').",
    )
    parser.add_argument(
        "--input-csv",
        default="../data/sample_inputs.csv",
        help="Path to input sequences CSV (default: ../data/sample_inputs.csv).",
    )
    parser.add_argument(
        "--ligands-file",
        required=True,
        help="Path to ligand library CSV (required).",
    )
    parser.add_argument(
        "--workdir",
        default="../workspace",
        help="Workspace directory for outputs (default: ../workspace).",
    )
    parser.add_argument(
        "--manifest",
        help="Manifest file path (default: <workdir>/MANIFEST.json).",
    )
    parser.add_argument(
        "--reports",
        help="Reports directory path (default: <workdir>/reports).",
    )
    parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        help="Output format. If omitted, inferred from file extension.",
    )
    parser.add_argument(
        "--dti-weight",
        type=float,
        default=0.4,
        help="Stage weight for DTI scores (default: 0.4).",
    )
    parser.add_argument(
        "--docking-weight",
        type=float,
        default=0.35,
        help="Stage weight for docking scores (default: 0.35).",
    )
    parser.add_argument(
        "--mmgbsa-weight",
        type=float,
        default=0.25,
        help="Stage weight for MM/GBSA scores (default: 0.25).",
    )
    parser.add_argument(
        "--consensus-constant",
        type=int,
        default=60,
        help="Consensus constant for WRRF (default: 60).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output).expanduser().resolve()

    devices = [int(device.strip()) for device in args.devices.split(",") if device.strip()]
    if not devices:
        devices = [0]

    workdir = Path(args.workdir)
    manifest = Path(args.manifest) if args.manifest else workdir / "MANIFEST.json"
    reports = Path(args.reports) if args.reports else workdir / "reports"

    # Assemble the configuration structure expected by DUET-Screen.
    # Users can tweak most key parameters from the CLI without editing JSON by hand.
    config: Dict[str, object] = {
        "pipeline": {
            "chunk_size": args.chunk_size,
            "num_workers": 1,
            "devices": devices,
            "simulator": True,
            "dti_top_k": args.dti_top_k,
            "docking_top_k": args.docking_top_k,
            "mmgbsa_top_k": args.mmgbsa_top_k,
            "consensus_constant": args.consensus_constant,
            "stage_weights": {
                "dti": args.dti_weight,
                "docking": args.docking_weight,
                "mmgbsa": args.mmgbsa_weight,
            },
        },
        "inputs": {
            "sequences": args.input_csv,
        },
        "library": {
            "proteins": [
                {"id": "CRBN", "sequence": "MMDKEVQKSSSRTPSYQPGVSQSVE"},
                {"id": "IKZF1", "sequence": "MPLGKKAKLPEKKAPVTPQLPQLQ"},
            ],
            "ligands_file": args.ligands_file,
        },
        "paths": {
            "workdir": str(workdir),
            "manifest": str(manifest),
            "reports": str(reports),
        },
    }

    output_format = args.format or output_path.suffix.lower().lstrip(".")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "json":
        output_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    elif output_format in {"yaml", "yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to write YAML configs. Install with `pip install pyyaml`.")
        output_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported format '{output_format}'. Use json or yaml.")

    print(f"Wrote config to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
