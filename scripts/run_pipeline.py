#!/usr/bin/env python3
"""End-to-end pipeline runner for DUET-Screen."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency already required elsewhere
    yaml = None


STAGES = ["validate", "prep", "dti", "dock", "mmgbsa", "aggregate", "report"]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full DUET-Screen pipeline sequentially.")
    parser.add_argument("--config", required=True, help="Path to pipeline configuration file.")
    parser.add_argument(
        "--devices",
        default="0",
        help="GPU device list passed to dti stage (default: 0). Use empty string for CPU-only.",
    )
    parser.add_argument(
        "--smi-dir",
        help="Directory containing downloaded ZINC .smi files. Required if --sample-size is set.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        help="If provided, create a random ligand sample of this size before running the pipeline.",
    )
    parser.add_argument(
        "--sample-output",
        help="Destination CSV path for the sampled ligands (required when --sample-size is set).",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        help="Random seed used when sampling ligands.",
    )
    parser.add_argument(
        "--override-workdir",
        help="Override workdir path via environment (HVS_PATHS__WORKDIR).",
    )
    parser.add_argument(
        "--override-manifest",
        help="Override manifest path via environment (HVS_PATHS__MANIFEST).",
    )
    parser.add_argument(
        "--override-reports",
        help="Override reports path via environment (HVS_PATHS__REPORTS).",
    )
    parser.add_argument(
        "--export-json",
        help="Optional path to export summary JSON after report stage.",
    )
    parser.add_argument(
        "--export-xlsx",
        help="Optional path to export summary Excel after report stage.",
    )
    parser.add_argument(
        "--ligand-library",
        help="Optional ligand CSV for export_results.py (adds SMILES).",
    )
    parser.add_argument(
        "--protein-library",
        help="Optional protein CSV for export_results.py (adds sequences).",
    )
    parser.add_argument(
        "--export-limit",
        type=int,
        default=50,
        help="Max partners per input when exporting (default: 50).",
    )
    parser.add_argument(
        "--aggregate-path",
        help="Override aggregate JSON path; defaults to workspace/aggregate/final_rankings.json derived from config.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config_data = _load_config_data(config_path)
    base_paths = config_data.get("paths", {})

    # Resolve workdir/manifest/report paths so we can optionally override them via env vars.
    config_workdir = _resolve_path(None, base_paths.get("workdir", "workspace"), config_path)
    workdir = _resolve_path(args.override_workdir, config_workdir, config_path)

    config_manifest = _resolve_path(
        None,
        base_paths.get("manifest", config_workdir / "MANIFEST.json"),
        config_path,
        default_parent=config_workdir,
    )
    if args.override_manifest:
        manifest = _resolve_path(args.override_manifest, config_manifest, config_path)
    elif workdir != config_workdir:
        manifest = (workdir / "MANIFEST.json").resolve()
    else:
        manifest = config_manifest

    config_reports = _resolve_path(
        None,
        base_paths.get("reports", config_workdir / "reports"),
        config_path,
        default_parent=config_workdir,
    )
    if args.override_reports:
        reports = _resolve_path(args.override_reports, config_reports, config_path)
    elif workdir != config_workdir:
        reports = (workdir / "reports").resolve()
    else:
        reports = config_reports

    env_overrides: Dict[str, str] = {}
    if workdir != config_workdir:
        env_overrides["HVS_PATHS__WORKDIR"] = str(workdir)
    if manifest != config_manifest:
        env_overrides["HVS_PATHS__MANIFEST"] = str(manifest)
    if reports != config_reports:
        env_overrides["HVS_PATHS__REPORTS"] = str(reports)

    sample_output_path: Optional[Path] = None
    if args.sample_size:
        if not args.smi_dir or not args.sample_output:
            raise ValueError("--smi-dir and --sample-output are required when --sample-size is set")
        sample_output_path = Path(args.sample_output).expanduser().resolve()
        # Build a temporary ligand CSV via reservoir sampling.  Reuses the main builder script so
        # CLI flags stay consistent across manual and automated workflows.
        builder_cmd = [
            "python",
            str(Path(__file__).resolve().parent / "build_ligand_library_from_smi.py"),
            "--input-dir",
            str(Path(args.smi_dir).expanduser().resolve()),
            "--output",
            str(sample_output_path),
            "--skip-missing",
            "--limit",
            str(args.sample_size),
            "--random-sample",
        ]
        if args.sample_seed is not None:
            builder_cmd.extend(["--seed", str(args.sample_seed)])
        run_command(builder_cmd, env=os.environ.copy())
        env_overrides["HVS_LIBRARY__LIGANDS_FILE"] = str(sample_output_path)
    elif args.ligand_library:
        sample_output_path = Path(args.ligand_library).expanduser().resolve()

    base_env = os.environ.copy()
    base_env.update(env_overrides)

    duet_cli = shutil.which("duet_screen")
    if duet_cli:
        duet_cmd_prefix = [duet_cli]
    else:
        duet_cmd_prefix = [sys.executable, "-m", "duet_screen.cli"]

    for command in STAGES:
        stage_args = [*duet_cmd_prefix, command, "--config", str(config_path)]
        if command == "dti" and args.devices:
            stage_args.extend(["--devices", args.devices])
        # Propagate env overrides (workdir/manifest/reports) to each stage.
        run_command(stage_args, env=base_env)

    if args.export_json or args.export_xlsx:
        aggregate_path = (
            Path(args.aggregate_path).expanduser().resolve()
            if args.aggregate_path
            else workdir / "aggregate" / "final_rankings.json"
        )
        export_args = [
            "python",
            str(Path(__file__).resolve().parent / "export_results.py"),
            "--aggregate",
            str(aggregate_path),
            "--limit",
            str(args.export_limit),
        ]
        if args.export_json:
            export_args.extend(["--output-json", str(Path(args.export_json).expanduser().resolve())])
        else:
            export_args.extend(["--output-json", "/dev/null"])
        if args.export_xlsx:
            export_args.extend(["--output-xlsx", str(Path(args.export_xlsx).expanduser().resolve())])
        else:
            export_args.extend(["--output-xlsx", "/dev/null"])
        ligand_path_for_export = sample_output_path if sample_output_path else args.ligand_library
        if ligand_path_for_export:
            export_args.extend(["--ligand-library", str(Path(ligand_path_for_export).expanduser().resolve())])
        if args.protein_library:
            export_args.extend(["--protein-library", str(Path(args.protein_library).expanduser().resolve())])

        run_command(export_args, env=base_env)

    return 0


def run_command(args: Sequence[str], env: Optional[Dict[str, str]] = None) -> None:
    start = time.time()
    print(f"[run_pipeline] Executing ({time.strftime('%H:%M:%S')}): {' '.join(args)}")
    subprocess.run(args, check=True, env=env)
    elapsed = time.time() - start
    print(f"[run_pipeline] Completed in {elapsed:.1f}s")


def _load_config_data(config_path: Path) -> Dict[str, Any]:  # type: ignore[name-defined]
    with config_path.open("r", encoding="utf-8") as handle:
        if config_path.suffix.lower() in {".json"}:
            return json.load(handle)
        if yaml is None:
            raise RuntimeError("PyYAML is required to parse non-JSON config files.")
        return yaml.safe_load(handle)


def _resolve_path(
    override: Optional[str],
    default_value,
    config_path: Path,
    *,
    default_parent: Optional[Path] = None,
) -> Path:
    if override:
        return Path(override).expanduser().resolve()
    if isinstance(default_value, Path):
        base = default_value
    else:
        base = Path(default_value)
    if base.is_absolute():
        return base
    parent = default_parent if default_parent is not None else config_path.parent
    return (parent / base).resolve()


if __name__ == "__main__":
    raise SystemExit(main())
