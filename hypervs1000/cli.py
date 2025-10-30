"""Command line interface for HyperVS1000."""

from __future__ import annotations

import argparse
from typing import Optional, Sequence

from hypervs1000.config import Config, PipelineSettings, load_config
from hypervs1000.manifest import log_invocation
from hypervs1000.pipeline import (
    run_aggregate,
    run_docking,
    run_dti,
    run_mmgbsa,
    run_prep,
    run_report,
    run_validate,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    devices = _parse_devices(args.devices)
    if devices is not None:
        config = config.with_pipeline(config.pipeline.with_devices(devices))

    if args.command == "validate":
        run_validate(config)
    elif args.command == "prep":
        run_prep(config)
    elif args.command == "dti":
        run_dti(config, devices=devices)
    elif args.command == "dock":
        run_docking(config)
    elif args.command == "mmgbsa":
        run_mmgbsa(config)
    elif args.command == "aggregate":
        run_aggregate(config)
    elif args.command == "report":
        run_report(config)
    else:
        parser.error(f"Unknown command {args.command}")

    log_invocation(
        config.paths.manifest,
        command=args.command,
        config_path=str(args.config),
        devices=args.devices,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hypervs1000")

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--config", required=True, help="Path to pipeline configuration.")
    parent.add_argument("--devices", help="Comma-separated list of GPU device ids.")

    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "prep", "dti", "dock", "mmgbsa", "aggregate", "report"):
        subparsers.add_parser(command, parents=[parent])
    return parser


def _parse_devices(text: Optional[str]) -> Optional[Sequence[int]]:
    if text is None:
        return None
    items = [segment.strip() for segment in text.split(",") if segment.strip()]
    if not items:
        return None
    return [int(item) for item in items]


if __name__ == "__main__":
    raise SystemExit(main())
