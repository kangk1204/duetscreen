"""Manifest writer for reproducibility."""

from __future__ import annotations

import json
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from hypervs1000 import __version__
from hypervs1000.utils import ensure_directory, git_commit_hash, now_utc_iso


@dataclass
class Manifest:
    path: Path
    data: Dict[str, Any]

    def save(self) -> None:
        ensure_directory(self.path.parent)
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")


def load_manifest(path: Path) -> Manifest:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        data = _create_manifest_stub()
    return Manifest(path=path, data=data)


def log_invocation(manifest_path: Path, command: str, *, config_path: Optional[str] = None, devices: Optional[str] = None) -> None:
    manifest = load_manifest(manifest_path)
    entry = {
        "timestamp": now_utc_iso(),
        "command": command,
        "config": config_path,
        "devices": devices,
    }
    manifest.data.setdefault("invocations", []).append(entry)
    manifest.save()


def _create_manifest_stub() -> Dict[str, Any]:
    return {
        "created_at": now_utc_iso(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cuda": _cuda_metadata(),
        "nvcc": _nvcc_version(),
        "package_versions": {"hypervs1000": __version__},
        "git": {"commit": git_commit_hash()},
        "invocations": [],
    }


def _cuda_metadata() -> Dict[str, Optional[str]]:
    query = [
        "nvidia-smi",
        "--query-gpu=driver_version,cuda_version",
        "--format=csv,noheader",
    ]
    output = _run_capture(query)
    if not output:
        return {"driver_version": None, "cuda_version": None}
    first = output.splitlines()[0]
    parts = [part.strip() for part in first.split(",")]
    if len(parts) >= 2:
        return {"driver_version": parts[0], "cuda_version": parts[1]}
    return {"driver_version": None, "cuda_version": None}


def _nvcc_version() -> Optional[str]:
    output = _run_capture(["nvcc", "-V"])
    return output or None


def _run_capture(command: List[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return completed.stdout.strip()
    except Exception:
        return ""
