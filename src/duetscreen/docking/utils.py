from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Mapping, Sequence


def which(cmd: str) -> str | None:
    return shutil.which(cmd)


def run_cmd(
    args: Sequence[str],
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    from duetscreen.docking.errors import ExternalToolError

    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    try:
        return subprocess.run(
            list(args),
            cwd=str(cwd) if cwd else None,
            env=merged_env,
            check=check,
            capture_output=capture_output,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ExternalToolError(f"Command not found: {args[0]}") from exc
    except subprocess.CalledProcessError as exc:
        raise ExternalToolError(f"Command failed ({exc.returncode}): {' '.join(args)}") from exc
