from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def _load_state(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _fmt_ratio(current: Any, total: Any) -> str:
    if current is None and total is None:
        return "n/a"
    if total is None:
        return str(current)
    return f"{current}/{total}"


def show_status(state_path: Path, json_output: bool = False) -> None:
    state = _load_state(state_path)
    if json_output:
        if state is None:
            print(json.dumps({"error": "missing_state", "path": str(state_path)}))
            return
        print(json.dumps(state, indent=2))
        return

    if state is None:
        print(f"ZINC22 state not found: {state_path}")
        return

    listing_errors = state.get("listing_errors", [])
    file_errors = state.get("file_errors", [])

    lines = [
        f"state_path={state_path}",
        f"stage={state.get('stage', 'unknown')}",
        f"listing={_fmt_ratio(state.get('listing_index'), state.get('listing_total'))}",
    ]
    if state.get("listing_current"):
        lines.append(f"listing_current={state.get('listing_current')}")
    lines.extend(
        [
            f"selected={_fmt_ratio(state.get('selected_count'), state.get('target_count'))}",
            f"files={_fmt_ratio(state.get('file_index'), state.get('total_files'))}",
            f"listing_errors={len(listing_errors)}",
            f"file_errors={len(file_errors)}",
        ]
    )
    if state.get("updated_at"):
        lines.append(f"updated_at={state.get('updated_at')}")
    print("\n".join(lines))
