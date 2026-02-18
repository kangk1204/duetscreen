#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import OrderedDict
from pathlib import Path


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _write_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def _bucket_index(value: bytes, bucket_count: int) -> int:
    digest = hashlib.blake2b(value, digest_size=2).digest()
    return int.from_bytes(digest, "big") % bucket_count


class BucketWriter:
    def __init__(self, base_dir: Path, max_open: int) -> None:
        self.base_dir = base_dir
        self.max_open = max_open
        self._handles: OrderedDict[tuple[str, int], object] = OrderedDict()

    def write(self, prefix: str, bucket_idx: int, line: bytes) -> None:
        key = (prefix, bucket_idx)
        handle = self._handles.get(key)
        if handle is None:
            if len(self._handles) >= self.max_open:
                _, old = self._handles.popitem(last=False)
                old.close()
            bucket_dir = self.base_dir / prefix
            bucket_dir.mkdir(parents=True, exist_ok=True)
            path = bucket_dir / f"bucket_{bucket_idx:04d}.txt"
            handle = path.open("ab", buffering=1024 * 1024)
            self._handles[key] = handle
        else:
            self._handles.move_to_end(key)
        handle.write(line + b"\n")

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()


def _wait_for_relaxed(state_path: Path, poll_seconds: int) -> None:
    while True:
        if not state_path.exists():
            time.sleep(poll_seconds)
            continue
        try:
            state = json.loads(state_path.read_text())
        except Exception:
            time.sleep(poll_seconds)
            continue
        if state.get("complete"):
            return
        selected = state.get("selected_count")
        target = state.get("target_count")
        if selected is not None and target is not None and selected >= target:
            return
        total = state.get("total_files")
        file_index = state.get("file_index")
        if total and file_index is not None and file_index >= total:
            return
        time.sleep(poll_seconds)


def _bucketize(
    input_path: Path,
    prefix: str,
    bucket_count: int,
    writer: BucketWriter,
    state: dict,
    state_path: Path,
    offset_key: str,
    update_every: int = 200000,
) -> None:
    size = input_path.stat().st_size
    offset = int(state.get(offset_key, 0))
    if offset >= size:
        return
    with input_path.open("rb") as f:
        f.seek(offset)
        lines = 0
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.split()
            if not parts:
                continue
            smi = parts[0].strip()
            if not smi:
                continue
            bucket_idx = _bucket_index(smi, bucket_count)
            writer.write(prefix, bucket_idx, smi)
            lines += 1
            if lines % update_every == 0:
                state[offset_key] = f.tell()
                state["updated_at"] = _now()
                _write_state(state_path, state)
        state[offset_key] = f.tell()
        state["updated_at"] = _now()
        _write_state(state_path, state)


def _merge_buckets(
    bucket_dir: Path,
    out_path: Path,
    bucket_count: int,
    target_count: int,
    state: dict,
    state_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not out_path.exists():
        out_path.touch()

    output_count = int(state.get("output_count", 0))
    start_bucket = int(state.get("output_bucket_index", 0))

    if state.get("output_offset") is not None:
        offset = int(state["output_offset"])
        with out_path.open("ab") as out_f:
            out_f.truncate(offset)
        state["output_offset"] = None
        state["updated_at"] = _now()
        _write_state(state_path, state)

    with out_path.open("ab") as out_f:
        for bucket_idx in range(start_bucket, bucket_count):
            state["output_offset"] = out_f.tell()
            state["updated_at"] = _now()
            _write_state(state_path, state)

            seen: set[bytes] = set()
            base_path = bucket_dir / "base" / f"bucket_{bucket_idx:04d}.txt"
            if base_path.exists():
                with base_path.open("rb") as f:
                    for line in f:
                        parts = line.split()
                        if not parts:
                            continue
                        smi = parts[0]
                        if smi in seen:
                            continue
                        seen.add(smi)
                        out_f.write(smi + b"\n")
                        output_count += 1
                base_path.unlink()

            relaxed_path = bucket_dir / "relaxed" / f"bucket_{bucket_idx:04d}.txt"
            if relaxed_path.exists():
                with relaxed_path.open("rb") as f:
                    for line in f:
                        parts = line.split()
                        if not parts:
                            continue
                        smi = parts[0]
                        if smi in seen:
                            continue
                        seen.add(smi)
                        out_f.write(smi + b"\n")
                        output_count += 1
                        if output_count >= target_count:
                            break
                relaxed_path.unlink()

            state["output_bucket_index"] = bucket_idx + 1
            state["output_count"] = output_count
            state["output_offset"] = None
            state["updated_at"] = _now()
            if output_count >= target_count:
                state["phase"] = "complete"
                _write_state(state_path, state)
                return
            _write_state(state_path, state)

    state["phase"] = "complete"
    state["updated_at"] = _now()
    _write_state(state_path, state)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, default=Path("data/zinc22/purchasable_druglike.smi"))
    parser.add_argument("--relaxed", type=Path, default=Path("data/zinc22/purchasable_druglike_relaxed.smi"))
    parser.add_argument("--relaxed-state", type=Path, default=Path("data/zinc22/purchasable_druglike_relaxed.smi.state.json"))
    parser.add_argument("--out", type=Path, default=Path("data/zinc22/purchasable_druglike_merged.smi"))
    parser.add_argument("--state", type=Path, default=Path("data/zinc22/purchasable_druglike_merged.smi.state.json"))
    parser.add_argument("--target-count", type=int, default=100000000)
    parser.add_argument("--bucket-count", type=int, default=4096)
    parser.add_argument("--max-open", type=int, default=128)
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--keep-buckets", action="store_true")
    args = parser.parse_args()

    state = _load_state(args.state)
    if not state:
        ts = time.strftime("%Y%m%d_%H%M%S")
        bucket_dir = Path("data/zinc22") / f"merge_tmp_{ts}"
        state = {
            "phase": "waiting_relaxed",
            "bucket_dir": str(bucket_dir),
            "bucket_count": args.bucket_count,
            "max_open": args.max_open,
            "target_count": args.target_count,
            "base_path": str(args.base),
            "relaxed_path": str(args.relaxed),
            "out_path": str(args.out),
            "base_offset": 0,
            "relaxed_offset": 0,
            "output_bucket_index": 0,
            "output_count": 0,
            "output_offset": None,
            "updated_at": _now(),
        }
        _write_state(args.state, state)
    else:
        bucket_dir = Path(state.get("bucket_dir", "data/zinc22/merge_tmp_unknown"))
        if "bucket_dir" not in state:
            state["bucket_dir"] = str(bucket_dir)
            state["updated_at"] = _now()
            _write_state(args.state, state)

    if not args.no_wait and state.get("phase") == "waiting_relaxed":
        print("phase=waiting_relaxed")
        _wait_for_relaxed(args.relaxed_state, args.poll_seconds)
        state["phase"] = "bucketize_base"
        state["updated_at"] = _now()
        _write_state(args.state, state)
        print("phase=bucketize_base")
    elif args.no_wait and state.get("phase") == "waiting_relaxed":
        state["phase"] = "bucketize_base"
        state["updated_at"] = _now()
        _write_state(args.state, state)
        print("phase=bucketize_base")

    writer = BucketWriter(bucket_dir, args.max_open)
    try:
        if state.get("phase") == "bucketize_base":
            _bucketize(args.base, "base", args.bucket_count, writer, state, args.state, "base_offset")
            state["phase"] = "bucketize_relaxed"
            state["updated_at"] = _now()
            _write_state(args.state, state)

        if state.get("phase") == "bucketize_relaxed":
            _bucketize(args.relaxed, "relaxed", args.bucket_count, writer, state, args.state, "relaxed_offset")
            state["phase"] = "merge_output"
            state["updated_at"] = _now()
            _write_state(args.state, state)
    finally:
        writer.close()

    if state.get("phase") == "merge_output":
        _merge_buckets(bucket_dir, args.out, args.bucket_count, args.target_count, state, args.state)

    if state.get("phase") == "complete" and not args.keep_buckets:
        try:
            for path in (bucket_dir / "base").glob("bucket_*.txt"):
                path.unlink()
            for path in (bucket_dir / "relaxed").glob("bucket_*.txt"):
                path.unlink()
            for sub in [bucket_dir / "base", bucket_dir / "relaxed"]:
                if sub.exists():
                    sub.rmdir()
            if bucket_dir.exists():
                bucket_dir.rmdir()
        except Exception:
            pass


if __name__ == "__main__":
    main()
