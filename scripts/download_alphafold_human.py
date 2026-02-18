#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from duetscreen.data.alphafold import fetch_alphafold_pdb, fetch_human_proteome_fasta


def iter_uniprot_ids(fasta_path: Path):
    with fasta_path.open("r") as f:
        for line in f:
            if not line.startswith(">"):
                continue
            header = line[1:].strip()
            if "|" in header:
                parts = header.split("|")
                if len(parts) >= 2 and parts[1]:
                    yield parts[1]
                    continue
            yield header.split()[0]


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fasta", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path, default=Path("data/targets"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--start", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--log-every", type=int, default=50)
    ap.add_argument("--state", type=Path, default=Path("data/targets/alphafold_human_download.state.json"))
    ap.add_argument("--failed", type=Path, default=Path("data/targets/alphafold_human_failed.txt"))
    ap.add_argument("--skip-existing", action="store_true", default=True)
    ap.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = ap.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    fasta_path = args.fasta
    if fasta_path is None:
        fasta_path = fetch_human_proteome_fasta(out_dir)
    elif not fasta_path.exists():
        fasta_path = fetch_human_proteome_fasta(out_dir)

    ids = list(iter_uniprot_ids(fasta_path))
    # de-duplicate while preserving order
    seen = set()
    uniq = []
    for pid in ids:
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    ids = uniq

    if args.limit is not None:
        ids = ids[: args.limit]

    state = load_state(args.state)
    start = args.start if args.start is not None else int(state.get("index", 0))

    total = len(ids)
    downloaded = int(state.get("downloaded", 0))
    skipped = int(state.get("skipped", 0))
    failed = int(state.get("failed", 0))

    for idx in range(start, total):
        uniprot_id = ids[idx]
        target = out_dir / f"AF-{uniprot_id}-F1.pdb"
        if args.skip_existing and target.exists() and target.stat().st_size > 1000:
            skipped += 1
        else:
            try:
                fetch_alphafold_pdb(uniprot_id, out_dir)
                downloaded += 1
            except Exception:
                failed += 1
                args.failed.parent.mkdir(parents=True, exist_ok=True)
                with args.failed.open("a") as f:
                    f.write(f"{uniprot_id}\n")
        if args.sleep:
            time.sleep(args.sleep)
        if args.log_every and (idx + 1) % args.log_every == 0:
            print(f"[alphafold] {idx+1}/{total} downloaded={downloaded} skipped={skipped} failed={failed}")
        if (idx + 1) % 10 == 0:
            save_state(
                args.state,
                {
                    "index": idx + 1,
                    "total": total,
                    "downloaded": downloaded,
                    "skipped": skipped,
                    "failed": failed,
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )

    save_state(
        args.state,
        {
            "index": total,
            "total": total,
            "downloaded": downloaded,
            "skipped": skipped,
            "failed": failed,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    print("[alphafold] done")


if __name__ == "__main__":
    main()
