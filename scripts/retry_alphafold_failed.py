#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from pathlib import Path

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from duetscreen.data.alphafold import ALPHAFOLD_API, fetch_human_proteome_fasta, _validate_pdb


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


def load_failed(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    seen = set()
    uniq = []
    for pid in lines:
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    return uniq


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fasta", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path, default=Path("data/targets"))
    ap.add_argument("--failed", type=Path, default=Path("data/targets/alphafold_human_failed.txt"))
    ap.add_argument("--state", type=Path, default=Path("data/targets/alphafold_human_download.state.json"))
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--log-every", type=int, default=10)
    args = ap.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    fasta_path = args.fasta
    if fasta_path is None:
        fasta_path = fetch_human_proteome_fasta(out_dir)
    elif not fasta_path.exists():
        fasta_path = fetch_human_proteome_fasta(out_dir)

    failed_ids = load_failed(args.failed)
    if not failed_ids:
        print("[alphafold] no failed ids to retry")

    def attempt(uniprot_id: str) -> tuple[str, str]:
        target = out_dir / f"AF-{uniprot_id}-F1.pdb"
        if target.exists() and _validate_pdb(target):
            return ("ok", uniprot_id)
        try:
            entry = requests.get(f"{ALPHAFOLD_API}/{uniprot_id}", timeout=args.timeout)
            entry.raise_for_status()
            data = entry.json()
            if not data:
                raise RuntimeError("no entry")
            pdb_url = data[0]["pdbUrl"]
            model_id = data[0]["entryId"]
            dest = out_dir / f"{model_id}.pdb"
            with requests.get(pdb_url, stream=True, timeout=args.timeout) as resp:
                resp.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                with tmp.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                tmp.rename(dest)
            if not _validate_pdb(dest):
                dest.unlink(missing_ok=True)
                raise RuntimeError("invalid pdb")
            return ("ok", uniprot_id)
        except Exception:
            return ("fail", uniprot_id)

    remaining = []
    recovered = 0
    completed = 0
    if failed_ids:
        print(f"[alphafold] retry start total={len(failed_ids)} workers={args.workers}")
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futures = {ex.submit(attempt, pid): pid for pid in failed_ids}
            for fut in as_completed(futures):
                status, pid = fut.result()
                completed += 1
                if status == "ok":
                    recovered += 1
                else:
                    remaining.append(pid)
                if args.sleep:
                    time.sleep(args.sleep)
                if args.log_every and completed % args.log_every == 0:
                    print(f"[alphafold] retry {completed}/{len(failed_ids)} recovered={recovered} remaining={len(remaining)}")

    # recompute missing ids
    ids = list(iter_uniprot_ids(fasta_path))
    seen = set()
    uniq = []
    for pid in ids:
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    ids = uniq

    missing = []
    for uniprot_id in ids:
        target = out_dir / f"AF-{uniprot_id}-F1.pdb"
        if not (target.exists() and _validate_pdb(target)):
            missing.append(uniprot_id)

    args.failed.parent.mkdir(parents=True, exist_ok=True)
    args.failed.write_text("\n".join(missing) + ("\n" if missing else ""))

    # update state file for final status
    args.state.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "index": len(ids),
        "total": len(ids),
        "downloaded": len(ids) - len(missing),
        "skipped": 0,
        "failed": len(missing),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    args.state.write_text(__import__("json").dumps(state, indent=2))

    print(f"[alphafold] retry done. missing={len(missing)}")


if __name__ == "__main__":
    main()
