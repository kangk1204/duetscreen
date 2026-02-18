#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from pathlib import Path


def count_mols(path: Path) -> int:
    cnt = 0
    with path.open("r") as f:
        for line in f:
            if line.startswith("$$$$"):
                cnt += 1
    return cnt


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor SDF growth and ETA.")
    parser.add_argument("--sdf", type=Path, required=True)
    parser.add_argument("--total", type=int, default=0, help="Expected total molecule count")
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--interval", type=int, default=600)
    args = parser.parse_args()

    prev_size = None
    prev_mols = None
    prev_ts = None

    args.log.parent.mkdir(parents=True, exist_ok=True)

    while True:
        if args.sdf.exists():
            size = args.sdf.stat().st_size
            mols = count_mols(args.sdf)
            now = time.time()
            rate_bps = None
            mols_per_sec = None
            eta_sec = None
            if prev_ts is not None and now > prev_ts:
                dt = now - prev_ts
                if prev_size is not None:
                    rate_bps = (size - prev_size) / dt
                if prev_mols is not None:
                    mols_per_sec = (mols - prev_mols) / dt
                if rate_bps and rate_bps > 0 and args.total and mols:
                    avg_bytes = size / mols
                    est_total = avg_bytes * args.total
                    if est_total > size:
                        eta_sec = (est_total - size) / rate_bps

            ts_str = time.strftime("%Y-%m-%d %H:%M:%S")
            with args.log.open("a") as f:
                f.write(
                    f"{ts_str}\tbytes={size}\tmols={mols}\trate_Bps={rate_bps}\t"
                    f"mols_per_sec={mols_per_sec}\teta_sec={eta_sec}\n"
                )

            prev_size = size
            prev_mols = mols
            prev_ts = now

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
