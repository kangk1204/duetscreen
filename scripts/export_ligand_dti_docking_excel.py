#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def load_gene_map(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, sep="\t")
    except Exception:
        return {}
    id_col = None
    gene_col = None
    for cand in ["Entry", "accession", "Accessions", "Uniprot", "uniprot_id"]:
        if cand in df.columns:
            id_col = cand
            break
    for cand in ["Gene Names (primary)", "gene_primary", "Gene Names", "gene_name"]:
        if cand in df.columns:
            gene_col = cand
            break
    if id_col is None or gene_col is None:
        return {}
    mapping = {}
    for acc, gene in zip(df[id_col], df[gene_col]):
        if pd.isna(acc):
            continue
        if pd.isna(gene):
            continue
        mapping[str(acc)] = str(gene)
    return mapping


def _read_best_gnina_score(scores_path: Path) -> dict | None:
    try:
        df = pd.read_csv(scores_path)
    except Exception:
        return None
    if df.empty:
        return None
    # Prefer gnina score column if present
    score_col = "score" if "score" in df.columns else "gnina_score"
    if score_col not in df.columns:
        return None
    df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
    df = df.dropna(subset=[score_col])
    if df.empty:
        return None
    row = df.loc[df[score_col].idxmax()]
    return {
        "gnina_score": row.get("gnina_score", row.get("score")),
        "cnn_score": row.get("cnn_score"),
        "cnn_affinity": row.get("cnn_affinity"),
        "vina_affinity": row.get("vina_affinity"),
        "pocket_id": row.get("pocket_id"),
    }


def _read_docking_ranked(ranked_path: Path) -> dict | None:
    try:
        df = pd.read_csv(ranked_path)
    except Exception:
        return None
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "gnina_score": row.get("gnina_score", row.get("score")),
    }


def _base_uniprot_id(name: str) -> str:
    return name.split("-")[0]


def build_dock_index(
    dock_dirs: list[Path], progress_every: int | None = None
) -> dict[str, dict]:
    """Scan docking folders once and keep best record per base UniProt ID."""
    best_by_base: dict[str, dict] = {}
    best_score_by_base: dict[str, float | None] = {}
    total = len(dock_dirs)
    for idx, base in enumerate(dock_dirs, start=1):
        if progress_every and (idx % progress_every == 0 or idx == total):
            print(
                f"[dock_index] {idx}/{total} ({idx/total:.1%})",
                flush=True,
            )
        record = None
        score_path = base / "gnina" / "gnina_scores.csv"
        if score_path.exists():
            record = _read_best_gnina_score(score_path)
        if record is None:
            ranked_path = base / "docking_ranked.csv"
            if ranked_path.exists():
                record = _read_docking_ranked(ranked_path)
        if record is None:
            continue
        score_val = record.get("gnina_score")
        try:
            score_val = float(score_val)
        except Exception:
            score_val = None
        base_id = _base_uniprot_id(base.name)
        prev = best_score_by_base.get(base_id)
        if prev is None or (score_val is not None and score_val > prev):
            record = dict(record)
            record["dock_source_id"] = base.name
            best_by_base[base_id] = record
            best_score_by_base[base_id] = score_val
    return best_by_base


def load_docking_scores_from_index(
    dock_index: dict[str, dict], protein_ids: list[str]
) -> pd.DataFrame:
    rows: list[dict] = []
    for pid in protein_ids:
        base_id = _base_uniprot_id(str(pid))
        record = dock_index.get(base_id)
        if record is None:
            continue
        merged = dict(record)
        merged["protein_id"] = pid
        rows.append(merged)
    if not rows:
        return pd.DataFrame(columns=["protein_id"])
    return pd.DataFrame(rows)


def add_docking_ranks(df: pd.DataFrame) -> pd.DataFrame:
    for col, asc in [
        ("gnina_score", False),
        ("cnn_affinity", False),
        ("vina_affinity", True),
    ]:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[f"{col}_rank"] = df[col].rank(ascending=asc, method="min")
    return df


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Merge ligand DTI results with docking scores and export to Excel."
    )
    parser.add_argument(
        "--all",
        type=Path,
        default=root / "data/results/ligand_human_top_all.csv",
        help="Full DTI result CSV (no filtering).",
    )
    parser.add_argument(
        "--union",
        type=Path,
        default=root / "data/results/ligand_human_top_union.csv",
    )
    parser.add_argument(
        "--intersection",
        type=Path,
        default=root / "data/results/ligand_human_top_intersection.csv",
    )
    parser.add_argument(
        "--dock-root",
        type=Path,
        default=root / "data/docking/ligand_human_all",
    )
    parser.add_argument(
        "--gene-map",
        type=Path,
        default=root / "data/targets/uniprot_human_gene_map.tsv",
        help="TSV mapping with UniProt accession to gene name.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=root / "data/results/ligand_dti_docking_merged.xlsx",
    )
    parser.add_argument(
        "--only-all",
        action="store_true",
        help="Write only the full DTI sheet (no union/intersection).",
    )
    parser.add_argument(
        "--include-all-docking",
        action="store_true",
        help="Include a full docking-only sheet for all completed proteins.",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=None,
        help="Optional CSV output path for faster export.",
    )
    parser.add_argument(
        "--xlsx-engine",
        type=str,
        default=None,
        help="Excel writer engine override (xlsxwriter or openpyxl).",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print progress while building docking index.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Progress interval for docking index scan.",
    )
    args = parser.parse_args()
    gene_map = load_gene_map(args.gene_map)
    dock_dirs = [p for p in args.dock_root.iterdir() if p.is_dir()]
    if args.progress:
        print(f"[dock_index] scanning {len(dock_dirs)} folders", flush=True)
    dock_index = build_dock_index(
        dock_dirs, progress_every=args.progress_every if args.progress else None
    )
    if args.progress:
        print(f"[dock_index] done: {len(dock_index)} base IDs", flush=True)
    engine = args.xlsx_engine
    if engine is None:
        try:
            import xlsxwriter  # noqa: F401

            engine = "xlsxwriter"
        except Exception:
            engine = "openpyxl"

    if args.only_all:
        all_df = pd.read_csv(args.all)
        if args.progress:
            print(f"[merge] DTI rows: {len(all_df)}", flush=True)
        dock_all = load_docking_scores_from_index(
            dock_index, all_df["protein_id"].tolist()
        )
        merged_all = all_df.merge(dock_all, on="protein_id", how="left")
        merged_all["dock_available"] = merged_all["gnina_score"].notna()
        merged_all = add_docking_ranks(merged_all)
        if gene_map:
            merged_all.insert(
                0,
                "gene_symbol",
                merged_all["protein_id"]
                .astype(str)
                .map(lambda x: gene_map.get(x) or gene_map.get(x.split("-")[0])),
            )
        if args.csv_out:
            if args.progress:
                print(f"[write] CSV -> {args.csv_out}", flush=True)
            merged_all.to_csv(args.csv_out, index=False)
        if args.progress:
            print(f"[write] XLSX -> {args.out}", flush=True)
        with pd.ExcelWriter(args.out, engine=engine) as writer:
            merged_all.to_excel(writer, sheet_name="DTI_all_20659", index=False)
        print(f"Wrote Excel: {args.out}")
        return

    union = pd.read_csv(args.union)
    intersection = pd.read_csv(args.intersection)

    dock_union = load_docking_scores_from_index(
        dock_index, union["protein_id"].tolist()
    )
    dock_inter = load_docking_scores_from_index(
        dock_index, intersection["protein_id"].tolist()
    )

    merged_union = union.merge(dock_union, on="protein_id", how="left")
    merged_inter = intersection.merge(dock_inter, on="protein_id", how="left")
    merged_union["dock_available"] = merged_union["gnina_score"].notna()
    merged_inter["dock_available"] = merged_inter["gnina_score"].notna()

    merged_union = add_docking_ranks(merged_union)
    merged_inter = add_docking_ranks(merged_inter)
    if gene_map:
        merged_union.insert(
            0,
            "gene_symbol",
            merged_union["protein_id"]
            .astype(str)
            .map(lambda x: gene_map.get(x) or gene_map.get(x.split("-")[0])),
        )
        merged_inter.insert(
            0,
            "gene_symbol",
            merged_inter["protein_id"]
            .astype(str)
            .map(lambda x: gene_map.get(x) or gene_map.get(x.split("-")[0])),
        )

    with pd.ExcelWriter(args.out, engine=engine) as writer:
        merged_union.to_excel(writer, sheet_name="DTI_union_2640", index=False)
        merged_inter.to_excel(writer, sheet_name="DTI_intersection_47", index=False)
        if args.include_all_docking:
            all_proteins = [p.name for p in args.dock_root.iterdir() if p.is_dir()]
            dock_all = load_docking_scores(args.dock_root, all_proteins)
            dock_all = add_docking_ranks(dock_all)
            dock_all.to_excel(writer, sheet_name="Docking_completed", index=False)

    print(f"Wrote Excel: {args.out}")


if __name__ == "__main__":
    main()
