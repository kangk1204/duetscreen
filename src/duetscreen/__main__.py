from __future__ import annotations

import argparse
from pathlib import Path

from duetscreen.config import CONTROLS_DIR, DATA_DIR, MODELS_DIR, PROCESSED_DIR, TARGETS_DIR, ZINC_DIR
from duetscreen.data.alphafold import fetch_crbn
from duetscreen.data.bindingdb import prepare_bindingdb_for_models, sync_bindingdb_to_third_party
from duetscreen.data.positive_controls import write_positive_controls
from duetscreen.data.testset import create_crbn_testset

KNOWN_TARGETS = {
    "crbn": "Q96SW2",
}


def _resolve_uniprot(uniprot: str | None, protein_name: str | None) -> str | None:
    if uniprot:
        return uniprot
    if not protein_name:
        return None
    key = protein_name.strip().lower()
    return KNOWN_TARGETS.get(key)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="duetscreen")
    sub = parser.add_subparsers(dest="command", required=True)

    setup = sub.add_parser("setup-data", help="Download and prepare datasets + CRBN + ZINC22 subset")
    setup.add_argument("--bindingdb-split", default="bindingdb")
    setup.add_argument("--zinc-target-count", type=int, default=200000)
    setup.add_argument("--zinc-tranches", default="AA,AB,AC,AD,AE,AF,AG,AH")
    setup.add_argument("--zinc-reactivity", default="A")
    setup.add_argument("--zinc-purchasability", default="ABC")
    setup.add_argument("--zinc-strategy", choices=["simple", "stratified"], default="simple")
    setup.add_argument("--zinc-reactive-max", type=int, default=10)
    setup.add_argument("--zinc-purch-min", type=int, default=30)
    setup.add_argument("--zinc-mw-min", type=float, default=200.0)
    setup.add_argument("--zinc-mw-max", type=float, default=500.0)
    setup.add_argument("--zinc-logp-min", type=float, default=-1.0)
    setup.add_argument("--zinc-logp-max", type=float, default=5.0)
    setup.add_argument("--zinc-mw-bin", type=float, default=50.0)
    setup.add_argument("--zinc-logp-bin", type=float, default=1.0)
    setup.add_argument("--zinc-path", type=Path, default=None)

    fetch = sub.add_parser("fetch-crbn", help="Fetch CRBN from AlphaFold DB")
    fetch.add_argument("--uniprot", default="Q96SW2")

    zinc = sub.add_parser("download-zinc22", help="Download ZINC22 purchasable drug-like subset")
    zinc.add_argument("--zinc-target-count", type=int, default=200000)
    zinc.add_argument("--zinc-tranches", default="AA,AB,AC,AD,AE,AF,AG,AH")
    zinc.add_argument("--zinc-reactivity", default="A")
    zinc.add_argument("--zinc-purchasability", default="ABC")
    zinc.add_argument("--zinc-strategy", choices=["simple", "stratified"], default="simple")
    zinc.add_argument("--zinc-reactive-max", type=int, default=10)
    zinc.add_argument("--zinc-purch-min", type=int, default=30)
    zinc.add_argument("--zinc-mw-min", type=float, default=200.0)
    zinc.add_argument("--zinc-mw-max", type=float, default=500.0)
    zinc.add_argument("--zinc-logp-min", type=float, default=-1.0)
    zinc.add_argument("--zinc-logp-max", type=float, default=5.0)
    zinc.add_argument("--zinc-mw-bin", type=float, default=50.0)
    zinc.add_argument("--zinc-logp-bin", type=float, default=1.0)

    train = sub.add_parser("train", help="Train models")
    train.add_argument("--model", choices=["moltrans", "drugban", "graphdta", "all"], default="all")
    train.add_argument("--epochs", type=int, default=10)
    train.add_argument("--no-resume", action="store_true", help="Disable checkpoint resume")

    screen = sub.add_parser("screen", help="Run screening for CRBN")
    screen.add_argument("--protein", type=Path, default=TARGETS_DIR / "crbn.fasta")
    screen.add_argument("--chunk-size", type=int, default=4096)
    screen.add_argument("--zinc-path", type=Path, default=None)
    screen.add_argument("--out-prefix", type=str, default=None)

    agg = sub.add_parser("aggregate", help="Aggregate top intersection")
    agg.add_argument("--topk", type=int, default=10000)
    agg.add_argument("--per-model-k", type=int, default=100000)
    agg.add_argument("--prefix", type=str, default=None)

    screen_targets = sub.add_parser("screen-targets", help="Screen one ligand against a proteome")
    screen_targets.add_argument("--ligand-name", type=str, default=None)
    screen_targets.add_argument("--ligand-smiles", type=str, default=None)
    screen_targets.add_argument("--proteome", choices=["human"], default="human")
    screen_targets.add_argument("--proteome-fasta", type=Path, default=None)
    screen_targets.add_argument("--chunk-size", type=int, default=512)
    screen_targets.add_argument("--limit", type=int, default=None)
    screen_targets.add_argument("--preview", type=int, default=5)
    screen_targets.add_argument("--out-prefix", type=str, default=None)

    agg_targets = sub.add_parser("aggregate-targets", help="Aggregate proteome target screen")
    agg_targets.add_argument("--prefix", type=str, required=True)
    agg_targets.add_argument("--topk", type=int, default=1000)

    status = sub.add_parser("status", help="Show pipeline status")
    status.add_argument("--zinc-state", type=Path, default=ZINC_DIR / "purchasable_druglike.smi.state.json")
    status.add_argument("--json", action="store_true")

    dock = sub.add_parser("dock", help="Run pocket prediction + docking + MMGBSA pipeline")
    dock.add_argument("--uniprot", type=str, default=None)
    dock.add_argument("--protein-name", type=str, default=None)
    dock.add_argument("--pdb-id", type=str, default=None)
    dock.add_argument("--receptor-pdb", type=Path, default=None)
    dock.add_argument("--ligands", type=Path, required=True)
    dock.add_argument("--ligands-format", choices=["csv", "smi", "sdf"], default="csv")
    dock.add_argument("--smiles-column", type=str, default="smiles")
    dock.add_argument("--id-column", type=str, default=None)
    dock.add_argument("--limit", type=int, default=None)
    dock.add_argument("--out-prefix", type=str, default="dock")
    dock.add_argument("--pockets", type=int, default=3)
    dock.add_argument("--dockers", type=str, default="gnina,diffdock")
    dock.add_argument("--diffdock-dir", type=Path, default=None)
    dock.add_argument("--diffdock-python", type=str, default=None)
    dock.add_argument("--diffdock-env", type=str, default=None)
    dock.add_argument("--run-mmgbsa", action="store_true")
    dock.add_argument("--mmgbsa-topk", type=int, default=100)
    dock.add_argument("--platform", type=str, default="CUDA")

    hier_build = sub.add_parser("hierarchy-build", help="Build hierarchical cluster reps for SMILES")
    hier_build.add_argument("--smiles-path", type=Path, required=True)
    hier_build.add_argument("--layer", type=int, default=1)
    hier_build.add_argument("--bits", type=int, default=0)
    hier_build.add_argument("--rep-target", type=int, default=10000)
    hier_build.add_argument("--scaffold-mode", type=str, default="murcko",
                            choices=["murcko", "generic", "none"])
    hier_build.add_argument("--per-parent", type=int, default=None)
    hier_build.add_argument("--parent-bits", type=int, default=None)
    hier_build.add_argument("--parent-keys", type=Path, default=None)
    hier_build.add_argument("--out-dir", type=Path, default=None)
    hier_build.add_argument("--limit", type=int, default=None)
    hier_build.add_argument("--write-all-counts", action="store_true")

    hier_expand = sub.add_parser("hierarchy-expand", help="Expand clusters to members for next layer")
    hier_expand.add_argument("--smiles-path", type=Path, required=True)
    hier_expand.add_argument("--selected-keys", type=Path, required=True)
    hier_expand.add_argument("--bits", type=int, default=0)
    hier_expand.add_argument("--scaffold-mode", type=str, default="murcko",
                             choices=["murcko", "generic", "none"])
    hier_expand.add_argument("--out-path", type=Path, required=True)
    hier_expand.add_argument("--limit", type=int, default=None)

    hier_select = sub.add_parser("hierarchy-select", help="Select top clusters from score file")
    hier_select.add_argument("--scores", type=Path, required=True)
    hier_select.add_argument("--rep-meta", type=Path, required=True)
    hier_select.add_argument("--out-keys", type=Path, required=True)
    hier_select.add_argument("--topk", type=int, default=None)
    hier_select.add_argument("--top-fraction", type=float, default=None)
    hier_select.add_argument("--top-percent", type=float, default=None)
    hier_select.add_argument("--score-column", type=str, default="score")
    hier_select.add_argument("--smiles-column", type=str, default="smiles")
    hier_select.add_argument("--ascending", action="store_true")

    run_all = sub.add_parser("run-all", help="Setup, train, screen, aggregate")
    run_all.add_argument("--bindingdb-split", default="bindingdb")
    run_all.add_argument("--zinc-target-count", type=int, default=200000)
    run_all.add_argument("--topk", type=int, default=10000)
    run_all.add_argument("--chunk-size", type=int, default=4096)
    run_all.add_argument("--epochs", type=int, default=10)
    run_all.add_argument("--zinc-path", type=Path, default=None)

    return parser.parse_args()


def _setup_data(args: argparse.Namespace) -> None:
    from duetscreen.data.zinc22 import download_zinc22_stratified, download_zinc22_subset
    processed = prepare_bindingdb_for_models()
    sync_bindingdb_to_third_party(processed)
    fetch_crbn()
    write_positive_controls(["lenalidomide", "pomalidomide", "thalidomide"])

    tranches = [t.strip().upper() for t in args.zinc_tranches.split(",") if t.strip()]
    zinc_out = args.zinc_path if args.zinc_path else (ZINC_DIR / "purchasable_druglike.smi")
    if args.zinc_path:
        if not zinc_out.exists():
            raise FileNotFoundError(f"ZINC SMILES not found: {zinc_out}")
    else:
        if args.zinc_strategy == "stratified":
            download_zinc22_stratified(
                tranches=tranches,
                target_count=args.zinc_target_count,
                out_path=zinc_out,
                reactive_max=args.zinc_reactive_max,
                purchasable_min=args.zinc_purch_min,
                mw_min=args.zinc_mw_min,
                mw_max=args.zinc_mw_max,
                logp_min=args.zinc_logp_min,
                logp_max=args.zinc_logp_max,
                mw_bin=args.zinc_mw_bin,
                logp_bin=args.zinc_logp_bin,
            )
        else:
            reactivity = set(args.zinc_reactivity)
            purch = set(args.zinc_purchasability)
            download_zinc22_subset(
                tranches=tranches,
                reactivity=reactivity,
                purchasability=purch,
                target_count=args.zinc_target_count,
                out_path=zinc_out,
            )

    create_crbn_testset(
        controls_csv=CONTROLS_DIR / "positive_controls.csv",
        zinc_smiles=zinc_out,
        out_path=PROCESSED_DIR / "crbn_test.csv",
    )


def main() -> None:
    args = _parse_args()

    if args.command == "setup-data":
        _setup_data(args)
        return

    if args.command == "fetch-crbn":
        fetch_crbn(args.uniprot)
        return

    if args.command == "download-zinc22":
        from duetscreen.data.zinc22 import download_zinc22_stratified, download_zinc22_subset
        tranches = [t.strip().upper() for t in args.zinc_tranches.split(",") if t.strip()]
        if args.zinc_strategy == "stratified":
            download_zinc22_stratified(
                tranches=tranches,
                target_count=args.zinc_target_count,
                out_path=ZINC_DIR / "purchasable_druglike.smi",
                reactive_max=args.zinc_reactive_max,
                purchasable_min=args.zinc_purch_min,
                mw_min=args.zinc_mw_min,
                mw_max=args.zinc_mw_max,
                logp_min=args.zinc_logp_min,
                logp_max=args.zinc_logp_max,
                mw_bin=args.zinc_mw_bin,
                logp_bin=args.zinc_logp_bin,
            )
        else:
            reactivity = set(args.zinc_reactivity)
            purch = set(args.zinc_purchasability)
            download_zinc22_subset(
                tranches=tranches,
                reactivity=reactivity,
                purchasability=purch,
                target_count=args.zinc_target_count,
                out_path=ZINC_DIR / "purchasable_druglike.smi",
            )
        return

    if args.command == "train":
        from duetscreen.pipeline.train import train_all_models
        from duetscreen.models.moltrans import train_moltrans
        from duetscreen.models.drugban import train_drugban
        from duetscreen.models.graphdta import train_graphdta

        resume = not args.no_resume
        if args.model == "all":
            train_all_models(epochs=args.epochs, resume=resume)
        elif args.model == "moltrans":
            train_moltrans(PROCESSED_DIR / "bindingdb" / "moltrans",
                           MODELS_DIR / "moltrans" / "best.pt",
                           epochs=args.epochs,
                           resume=resume)
        elif args.model == "drugban":
            out_dir = MODELS_DIR / "drugban"
            out_dir.mkdir(parents=True, exist_ok=True)
            best = train_drugban(PROCESSED_DIR / "bindingdb" / "drugban", out_dir, epochs=args.epochs, resume=resume)
            if Path(best) != (out_dir / "best.pth"):
                (out_dir / "best.pth").write_bytes(Path(best).read_bytes())
        elif args.model == "graphdta":
            train_graphdta(PROCESSED_DIR / "bindingdb" / "graphdta",
                           MODELS_DIR / "graphdta" / "best.pt",
                           epochs=args.epochs,
                           resume=resume)
        return

    if args.command == "screen":
        from duetscreen.pipeline.screen import screen_all_models
        screen_all_models(
            args.protein,
            chunk_size=args.chunk_size,
            zinc_path=args.zinc_path,
            out_prefix=args.out_prefix,
        )
        return

    if args.command == "aggregate":
        from duetscreen.pipeline.aggregate import aggregate_topk
        aggregate_topk(topk=args.topk, per_model_k=args.per_model_k, prefix=args.prefix)
        return

    if args.command == "screen-targets":
        from duetscreen.pipeline.target_screen import screen_ligand_against_proteome
        proteome_fasta = args.proteome_fasta
        if args.proteome == "human" and proteome_fasta is None:
            proteome_fasta = None
        screen_ligand_against_proteome(
            ligand_name=args.ligand_name,
            ligand_smiles=args.ligand_smiles,
            proteome_fasta=proteome_fasta,
            out_prefix=args.out_prefix,
            chunk_size=args.chunk_size,
            limit=args.limit,
            preview=args.preview,
        )
        return

    if args.command == "aggregate-targets":
        from duetscreen.pipeline.aggregate import aggregate_protein_topk
        aggregate_protein_topk(prefix=args.prefix, topk=args.topk)
        return
    if args.command == "status":
        from duetscreen.pipeline.status import show_status
        show_status(args.zinc_state, json_output=args.json)
        return

    if args.command == "dock":
        from duetscreen.pipeline.docking import run_docking_pipeline
        dockers = [d.strip() for d in args.dockers.split(",") if d.strip()]
        uniprot = _resolve_uniprot(args.uniprot, args.protein_name)
        if args.pdb_id and args.receptor_pdb:
            raise ValueError("Provide either --pdb-id or --receptor-pdb (not both).")
        if args.pdb_id:
            if not uniprot:
                raise ValueError("Provide --uniprot or --protein-name to run AlphaFold docking alongside --pdb-id.")
            pdb_prefix = f"{args.out_prefix}_pdb_{args.pdb_id.lower()}"
            af_prefix = f"{args.out_prefix}_af"
            run_docking_pipeline(
                uniprot=None,
                receptor_pdb=None,
                pdb_id=args.pdb_id,
                ligands_path=args.ligands,
                ligands_format=args.ligands_format,
                smiles_column=args.smiles_column,
                id_column=args.id_column,
                limit=args.limit,
                out_prefix=pdb_prefix,
                pockets=args.pockets,
                dockers=dockers,
                diffdock_dir=args.diffdock_dir,
                diffdock_python=args.diffdock_python,
                diffdock_env=args.diffdock_env,
                run_mmgbsa=args.run_mmgbsa,
                mmgbsa_topk=args.mmgbsa_topk,
                platform=args.platform,
            )
            run_docking_pipeline(
                uniprot=uniprot,
                receptor_pdb=None,
                pdb_id=None,
                ligands_path=args.ligands,
                ligands_format=args.ligands_format,
                smiles_column=args.smiles_column,
                id_column=args.id_column,
                limit=args.limit,
                out_prefix=af_prefix,
                pockets=args.pockets,
                dockers=dockers,
                diffdock_dir=args.diffdock_dir,
                diffdock_python=args.diffdock_python,
                diffdock_env=args.diffdock_env,
                run_mmgbsa=args.run_mmgbsa,
                mmgbsa_topk=args.mmgbsa_topk,
                platform=args.platform,
            )
        else:
            run_docking_pipeline(
                uniprot=uniprot,
                receptor_pdb=args.receptor_pdb,
                pdb_id=None,
                ligands_path=args.ligands,
                ligands_format=args.ligands_format,
                smiles_column=args.smiles_column,
                id_column=args.id_column,
                limit=args.limit,
                out_prefix=args.out_prefix,
                pockets=args.pockets,
                dockers=dockers,
                diffdock_dir=args.diffdock_dir,
                diffdock_python=args.diffdock_python,
                diffdock_env=args.diffdock_env,
                run_mmgbsa=args.run_mmgbsa,
                mmgbsa_topk=args.mmgbsa_topk,
                platform=args.platform,
            )
        return

    if args.command == "hierarchy-build":
        from duetscreen.pipeline.hierarchy import build_layer
        out_dir = args.out_dir
        if out_dir is None:
            out_dir = DATA_DIR / "hierarchy" / f"layer{args.layer}"
        build_layer(
            smiles_path=args.smiles_path,
            out_dir=out_dir,
            bits=args.bits,
            rep_target=args.rep_target,
            limit=args.limit,
            write_all_counts=args.write_all_counts,
            per_parent=args.per_parent,
            parent_bits=args.parent_bits,
            parent_keys_path=args.parent_keys,
            scaffold_mode=args.scaffold_mode,
        )
        return

    if args.command == "hierarchy-expand":
        from duetscreen.pipeline.hierarchy import expand_layer
        expand_layer(
            smiles_path=args.smiles_path,
            selected_keys_path=args.selected_keys,
            bits=args.bits,
            out_path=args.out_path,
            limit=args.limit,
            scaffold_mode=args.scaffold_mode,
        )
        return

    if args.command == "hierarchy-select":
        from duetscreen.pipeline.hierarchy import select_keys
        select_keys(
            scores_path=args.scores,
            rep_meta_path=args.rep_meta,
            out_keys=args.out_keys,
            topk=args.topk,
            score_column=args.score_column,
            smiles_column=args.smiles_column,
            ascending=args.ascending,
            top_fraction=args.top_fraction,
            top_percent=args.top_percent,
        )
        return

    if args.command == "run-all":
        from duetscreen.pipeline.aggregate import aggregate_topk
        from duetscreen.pipeline.screen import screen_all_models
        from duetscreen.pipeline.train import train_all_models
        _setup_data(args)
        train_all_models(epochs=args.epochs, resume=True)
        screen_all_models(TARGETS_DIR / "crbn.fasta", chunk_size=args.chunk_size, zinc_path=args.zinc_path)
        aggregate_topk(topk=args.topk)
        return


if __name__ == "__main__":
    main()
