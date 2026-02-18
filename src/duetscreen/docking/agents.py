from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

from duetscreen.docking.aggregate import aggregate_rankings
from duetscreen.docking.diffdock import dock_diffdock
from duetscreen.docking.errors import ExternalToolError, InternalLogicError
from duetscreen.docking.gnina import dock_gnina
from duetscreen.docking.mmgbsa import batch_mmgbsa
from duetscreen.docking.pockets import Pocket, predict_pockets, write_pockets


@dataclass
class DockingContext:
    receptor: Path
    ligands: List[Tuple[str, str]]
    ligands_sdf: Path
    ligand_table: Path
    work_dir: Path
    pockets: List[Pocket] = field(default_factory=list)
    gnina_score_files: List[Path] = field(default_factory=list)
    gnina_scores: Path | None = None
    diffdock_scores: Path | None = None
    mmgbsa_scores: Path | None = None


@dataclass
class AgentResult:
    name: str
    success: bool
    outputs: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None


def _count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r") as f:
        return max(0, sum(1 for _ in f) - 1)


def _merge_score_files(paths: List[Path], out_path: Path, higher_is_better: bool = True) -> Path:
    best: Dict[str, Dict[str, str]] = {}
    best_score: Dict[str, float] = {}
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lig_id = row.get("ligand_id")
                if not lig_id:
                    continue
                try:
                    score = float(row.get("score"))
                except Exception:
                    continue
                current = best_score.get(lig_id)
                if current is None:
                    best_score[lig_id] = score
                    best[lig_id] = row
                else:
                    better = score > current if higher_is_better else score < current
                    if better:
                        best_score[lig_id] = score
                        best[lig_id] = row
    out_path.parent.mkdir(parents=True, exist_ok=True)
    extra_fields = []
    for row in best.values():
        for key in row.keys():
            if key in {"ligand_id", "score"}:
                continue
            if key not in extra_fields:
                extra_fields.append(key)
    fieldnames = ["ligand_id", "score"] + extra_fields
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lig_id, row in best.items():
            out_row = {key: row.get(key, "") for key in fieldnames}
            out_row["ligand_id"] = lig_id
            out_row["score"] = best_score.get(lig_id)
            writer.writerow(out_row)
    return out_path


def _best_gnina_pocket(score_paths: List[Path]) -> Dict[str, str]:
    best: Dict[str, Tuple[float, str]] = {}
    for path in score_paths:
        if not path.exists():
            continue
        with path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lig_id = row.get("ligand_id")
                pocket_id = row.get("pocket_id")
                if not lig_id or not pocket_id:
                    continue
                try:
                    score = float(row.get("score"))
                except Exception:
                    continue
                if lig_id not in best or score > best[lig_id][0]:
                    best[lig_id] = (score, pocket_id)
    return {lig_id: pocket for lig_id, (score, pocket) in best.items()}


def _extract_pose(sdf_path: Path, ligand_id: str, out_path: Path) -> bool:
    if not sdf_path.exists():
        return False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with sdf_path.open("r") as src, out_path.open("w") as dst:
        while True:
            name = src.readline()
            if not name:
                break
            record = [name]
            while True:
                line = src.readline()
                if not line:
                    break
                record.append(line)
                if line.startswith("$$$$"):
                    break
            if name.strip() == ligand_id:
                dst.writelines(record)
                return True
    return False


class PocketAgent:
    name = "pockets"

    def __init__(self, topk: int = 3, config: str = "alphafold"):
        self.topk = topk
        self.config = config

    def run(self, ctx: DockingContext) -> AgentResult:
        pockets = predict_pockets(ctx.receptor, ctx.work_dir / "pockets", topk=self.topk, config=self.config)
        ctx.pockets = pockets
        pockets_path = write_pockets(pockets, ctx.work_dir / "pockets.csv")
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"pockets": str(pockets_path)},
            metrics={"pocket_count": float(len(pockets))},
        )


class GninaAgent:
    name = "gnina"

    def __init__(self, exhaustiveness: int = 8, num_modes: int = 3):
        self.exhaustiveness = exhaustiveness
        self.num_modes = num_modes

    def run(self, ctx: DockingContext) -> AgentResult:
        if not ctx.pockets:
            raise InternalLogicError("No pockets available for GNINA.")
        gnina_dir = ctx.work_dir / "gnina"
        score_files = dock_gnina(
            ctx.receptor,
            ctx.ligands_sdf,
            ctx.pockets,
            gnina_dir,
            exhaustiveness=self.exhaustiveness,
            num_modes=self.num_modes,
        )
        merged = _merge_score_files(score_files, gnina_dir / "gnina_scores.csv", higher_is_better=True)
        ctx.gnina_score_files = score_files
        ctx.gnina_scores = merged
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"scores": str(merged)},
            metrics={"ligand_scores": float(_count_rows(merged))},
        )


class DiffDockAgent:
    name = "diffdock"

    def __init__(self, diffdock_dir: Path | None = None, python_cmd: str | None = None, conda_env: str | None = None):
        self.diffdock_dir = diffdock_dir
        self.python_cmd = python_cmd
        self.conda_env = conda_env

    def run(self, ctx: DockingContext) -> AgentResult:
        if not any(smi for _, smi in ctx.ligands):
            raise InternalLogicError("DiffDock requires SMILES input; ligand SMILES missing.")
        diffdock_dir_out = ctx.work_dir / "diffdock"
        scores = dock_diffdock(
            ctx.receptor,
            ctx.ligands,
            diffdock_dir_out,
            diffdock_dir=self.diffdock_dir,
            python_cmd=self.python_cmd,
            conda_env=self.conda_env,
        )
        ctx.diffdock_scores = scores
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"scores": str(scores)},
            metrics={"ligand_scores": float(_count_rows(scores))},
        )


class MMGBSAAgent:
    name = "mmgbsa"

    def __init__(self, topk: int = 100, platform: str = "CUDA"):
        self.topk = topk
        self.platform = platform

    def run(self, ctx: DockingContext) -> AgentResult:
        if not ctx.gnina_score_files:
            raise InternalLogicError("MMGBSA requires GNINA pose outputs.")
        best_pockets = _best_gnina_pocket(ctx.gnina_score_files)
        if ctx.gnina_scores is None:
            raise InternalLogicError("GNINA merged scores missing for MMGBSA selection.")

        ranked = []
        with ctx.gnina_scores.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ranked.append((row["ligand_id"], float(row["score"])))
                except Exception:
                    continue
        ranked.sort(key=lambda x: x[1], reverse=True)
        selected = ranked[: self.topk]

        pose_dir = ctx.work_dir / "mmgbsa_poses"
        ligand_pose_sdfs = []
        for lig_id, _ in selected:
            pocket_id = best_pockets.get(lig_id)
            if not pocket_id:
                continue
            sdf_path = ctx.work_dir / "gnina" / f"gnina_{pocket_id}.sdf"
            out_pose = pose_dir / f"{lig_id}.sdf"
            if _extract_pose(sdf_path, lig_id, out_pose):
                ligand_pose_sdfs.append((lig_id, out_pose))

        out_csv = ctx.work_dir / "mmgbsa_scores.csv"
        batch_mmgbsa(ctx.receptor, ligand_pose_sdfs, out_csv, platform_name=self.platform)
        ctx.mmgbsa_scores = out_csv
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"scores": str(out_csv)},
            metrics={"ligand_scores": float(_count_rows(out_csv))},
        )


class EvaluationAgent:
    name = "evaluation"

    def run(self, ctx: DockingContext, results: List[AgentResult]) -> AgentResult:
        metrics = {
            "pocket_count": float(len(ctx.pockets)) if ctx.pockets else 0.0,
            "gnina_scores": float(_count_rows(ctx.gnina_scores)) if ctx.gnina_scores else 0.0,
            "diffdock_scores": float(_count_rows(ctx.diffdock_scores)) if ctx.diffdock_scores else 0.0,
            "mmgbsa_scores": float(_count_rows(ctx.mmgbsa_scores)) if ctx.mmgbsa_scores else 0.0,
        }
        out_path = ctx.work_dir / "agent_evaluation.json"
        out_path.write_text(json.dumps(metrics, indent=2))
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"evaluation": str(out_path)},
            metrics=metrics,
        )


class IntegrationAgent:
    name = "integration"

    def run(self, ctx: DockingContext) -> AgentResult:
        methods: List[Tuple[str, Path, str]] = []
        if ctx.gnina_scores:
            methods.append(("gnina", ctx.gnina_scores, "higher"))
        if ctx.diffdock_scores:
            methods.append(("diffdock", ctx.diffdock_scores, "higher"))
        if ctx.mmgbsa_scores:
            methods.append(("mmgbsa", ctx.mmgbsa_scores, "lower"))

        if not methods:
            raise InternalLogicError("No docking scores available for integration.")

        out_rank = ctx.work_dir / "docking_ranked.csv"
        aggregate_rankings(methods, ctx.ligand_table, out_rank)
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"ranked": str(out_rank)},
            metrics={"rows": float(_count_rows(out_rank))},
        )


class ErrorAgent:
    name = "error_agent"

    def run(self, results: List[AgentResult], out_path: Path) -> AgentResult:
        internal = []
        for res in results:
            if not res.error_type:
                continue
            if res.error_type == ExternalToolError.__name__:
                continue
            internal.append(
                {
                    "agent": res.name,
                    "error_type": res.error_type,
                    "error_message": res.error_message,
                }
            )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"internal_errors": internal}, indent=2))
        return AgentResult(
            name=self.name,
            success=True,
            outputs={"errors": str(out_path)},
            metrics={"internal_error_count": float(len(internal))},
        )
