from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

from duetscreen.config import DOCKING_DIR, TARGETS_DIR
from duetscreen.data.alphafold import fetch_alphafold_pdb
from duetscreen.data.rcsb import fetch_rcsb_pdb
from duetscreen.docking.agents import (
    AgentResult,
    DockingContext,
    ErrorAgent,
    EvaluationAgent,
    GninaAgent,
    DiffDockAgent,
    IntegrationAgent,
    MMGBSAAgent,
    PocketAgent,
)
from duetscreen.docking.errors import ExternalToolError, InternalLogicError
from duetscreen.docking.ligands import read_ligands, smiles_to_sdf, write_ligand_table


def _ensure_receptor(
    uniprot: str | None,
    receptor_pdb: Path | None,
    pdb_id: str | None,
    out_dir: Path,
) -> Path:
    if receptor_pdb:
        src = receptor_pdb
    elif pdb_id:
        src = fetch_rcsb_pdb(pdb_id, TARGETS_DIR)
    elif uniprot:
        src = fetch_alphafold_pdb(uniprot, TARGETS_DIR)
    else:
        raise ValueError("Provide receptor_pdb, pdb_id, or uniprot.")

    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "receptor.pdb"
    if not dest.exists():
        dest.write_bytes(src.read_bytes())
    return dest


def _read_sdf_ids(path: Path, limit: int | None = None) -> List[Tuple[str, str]]:
    ligands: List[Tuple[str, str]] = []
    with path.open("r") as f:
        while True:
            name = f.readline()
            if not name:
                break
            lig_id = name.strip() or f"L{len(ligands)+1:06d}"
            ligands.append((lig_id, ""))
            if limit and len(ligands) >= limit:
                break
            # skip rest of record
            while True:
                line = f.readline()
                if not line or line.startswith("$$$$"):
                    break
    return ligands


def run_docking_pipeline(
    uniprot: str | None,
    receptor_pdb: Path | None,
    pdb_id: str | None,
    ligands_path: Path,
    ligands_format: str = "csv",
    smiles_column: str = "smiles",
    id_column: str | None = None,
    limit: int | None = None,
    out_prefix: str = "dock",
    pockets: int = 3,
    dockers: List[str] | None = None,
    diffdock_dir: Path | None = None,
    diffdock_python: str | None = None,
    diffdock_env: str | None = None,
    run_mmgbsa: bool = False,
    mmgbsa_topk: int = 100,
    platform: str = "CUDA",
    pocket_config: str | None = None,
) -> Path:
    dockers = dockers or ["gnina", "diffdock"]
    work_dir = DOCKING_DIR / out_prefix
    receptor = _ensure_receptor(uniprot, receptor_pdb, pdb_id, work_dir)
    if pocket_config is None:
        pocket_config = "default" if (pdb_id or receptor_pdb) else "alphafold"

    if ligands_format == "sdf":
        ligands = _read_sdf_ids(ligands_path, limit)
        ligands_sdf = ligands_path
    else:
        ligands = read_ligands(ligands_path, smiles_column=smiles_column, id_column=id_column, limit=limit)
        ligands_sdf = work_dir / "ligands.sdf"
        if not ligands_sdf.exists():
            smiles_to_sdf(ligands, ligands_sdf)
    ligand_table = write_ligand_table(ligands, work_dir / "ligands.csv")

    ctx = DockingContext(
        receptor=receptor,
        ligands=ligands,
        ligands_sdf=ligands_sdf,
        ligand_table=ligand_table,
        work_dir=work_dir,
    )

    results: List[AgentResult] = []

    def _run(agent) -> AgentResult:
        try:
            res = agent.run(ctx)
            return res
        except ExternalToolError as exc:
            return AgentResult(
                name=agent.name,
                success=False,
                error_type=ExternalToolError.__name__,
                error_message=str(exc),
            )
        except InternalLogicError as exc:
            return AgentResult(
                name=agent.name,
                success=False,
                error_type=InternalLogicError.__name__,
                error_message=str(exc),
            )
        except Exception as exc:
            return AgentResult(
                name=agent.name,
                success=False,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

    results.append(_run(PocketAgent(topk=pockets, config=pocket_config)))

    if "gnina" in dockers:
        results.append(_run(GninaAgent()))
    if "diffdock" in dockers:
        results.append(_run(DiffDockAgent(diffdock_dir, diffdock_python, diffdock_env)))
    if run_mmgbsa:
        results.append(_run(MMGBSAAgent(topk=mmgbsa_topk, platform=platform)))

    results.append(EvaluationAgent().run(ctx, results))
    integration = _run(IntegrationAgent())
    results.append(integration)
    ErrorAgent().run(results, work_dir / "internal_errors.json")

    if not integration.success:
        raise InternalLogicError("Integration failed. See internal_errors.json for details.")

    return work_dir / "docking_ranked.csv"
