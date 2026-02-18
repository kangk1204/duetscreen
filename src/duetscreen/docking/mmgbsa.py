from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Tuple


def _select_platform(name: str = "CUDA"):
    try:
        from openmm import Platform

        return Platform.getPlatformByName(name)
    except Exception:
        try:
            from openmm import Platform

            return Platform.getPlatformByName("CPU")
        except Exception as exc:
            raise RuntimeError("OpenMM platform not available.") from exc


def _compute_energy(system, topology, positions, platform, minimize: bool = False) -> float:
    from openmm import LocalEnergyMinimizer, LangevinIntegrator, unit

    integrator = LangevinIntegrator(300 * unit.kelvin, 1 / unit.picosecond, 0.002 * unit.picoseconds)
    from openmm import Context

    context = Context(system, integrator, platform)
    context.setPositions(positions)
    if minimize:
        LocalEnergyMinimizer.minimize(context)
    state = context.getState(getEnergy=True)
    return state.getPotentialEnergy().value_in_unit(unit.kilocalorie_per_mole)


def _build_system_generator():
    from openmm import app
    from openmmforcefields.generators import SystemGenerator

    forcefields = ["amber14/protein.ff14SB.xml"]
    kwargs = dict(
        small_molecule_forcefield="openff-2.1.0",
        forcefield_kwargs={"implicitSolvent": app.OBC2},
    )
    try:
        return SystemGenerator(forcefields=forcefields, **kwargs)
    except TypeError:
        # Backward compatibility with older openmmforcefields versions.
        return SystemGenerator(forcefield_files=forcefields, **kwargs)


def _load_ligand_from_sdf(sdf_path: Path):
    from openff.toolkit.topology import Molecule

    mols = Molecule.from_file(str(sdf_path), allow_undefined_stereo=True)
    if isinstance(mols, list):
        if not mols:
            raise RuntimeError(f"No ligands in {sdf_path}")
        return mols[0]
    return mols


def mmgbsa_from_pose(
    receptor_pdb: Path,
    ligand_sdf: Path,
    out_csv: Path,
    ligand_id: str,
    platform_name: str = "CUDA",
    minimize: bool = False,
) -> Path:
    from openmm import app

    system_generator = _build_system_generator()
    platform = _select_platform(platform_name)

    receptor = app.PDBFile(str(receptor_pdb))
    ligand = _load_ligand_from_sdf(ligand_sdf)
    ligand_top = ligand.to_topology().to_openmm()
    ligand_pos = ligand.conformers[0]

    modeller = app.Modeller(receptor.topology, receptor.positions)
    modeller.add(ligand_top, ligand_pos)

    complex_system = system_generator.create_system(modeller.topology)
    receptor_system = system_generator.create_system(receptor.topology)
    ligand_system = system_generator.create_system(ligand_top)

    complex_energy = _compute_energy(complex_system, modeller.topology, modeller.positions, platform, minimize)
    receptor_energy = _compute_energy(receptor_system, receptor.topology, receptor.positions, platform, minimize)
    ligand_energy = _compute_energy(ligand_system, ligand_top, ligand_pos, platform, minimize)

    delta = complex_energy - receptor_energy - ligand_energy

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ligand_id", "delta_gbsa"])
        writer.writeheader()
        writer.writerow({"ligand_id": ligand_id, "delta_gbsa": delta})
    return out_csv


def batch_mmgbsa(
    receptor_pdb: Path,
    ligand_pose_sdfs: List[Tuple[str, Path]],
    out_csv: Path,
    platform_name: str = "CUDA",
    minimize: bool = False,
) -> Path:
    from openmm import app

    system_generator = _build_system_generator()
    platform = _select_platform(platform_name)

    receptor = app.PDBFile(str(receptor_pdb))
    receptor_system = system_generator.create_system(receptor.topology)
    receptor_energy = _compute_energy(receptor_system, receptor.topology, receptor.positions, platform, minimize)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ligand_id", "delta_gbsa"])
        writer.writeheader()
        for lig_id, sdf_path in ligand_pose_sdfs:
            ligand = _load_ligand_from_sdf(sdf_path)
            ligand_top = ligand.to_topology().to_openmm()
            ligand_pos = ligand.conformers[0]

            modeller = app.Modeller(receptor.topology, receptor.positions)
            modeller.add(ligand_top, ligand_pos)

            complex_system = system_generator.create_system(modeller.topology)
            ligand_system = system_generator.create_system(ligand_top)

            complex_energy = _compute_energy(
                complex_system, modeller.topology, modeller.positions, platform, minimize
            )
            ligand_energy = _compute_energy(ligand_system, ligand_top, ligand_pos, platform, minimize)
            delta = complex_energy - receptor_energy - ligand_energy
            writer.writerow({"ligand_id": lig_id, "delta_gbsa": delta})
    return out_csv
