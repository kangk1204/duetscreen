"""Configuration loading and validation for HyperVS1000."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None

from hypervs1000.utils import load_csv

ConfigPrimitive = Union[str, int, float, bool, None]
ConfigValue = Union[ConfigPrimitive, Sequence["ConfigValue"], Mapping[str, "ConfigValue"]]


@dataclass(frozen=True)
class StageWeights:
    """Weights for consensus aggregation."""

    dti: float = 0.4
    docking: float = 0.35
    mmgbsa: float = 0.25

    def normalized(self) -> "StageWeights":
        total = self.dti + self.docking + self.mmgbsa
        if total <= 0:
            raise ValueError("Stage weights must sum to a positive value.")
        return StageWeights(
            dti=self.dti / total,
            docking=self.docking / total,
            mmgbsa=self.mmgbsa / total,
        )


@dataclass(frozen=True)
class PipelineSettings:
    """Compute pipeline behaviour toggles."""

    chunk_size: int = 32
    num_workers: int = 1
    devices: Tuple[int, ...] = (0,)
    simulator: bool = True
    dti_top_k: int = 5
    docking_top_k: int = 5
    mmgbsa_top_k: int = 5
    consensus_constant: int = 60
    stage_weights: StageWeights = field(default_factory=StageWeights)

    def with_devices(self, devices: Optional[Iterable[int]]) -> "PipelineSettings":
        if not devices:
            return self
        return PipelineSettings(
            chunk_size=self.chunk_size,
            num_workers=self.num_workers,
            devices=tuple(int(d) for d in devices),
            simulator=self.simulator,
            dti_top_k=self.dti_top_k,
            docking_top_k=self.docking_top_k,
            mmgbsa_top_k=self.mmgbsa_top_k,
            consensus_constant=self.consensus_constant,
            stage_weights=self.stage_weights,
        )


@dataclass(frozen=True)
class InputSettings:
    """Input file definitions."""

    sequences: Path


@dataclass(frozen=True)
class LibraryProtein:
    """Reference protein entry."""

    id: str
    sequence: str


@dataclass(frozen=True)
class LibraryLigand:
    """Reference ligand entry."""

    id: str
    smiles: str


@dataclass(frozen=True)
class LibrarySettings:
    """Synthetic library of partner molecules."""

    proteins: Tuple[LibraryProtein, ...]
    ligands: Tuple[LibraryLigand, ...]
    proteins_source: Optional[Path] = None
    ligands_source: Optional[Path] = None

    def ensure_non_empty(self) -> None:
        if not self.proteins:
            raise ValueError("library.proteins must contain at least one entry.")
        if not self.ligands:
            raise ValueError("library.ligands must contain at least one entry.")


@dataclass(frozen=True)
class PathSettings:
    """Filesystem layout for intermediate results."""

    workdir: Path
    manifest: Path
    reports: Path


@dataclass(frozen=True)
class Config:
    """Root configuration model."""

    pipeline: PipelineSettings
    inputs: InputSettings
    library: LibrarySettings
    paths: PathSettings

    def resolve_paths(self, base: Optional[Path] = None) -> "Config":
        base_dir = base or Path(".")
        library = LibrarySettings(
            proteins=self.library.proteins,
            ligands=self.library.ligands,
            proteins_source=self._resolve_optional_path(self.library.proteins_source, base_dir),
            ligands_source=self._resolve_optional_path(self.library.ligands_source, base_dir),
        )
        return Config(
            pipeline=self.pipeline,
            inputs=InputSettings(sequences=(base_dir / self.inputs.sequences).resolve()),
            library=library,
            paths=PathSettings(
                workdir=(base_dir / self.paths.workdir).resolve(),
                manifest=(base_dir / self.paths.manifest).resolve(),
                reports=(base_dir / self.paths.reports).resolve(),
            ),
        )

    @staticmethod
    def _resolve_optional_path(path: Optional[Path], base: Path) -> Optional[Path]:
        if path is None:
            return None
        return (base / path).resolve()

    def with_pipeline(self, pipeline: PipelineSettings) -> "Config":
        return Config(pipeline=pipeline, inputs=self.inputs, library=self.library, paths=self.paths)


def load_config(path: Union[str, Path]) -> Config:
    """Load configuration from YAML/JSON with environment overrides."""

    path = Path(path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw_text = handle.read()

    raw_config = _parse_config_text(raw_text, suffix=path.suffix)
    _apply_env_overrides(raw_config, os.environ)

    config = _build_config(raw_config, base_dir=path.parent)
    return config


def _load_library_proteins(path: Path) -> List[LibraryProtein]:
    rows = load_csv(path)
    proteins: List[LibraryProtein] = []
    for row in rows:
        identifier = row.get("id") or row.get("ID") or row.get("zinc_id")
        sequence = row.get("sequence") or row.get("Sequence")
        if not identifier or not sequence:
            raise ValueError(f"Invalid protein row in {path}: {row}")
        proteins.append(LibraryProtein(id=str(identifier), sequence=str(sequence)))
    return proteins


def _load_library_ligands(path: Path) -> List[LibraryLigand]:
    rows = load_csv(path)
    ligands: List[LibraryLigand] = []
    for row in rows:
        identifier = row.get("id") or row.get("ID") or row.get("zinc_id")
        # Accept the `value` column produced by build_ligand_library_from_smi.py, and fall back to
        # common SMILES headers so externally supplied CSVs remain compatible.
        smiles = row.get("smiles") or row.get("SMILES") or row.get("Smiles") or row.get("value")
        if not identifier or not smiles:
            raise ValueError(f"Invalid ligand row in {path}: {row}")
        ligands.append(LibraryLigand(id=str(identifier), smiles=str(smiles)))
    return ligands


def _parse_config_text(text: str, suffix: str) -> Dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("Config file is empty.")
    if suffix.lower() in {".json"}:
        return json.loads(stripped)
    if stripped.startswith("{") or stripped.startswith("["):
        return json.loads(stripped)
    if yaml is not None:
        return yaml.safe_load(stripped)
    return _minimal_yaml_parse(stripped)


def _minimal_yaml_parse(text: str) -> Dict[str, Any]:
    """Very small YAML subset parser used if PyYAML is unavailable."""

    data: Dict[str, Any] = {}
    current_section: Optional[str] = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line and not line.startswith("-"):
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value == "":
                data[key] = []
                current_section = key
            else:
                data[key] = _coerce_scalar(value)
        elif line.startswith("-") and current_section:
            value = line[1:].strip()
            data.setdefault(current_section, []).append(_coerce_scalar(value))
        else:
            raise ValueError(f"Cannot parse config line: {line}")
    return data


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _apply_env_overrides(config: MutableMapping[str, Any], env: Mapping[str, str]) -> None:
    """Allow users to override config values via HVS_* environment variables."""
    prefix = "HVS_"
    for key, value in env.items():
        if not key.startswith(prefix):
            continue
        path = key[len(prefix) :].lower().split("__")
        _write_nested(config, path, _parse_env_value(value))


def _write_nested(config: MutableMapping[str, Any], path: Sequence[str], value: Any) -> None:
    cursor: MutableMapping[str, Any] = config
    for key in path[:-1]:
        if key not in cursor or not isinstance(cursor[key], MutableMapping):
            cursor[key] = {}
        cursor = cursor[key]
    cursor[path[-1]] = value


def _parse_env_value(value: str) -> Any:
    value = value.strip()
    if not value:
        return value
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            continue
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_env_value(part.strip()) for part in inner.split(",")]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        result: Dict[str, Any] = {}
        for segment in inner.split(","):
            if "=" in segment:
                k, v = segment.split("=", 1)
            elif ":" in segment:
                k, v = segment.split(":", 1)
            else:
                raise ValueError(f"Cannot parse env map: {segment}")
            result[k.strip()] = _parse_env_value(v.strip())
        return result
    return value


def _build_config(raw: Mapping[str, Any], base_dir: Path) -> Config:
    try:
        pipeline_raw = raw["pipeline"]
        inputs_raw = raw["inputs"]
        library_raw = raw["library"]
        paths_raw = raw["paths"]
    except KeyError as error:
        missing = error.args[0]
        raise KeyError(f"Missing required config section: {missing}") from error

    stage_weights = StageWeights(
        dti=float(pipeline_raw.get("stage_weights", {}).get("dti", StageWeights().dti)),
        docking=float(pipeline_raw.get("stage_weights", {}).get("docking", StageWeights().docking)),
        mmgbsa=float(pipeline_raw.get("stage_weights", {}).get("mmgbsa", StageWeights().mmgbsa)),
    ).normalized()

    devices_raw = pipeline_raw.get("devices")
    devices: Tuple[int, ...]
    if isinstance(devices_raw, str):
        devices = tuple(int(part.strip()) for part in devices_raw.split(",") if part.strip())
    elif isinstance(devices_raw, Iterable):
        devices = tuple(int(part) for part in devices_raw)
    else:
        devices = (0,)
    if not devices:
        devices = (0,)

    pipeline = PipelineSettings(
        chunk_size=int(pipeline_raw.get("chunk_size", 32)),
        num_workers=int(pipeline_raw.get("num_workers", 1)),
        devices=devices,
        simulator=bool(pipeline_raw.get("simulator", True)),
        dti_top_k=int(pipeline_raw.get("dti_top_k", 5)),
        docking_top_k=int(pipeline_raw.get("docking_top_k", 5)),
        mmgbsa_top_k=int(pipeline_raw.get("mmgbsa_top_k", 5)),
        consensus_constant=int(pipeline_raw.get("consensus_constant", 60)),
        stage_weights=stage_weights,
    )

    inputs = InputSettings(sequences=(base_dir / inputs_raw["sequences"]).resolve())

    proteins_file_raw = library_raw.get("proteins_file")
    ligands_file_raw = library_raw.get("ligands_file")
    proteins_source = (base_dir / proteins_file_raw).resolve() if proteins_file_raw else None
    ligands_source = (base_dir / ligands_file_raw).resolve() if ligands_file_raw else None

    proteins = list(
        LibraryProtein(id=str(item["id"]), sequence=str(item["sequence"]))
        for item in library_raw.get("proteins", [])
    )
    ligands = list(
        LibraryLigand(id=str(item["id"]), smiles=str(item["smiles"]))
        for item in library_raw.get("ligands", [])
    )

    if proteins_source:
        proteins.extend(_load_library_proteins(proteins_source))
    if ligands_source:
        ligands.extend(_load_library_ligands(ligands_source))

    library = LibrarySettings(
        proteins=tuple(proteins),
        ligands=tuple(ligands),
        proteins_source=proteins_source,
        ligands_source=ligands_source,
    )
    library.ensure_non_empty()

    workdir_path = (base_dir / paths_raw["workdir"]).resolve()
    manifest_path = Path(paths_raw.get("manifest", workdir_path / "MANIFEST.json"))
    reports_path = Path(paths_raw.get("reports", workdir_path / "reports"))
    manifest_path = (base_dir / manifest_path).resolve()
    reports_path = (base_dir / reports_path).resolve()

    paths = PathSettings(
        workdir=workdir_path,
        manifest=manifest_path,
        reports=reports_path,
    )

    return Config(pipeline=pipeline, inputs=inputs, library=library, paths=paths)
