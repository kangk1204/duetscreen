"""HyperVS1000: deterministic simulated DTI-to-docking pipeline."""

from importlib import metadata

try:
    __version__ = metadata.version("hypervs1000")
except metadata.PackageNotFoundError:  # pragma: no cover - during local dev
    __version__ = "0.0.0"

__all__ = ["__version__"]
