from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = DATA_DIR / "models"
RESULTS_DIR = DATA_DIR / "results"
TARGETS_DIR = DATA_DIR / "targets"
CONTROLS_DIR = DATA_DIR / "controls"
ZINC_DIR = DATA_DIR / "zinc22"
DOCKING_DIR = DATA_DIR / "docking"
THIRD_PARTY_DIR = ROOT_DIR / "third_party"
