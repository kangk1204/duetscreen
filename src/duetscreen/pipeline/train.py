from __future__ import annotations

from pathlib import Path

from duetscreen.config import MODELS_DIR, PROCESSED_DIR
from duetscreen.models.drugban import train_drugban
from duetscreen.models.graphdta import train_graphdta
from duetscreen.models.moltrans import train_moltrans


def train_all_models(epochs: int = 10, resume: bool = True) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    moltrans_dir = PROCESSED_DIR / "bindingdb" / "moltrans"
    drugban_dir = PROCESSED_DIR / "bindingdb" / "drugban"
    graphdta_dir = PROCESSED_DIR / "bindingdb" / "graphdta"

    moltrans_out = MODELS_DIR / "moltrans" / "best.pt"
    moltrans_out.parent.mkdir(parents=True, exist_ok=True)
    train_moltrans(moltrans_dir, moltrans_out, epochs=epochs, resume=resume)

    drugban_out_dir = MODELS_DIR / "drugban"
    drugban_out_dir.mkdir(parents=True, exist_ok=True)
    best_drugban = train_drugban(drugban_dir, drugban_out_dir, epochs=epochs, resume=resume)
    if Path(best_drugban) != (drugban_out_dir / "best.pth"):
        (drugban_out_dir / "best.pth").write_bytes(Path(best_drugban).read_bytes())

    graphdta_out = MODELS_DIR / "graphdta" / "best.pt"
    graphdta_out.parent.mkdir(parents=True, exist_ok=True)
    train_graphdta(graphdta_dir, graphdta_out, epochs=epochs, resume=resume)
