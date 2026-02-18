from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import roc_auc_score
from torch.utils import data

from duetscreen.config import THIRD_PARTY_DIR

MOLTRANS_DIR = THIRD_PARTY_DIR / "MolTrans"


def _import_moltrans():
    graphdta_path = str(MOLTRANS_DIR.parent / "GraphDTA")
    drugban_path = str(MOLTRANS_DIR.parent / "DrugBAN")
    cleaned = []
    for p in sys.path:
        norm = p.rstrip("/")
        if norm == graphdta_path or norm == drugban_path:
            continue
        if "/third_party/GraphDTA" in norm or "/third_party/DrugBAN" in norm:
            continue
        cleaned.append(p)
    sys.path = cleaned
    if str(MOLTRANS_DIR) not in sys.path:
        sys.path.insert(0, str(MOLTRANS_DIR))
    existing = sys.modules.get("models")
    if existing:
        del sys.modules["models"]
    cwd = os.getcwd()
    os.chdir(MOLTRANS_DIR)
    try:
        from config import BIN_config_DBPE
        from models import BIN_Interaction_Flat
        from stream import BIN_Data_Encoder
    finally:
        os.chdir(cwd)
    return BIN_config_DBPE, BIN_Interaction_Flat, BIN_Data_Encoder


def _get_device(device: str | None = None) -> torch.device:
    if device:
        return torch.device(device)
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _make_loader(df: pd.DataFrame, batch_size: int, shuffle: bool, drop_last: bool) -> data.DataLoader:
    _, _, BIN_Data_Encoder = _import_moltrans()
    labels = df["Label"].astype(float).values
    dataset = BIN_Data_Encoder(df.index.values, labels, df)
    params = {
        "batch_size": batch_size,
        "shuffle": shuffle,
        "num_workers": 0,
        "drop_last": drop_last,
    }
    return data.DataLoader(dataset, **params)


def train_moltrans(data_dir: Path, out_path: Path, epochs: int = 10, batch_size: int = 32, lr: float = 1e-4,
                   device: str | None = None, resume: bool = True, checkpoint_path: Path | None = None) -> Path:
    BIN_config_DBPE, BIN_Interaction_Flat, _ = _import_moltrans()
    device = _get_device(device)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if checkpoint_path is None:
        checkpoint_path = out_path.parent / "checkpoint.pt"

    start_epoch = 0
    best_auc = -1.0

    ckpt = None
    if resume and checkpoint_path.exists():
        ckpt = torch.load(checkpoint_path, map_location="cpu")
        if isinstance(ckpt, dict) and "epoch" in ckpt:
            start_epoch = int(ckpt.get("epoch", 0)) + 1
            best_auc = float(ckpt.get("best_auc", -1.0))
            ckpt_batch = ckpt.get("batch_size")
            if ckpt_batch is not None and ckpt_batch != batch_size:
                batch_size = int(ckpt_batch)

    config = BIN_config_DBPE()
    config["batch_size"] = batch_size

    # MolTrans assumes DataParallel if multiple GPUs are visible; force single-GPU behavior.
    torch.cuda.device_count = lambda: 1
    model = BIN_Interaction_Flat(**config).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fct = torch.nn.BCELoss()
    sigmoid = torch.nn.Sigmoid()

    if ckpt and "model_state" in ckpt:
        model.load_state_dict(ckpt["model_state"])
    if ckpt and "optim_state" in ckpt:
        opt.load_state_dict(ckpt["optim_state"])

    if start_epoch >= epochs:
        if not out_path.exists() and ckpt and "model_state" in ckpt:
            torch.save(ckpt["model_state"], out_path)
        return out_path

    df_train = pd.read_csv(data_dir / "train.csv")
    df_val = pd.read_csv(data_dir / "val.csv")

    train_loader = _make_loader(df_train, batch_size, shuffle=True, drop_last=True)
    val_loader = _make_loader(df_val, batch_size, shuffle=False, drop_last=True)

    best_state = None

    for epoch in range(start_epoch, epochs):
        model.train()
        for d, p, d_mask, p_mask, label in train_loader:
            d = d.long().to(device)
            p = p.long().to(device)
            d_mask = d_mask.long().to(device)
            p_mask = p_mask.long().to(device)
            label_t = torch.tensor(label, dtype=torch.float32, device=device)

            score = model(d, p, d_mask, p_mask)
            logits = torch.squeeze(sigmoid(score))
            loss = loss_fct(logits, label_t)

            opt.zero_grad()
            loss.backward()
            opt.step()

        # validation
        model.eval()
        y_true = []
        y_pred = []
        with torch.no_grad():
            for d, p, d_mask, p_mask, label in val_loader:
                d = d.long().to(device)
                p = p.long().to(device)
                d_mask = d_mask.long().to(device)
                p_mask = p_mask.long().to(device)
                score = model(d, p, d_mask, p_mask)
                logits = torch.squeeze(sigmoid(score)).detach().cpu().numpy()
                y_pred.extend(logits.tolist())
                y_true.extend(np.asarray(label).flatten().tolist())

        if len(set(y_true)) > 1:
            auc = roc_auc_score(y_true, y_pred)
        else:
            auc = 0.0
        if auc > best_auc:
            best_auc = auc
            best_state = model.state_dict()
            torch.save(best_state, out_path)

        torch.save(
            {
                "epoch": epoch,
                "best_auc": best_auc,
                "model_state": model.state_dict(),
                "optim_state": opt.state_dict(),
                "batch_size": batch_size,
            },
            checkpoint_path,
        )

    if best_state is None:
        if out_path.exists():
            return out_path
        best_state = model.state_dict()
        torch.save(best_state, out_path)
        return out_path
    torch.save(best_state, out_path)
    return out_path


def predict_moltrans(smiles: List[str], protein_seq: str, checkpoint_path: Path, batch_size: int = 64,
                     device: str | None = None) -> List[float]:
    scores = predict_moltrans_pairs(
        smiles=smiles,
        proteins=[protein_seq] * len(smiles),
        checkpoint_path=checkpoint_path,
        batch_size=batch_size,
        device=device,
    )
    return scores


def load_moltrans_model(checkpoint_path: Path, batch_size: int = 64,
                        device: str | None = None) -> torch.nn.Module:
    BIN_config_DBPE, BIN_Interaction_Flat, _ = _import_moltrans()
    device = _get_device(device)
    config = BIN_config_DBPE()
    config["batch_size"] = batch_size
    # MolTrans assumes DataParallel if multiple GPUs are visible; force single-GPU behavior.
    torch.cuda.device_count = lambda: 1
    model = BIN_Interaction_Flat(**config).to(device)
    model.load_state_dict(torch.load(checkpoint_path, map_location=device))
    model.eval()
    return model


def predict_moltrans_pairs(
    smiles: List[str],
    proteins: List[str],
    checkpoint_path: Path,
    batch_size: int = 64,
    device: str | None = None,
    model: torch.nn.Module | None = None,
) -> List[float]:
    if len(smiles) != len(proteins):
        raise ValueError("smiles and proteins must have the same length")
    device = _get_device(device)
    if model is None:
        model = load_moltrans_model(checkpoint_path, batch_size=batch_size, device=device)

    df = pd.DataFrame({
        "SMILES": smiles,
        "Target Sequence": proteins,
        "Label": [0] * len(smiles),
    })
    # Pad to avoid partial batches because MolTrans assumes fixed batch size.
    pad = (-len(df)) % batch_size
    if pad:
        df = pd.concat([df, df.iloc[:pad]], ignore_index=True)
    loader = _make_loader(df, batch_size, shuffle=False, drop_last=False)

    sigmoid = torch.nn.Sigmoid()
    scores: List[float] = []
    with torch.no_grad():
        for d, p, d_mask, p_mask, _ in loader:
            d = d.long().to(device)
            p = p.long().to(device)
            d_mask = d_mask.long().to(device)
            p_mask = p_mask.long().to(device)
            out = model(d, p, d_mask, p_mask)
            logits = torch.squeeze(sigmoid(out)).detach().cpu().numpy().tolist()
            if isinstance(logits, float):
                scores.append(logits)
            else:
                scores.extend(logits)
    if pad:
        scores = scores[:-pad]
    return scores
