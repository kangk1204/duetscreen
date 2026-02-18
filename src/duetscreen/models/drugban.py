from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import torch
from rdkit import Chem
from sklearn.metrics import average_precision_score, roc_auc_score
from torch.utils.data import DataLoader

from duetscreen.config import THIRD_PARTY_DIR

DRUGBAN_DIR = THIRD_PARTY_DIR / "DrugBAN"


def _import_drugban():
    if str(DRUGBAN_DIR) not in sys.path:
        sys.path.insert(0, str(DRUGBAN_DIR))
    for name in ("models", "dataloader", "utils", "configs"):
        existing = sys.modules.get(name)
        if not existing:
            continue
        origin = getattr(existing, "__file__", "") or ""
        if str(DRUGBAN_DIR) not in origin:
            del sys.modules[name]
        else:
            del sys.modules[name]
    from configs import get_cfg_defaults
    from dataloader import DTIDataset
    from models import DrugBAN, binary_cross_entropy
    from utils import graph_collate_func
    return get_cfg_defaults, DTIDataset, DrugBAN, binary_cross_entropy, graph_collate_func


def _get_device(device: str | None = None) -> torch.device:
    if device:
        return torch.device(device)
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _evaluate(model, loader, device, loss_fn) -> Tuple[float, float, float]:
    model.eval()
    y_true = []
    y_pred = []
    losses = []
    with torch.no_grad():
        for v_d, v_p, labels in loader:
            v_d = v_d.to(device)
            v_p = v_p.to(device)
            labels = labels.float().to(device)
            _, _, _, score = model(v_d, v_p)
            preds, loss = loss_fn(score, labels)
            y_true.extend(labels.detach().cpu().numpy().tolist())
            y_pred.extend(preds.detach().cpu().numpy().tolist())
            losses.append(loss.item())
    if len(set(y_true)) > 1:
        auc = roc_auc_score(y_true, y_pred)
        auprc = average_precision_score(y_true, y_pred)
    else:
        auc = 0.0
        auprc = 0.0
    val_loss = float(np.mean(losses)) if losses else 0.0
    return auc, auprc, val_loss


def _filter_invalid_smiles(df: pd.DataFrame) -> pd.DataFrame:
    mask = []
    for smi in df["SMILES"].astype(str).values:
        mask.append(Chem.MolFromSmiles(smi) is not None)
    return df.loc[mask].reset_index(drop=True)


def train_drugban(
    data_dir: Path,
    out_dir: Path,
    epochs: int = 10,
    batch_size: int = 64,
    lr: float = 5e-5,
    device: str | None = None,
    resume: bool = True,
    checkpoint_path: Path | None = None,
) -> Path:
    get_cfg_defaults, DTIDataset, DrugBAN, binary_cross_entropy, graph_collate_func = _import_drugban()
    device = _get_device(device)

    out_dir.mkdir(parents=True, exist_ok=True)
    if checkpoint_path is None:
        checkpoint_path = out_dir / "checkpoint.pt"
    best_path = out_dir / "best.pth"

    start_epoch = 0
    best_auc = -1.0
    ckpt = None
    if resume and checkpoint_path.exists():
        ckpt = torch.load(checkpoint_path, map_location="cpu")
        start_epoch = int(ckpt.get("epoch", 0)) + 1
        best_auc = float(ckpt.get("best_auc", -1.0))
        ckpt_batch = ckpt.get("batch_size")
        if ckpt_batch is not None and ckpt_batch != batch_size:
            batch_size = int(ckpt_batch)

    cfg = get_cfg_defaults()
    cfg.SOLVER.MAX_EPOCH = epochs
    cfg.SOLVER.BATCH_SIZE = batch_size
    cfg.SOLVER.LR = lr

    model = DrugBAN(**cfg).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.SOLVER.LR)

    if ckpt and "model_state" in ckpt:
        model.load_state_dict(ckpt["model_state"])
    if ckpt and "optim_state" in ckpt:
        opt.load_state_dict(ckpt["optim_state"])

    if start_epoch >= epochs:
        if not best_path.exists() and ckpt and "model_state" in ckpt:
            torch.save(ckpt["model_state"], best_path)
        return best_path

    df_train = pd.read_csv(data_dir / "train.csv")
    df_val = pd.read_csv(data_dir / "val.csv")
    df_test = pd.read_csv(data_dir / "test.csv")

    df_train = _filter_invalid_smiles(df_train)
    df_val = _filter_invalid_smiles(df_val)
    df_test = _filter_invalid_smiles(df_test)

    train_dataset = DTIDataset(df_train.index.values, df_train)
    val_dataset = DTIDataset(df_val.index.values, df_val)
    test_dataset = DTIDataset(df_test.index.values, df_test)

    params = {
        "batch_size": batch_size,
        "shuffle": True,
        "num_workers": 0,
        "drop_last": True,
        "collate_fn": graph_collate_func,
    }
    train_loader = DataLoader(train_dataset, **params)
    params["shuffle"] = False
    params["drop_last"] = False
    val_loader = DataLoader(val_dataset, **params)
    test_loader = DataLoader(test_dataset, **params)

    loss_fn = binary_cross_entropy

    for epoch in range(start_epoch, epochs):
        model.train()
        for v_d, v_p, labels in train_loader:
            v_d = v_d.to(device)
            v_p = v_p.to(device)
            labels = labels.float().to(device)
            opt.zero_grad()
            _, _, _, score = model(v_d, v_p)
            _, loss = loss_fn(score, labels)
            loss.backward()
            opt.step()

        val_auc, val_auprc, val_loss = _evaluate(model, val_loader, device, loss_fn)
        if val_auc > best_auc:
            best_auc = val_auc
            torch.save(model.state_dict(), best_path)

        torch.save(
            {
                "epoch": epoch,
                "best_auc": best_auc,
                "model_state": model.state_dict(),
                "optim_state": opt.state_dict(),
                "batch_size": batch_size,
                "val_auc": val_auc,
                "val_auprc": val_auprc,
                "val_loss": val_loss,
            },
            checkpoint_path,
        )

    if not best_path.exists():
        torch.save(model.state_dict(), best_path)

    # Run a final test pass to ensure the model works after resume.
    _evaluate(model, test_loader, device, loss_fn)

    return best_path


def predict_drugban(
    smiles: List[str],
    protein_seq: str,
    checkpoint_path: Path,
    batch_size: int = 64,
    device: str | None = None,
) -> List[float]:
    scores = predict_drugban_pairs(
        smiles=smiles,
        proteins=[protein_seq] * len(smiles),
        checkpoint_path=checkpoint_path,
        batch_size=batch_size,
        device=device,
    )
    return scores


def load_drugban_model(checkpoint_path: Path, device: str | None = None):
    get_cfg_defaults, _, DrugBAN, _, _ = _import_drugban()
    device = _get_device(device)
    cfg = get_cfg_defaults()
    model = DrugBAN(**cfg).to(device)
    model.load_state_dict(torch.load(checkpoint_path, map_location=device))
    model.eval()
    return model


def predict_drugban_pairs(
    smiles: List[str],
    proteins: List[str],
    checkpoint_path: Path,
    batch_size: int = 64,
    device: str | None = None,
    model=None,
) -> List[float]:
    if len(smiles) != len(proteins):
        raise ValueError("smiles and proteins must have the same length")

    get_cfg_defaults, DTIDataset, DrugBAN, _, graph_collate_func = _import_drugban()
    device = _get_device(device)

    valid_idx = []
    valid_smiles = []
    valid_proteins = []
    for idx, (smi, prot) in enumerate(zip(smiles, proteins)):
        if Chem.MolFromSmiles(smi) is not None:
            valid_idx.append(idx)
            valid_smiles.append(smi)
            valid_proteins.append(prot)

    if not valid_smiles:
        return [0.0] * len(smiles)

    df = pd.DataFrame({
        "SMILES": valid_smiles,
        "Protein": valid_proteins,
        "Y": [0] * len(valid_smiles),
    })
    dataset = DTIDataset(df.index.values, df)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=False, num_workers=0,
                        drop_last=False, collate_fn=graph_collate_func)

    if model is None:
        cfg = get_cfg_defaults()
        model = DrugBAN(**cfg).to(device)
        model.load_state_dict(torch.load(checkpoint_path, map_location=device))
        model.eval()

    scores: List[float] = []
    sigmoid = torch.nn.Sigmoid()
    with torch.no_grad():
        for v_d, v_p, _ in loader:
            v_d = v_d.to(device)
            v_p = v_p.to(device)
            _, _, score, _ = model(v_d, v_p, mode="eval")
            preds = torch.squeeze(sigmoid(score)).detach().cpu().numpy().tolist()
            if isinstance(preds, float):
                scores.append(preds)
            else:
                scores.extend(preds)
    if len(scores) != len(valid_smiles):
        raise RuntimeError(f"DrugBAN returned {len(scores)} scores for {len(valid_smiles)} smiles")
    output = [0.0] * len(smiles)
    for idx, score in zip(valid_idx, scores):
        output[idx] = score
    return output
