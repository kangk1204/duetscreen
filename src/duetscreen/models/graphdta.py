from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import sys
import torch
from rdkit import Chem
from torch.utils.data import Dataset
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader

from duetscreen.config import THIRD_PARTY_DIR

GRAPH_DTA_DIR = THIRD_PARTY_DIR / "GraphDTA"

SEQ_VOC = "ABCDEFGHIKLMNOPQRSTUVWXYZ"
SEQ_DICT = {v: (i + 1) for i, v in enumerate(SEQ_VOC)}
MAX_SEQ_LEN = 1000


def one_of_k_encoding(x, allowable_set):
    if x not in allowable_set:
        raise ValueError(f"input {x} not in allowable set")
    return [x == s for s in allowable_set]


def one_of_k_encoding_unk(x, allowable_set):
    if x not in allowable_set:
        x = allowable_set[-1]
    return [x == s for s in allowable_set]


def atom_features(atom):
    return np.array(
        one_of_k_encoding_unk(atom.GetSymbol(),
                              ['C', 'N', 'O', 'S', 'F', 'Si', 'P', 'Cl', 'Br', 'Mg', 'Na',
                               'Ca', 'Fe', 'As', 'Al', 'I', 'B', 'V', 'K', 'Tl', 'Yb', 'Sb',
                               'Sn', 'Ag', 'Pd', 'Co', 'Se', 'Ti', 'Zn', 'H', 'Li', 'Ge', 'Cu',
                               'Au', 'Ni', 'Cd', 'In', 'Mn', 'Zr', 'Cr', 'Pt', 'Hg', 'Pb', 'Unknown'])
        + one_of_k_encoding(atom.GetDegree(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        + one_of_k_encoding_unk(atom.GetTotalNumHs(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        + one_of_k_encoding_unk(atom.GetImplicitValence(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        + [atom.GetIsAromatic()]
    )


def smile_to_graph(smile: str) -> Tuple[int, np.ndarray, List[List[int]]]:
    mol = Chem.MolFromSmiles(smile)
    if mol is None:
        raise ValueError(f"Invalid SMILES: {smile}")
    c_size = mol.GetNumAtoms()
    features = []
    for atom in mol.GetAtoms():
        feature = atom_features(atom)
        features.append(feature / sum(feature))
    edges = []
    for bond in mol.GetBonds():
        edges.append([bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()])
    # undirected -> directed
    edge_index = []
    for e1, e2 in edges:
        edge_index.append([e1, e2])
        edge_index.append([e2, e1])
    return c_size, np.asarray(features), edge_index


def seq_cat(prot: str) -> np.ndarray:
    x = np.zeros(MAX_SEQ_LEN)
    for i, ch in enumerate(prot[:MAX_SEQ_LEN]):
        x[i] = SEQ_DICT.get(ch, 0)
    return x


class GraphDTADataset(Dataset):
    def __init__(self, smiles: List[str], proteins: List[str], labels: List[float]):
        self.smiles = smiles
        self.proteins = proteins
        self.labels = labels

    def __len__(self) -> int:
        return len(self.smiles)

    def __getitem__(self, idx: int) -> Data:
        smi = self.smiles[idx]
        prot = self.proteins[idx]
        label = self.labels[idx]
        c_size, features, edge_index = smile_to_graph(smi)
        data = Data(
            x=torch.tensor(features, dtype=torch.float),
            edge_index=torch.tensor(edge_index, dtype=torch.long).t().contiguous(),
            y=torch.tensor([label], dtype=torch.float),
        )
        data.target = torch.tensor([seq_cat(prot)], dtype=torch.long)
        data.c_size = torch.tensor([c_size], dtype=torch.long)
        return data


def _filter_invalid_smiles(df: pd.DataFrame, col: str) -> pd.DataFrame:
    mask = []
    for smi in df[col].astype(str).values:
        mask.append(Chem.MolFromSmiles(smi) is not None)
    return df.loc[mask].reset_index(drop=True)


def _get_device(device: str | None = None) -> torch.device:
    if device:
        return torch.device(device)
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _load_model(model_name: str = "gin"):
    if str(GRAPH_DTA_DIR) not in sys.path:
        sys.path.insert(0, str(GRAPH_DTA_DIR))
    existing = sys.modules.get("models")
    if existing:
        origin = getattr(existing, "__file__", "") or ""
        if str(GRAPH_DTA_DIR) not in origin:
            del sys.modules["models"]
    if model_name == "gin":
        from models.ginconv import GINConvNet
        return GINConvNet
    if model_name == "gcn":
        from models.gcn import GCNNet
        return GCNNet
    if model_name == "gat":
        from models.gat import GATNet
        return GATNet
    if model_name == "gat_gcn":
        from models.gat_gcn import GAT_GCN
        return GAT_GCN
    raise ValueError(f"Unknown GraphDTA model: {model_name}")


def train_graphdta(data_dir: Path, out_path: Path, epochs: int = 20, batch_size: int = 256, lr: float = 5e-4,
                   model_name: str = "gin", device: str | None = None, resume: bool = True,
                   checkpoint_path: Path | None = None) -> Path:
    device = _get_device(device)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if checkpoint_path is None:
        checkpoint_path = out_path.parent / "checkpoint.pt"

    start_epoch = 0
    best_loss = float("inf")
    ckpt = None
    if resume and checkpoint_path.exists():
        ckpt = torch.load(checkpoint_path, map_location="cpu")
        if ckpt.get("model_name") != model_name:
            ckpt = None
        else:
            start_epoch = int(ckpt.get("epoch", 0)) + 1
            best_loss = float(ckpt.get("best_loss", best_loss))
            ckpt_batch = ckpt.get("batch_size")
            if ckpt_batch is not None and ckpt_batch != batch_size:
                batch_size = int(ckpt_batch)
    df_train = pd.read_csv(data_dir / "train.csv")
    df_val = pd.read_csv(data_dir / "val.csv")

    df_train = _filter_invalid_smiles(df_train, "compound_iso_smiles")
    df_val = _filter_invalid_smiles(df_val, "compound_iso_smiles")

    train_ds = GraphDTADataset(
        smiles=df_train["compound_iso_smiles"].tolist(),
        proteins=df_train["target_sequence"].tolist(),
        labels=df_train["affinity"].astype(float).tolist(),
    )
    val_ds = GraphDTADataset(
        smiles=df_val["compound_iso_smiles"].tolist(),
        proteins=df_val["target_sequence"].tolist(),
        labels=df_val["affinity"].astype(float).tolist(),
    )

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)

    model_cls = _load_model(model_name)
    model = model_cls().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = torch.nn.MSELoss()

    if ckpt and "model_state" in ckpt:
        model.load_state_dict(ckpt["model_state"])
    if ckpt and "optim_state" in ckpt:
        optimizer.load_state_dict(ckpt["optim_state"])

    if start_epoch >= epochs:
        if not out_path.exists() and ckpt and "model_state" in ckpt:
            torch.save(ckpt["model_state"], out_path)
        return out_path

    best_state = None

    for epoch in range(start_epoch, epochs):
        model.train()
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            loss = loss_fn(output, batch.y.view(-1, 1))
            loss.backward()
            optimizer.step()

        model.eval()
        losses = []
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                output = model(batch)
                loss = loss_fn(output, batch.y.view(-1, 1))
                losses.append(loss.item())
        val_loss = float(np.mean(losses)) if losses else float("inf")
        if val_loss < best_loss:
            best_loss = val_loss
            best_state = model.state_dict()
            torch.save(best_state, out_path)

        torch.save(
            {
                "epoch": epoch,
                "best_loss": best_loss,
                "model_state": model.state_dict(),
                "optim_state": optimizer.state_dict(),
                "batch_size": batch_size,
                "model_name": model_name,
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


def predict_graphdta(smiles: List[str], protein_seq: str, checkpoint_path: Path, batch_size: int = 256,
                     model_name: str = "gin", device: str | None = None) -> List[float]:
    scores = predict_graphdta_pairs(
        smiles=smiles,
        proteins=[protein_seq] * len(smiles),
        checkpoint_path=checkpoint_path,
        batch_size=batch_size,
        model_name=model_name,
        device=device,
    )
    return scores


def load_graphdta_model(checkpoint_path: Path, model_name: str = "gin", device: str | None = None):
    device = _get_device(device)
    model_cls = _load_model(model_name)
    model = model_cls().to(device)
    model.load_state_dict(torch.load(checkpoint_path, map_location=device))
    model.eval()
    return model


def predict_graphdta_pairs(smiles: List[str], proteins: List[str], checkpoint_path: Path, batch_size: int = 256,
                           model_name: str = "gin", device: str | None = None, model=None) -> List[float]:
    if len(smiles) != len(proteins):
        raise ValueError("smiles and proteins must have the same length")

    device = _get_device(device)
    if model is None:
        model = load_graphdta_model(checkpoint_path, model_name=model_name, device=device)

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

    ds = GraphDTADataset(smiles=valid_smiles, proteins=valid_proteins, labels=[0.0] * len(valid_smiles))
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False)

    scores: List[float] = []
    with torch.no_grad():
        for batch in loader:
            batch = batch.to(device)
            output = model(batch).detach().cpu().numpy().flatten().tolist()
            scores.extend(output)
    if len(scores) != len(valid_smiles):
        raise RuntimeError(f"GraphDTA returned {len(scores)} scores for {len(valid_smiles)} smiles")
    output_scores = [0.0] * len(smiles)
    for idx, score in zip(valid_idx, scores):
        output_scores[idx] = score
    return output_scores
