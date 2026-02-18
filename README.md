# DuetScreen

Large-scale DTI screening + docking pipeline for target discovery.

## What it does
- Train **MolTrans**, **DrugBAN**, **GraphDTA** on a shared BindingDB split.
- Download/filter **ZINC22** (resumable) and screen ligands against a target protein.
- Aggregate per-model scores into **top-k intersection** (or union fallback).
- **Proteome screening**: score a single ligand against human proteins.
- **Docking pipeline**: pocket prediction (P2Rank/fpocket) + GNINA/DiffDock + optional MMGBSA.
- **Dual docking**: run **PDB** and **AlphaFold** in one command.
- **Hierarchical screening / pruning** for 100M-scale libraries.

## Requirements
- Linux, Python **>= 3.10** (conda recommended).
- CUDA GPU recommended for training/docking.
- Storage: ZINC22 subsets can be large (tens of GB).

## Install (DTI + screening)
```bash
# 1) Create conda env + install deps + third_party model repos
bash scripts/setup.sh

# Activate env (default: duetscreen or dl)
conda activate duetscreen
```

## Quickstart (CRBN)
```bash
# 1) Download/prepare BindingDB + ZINC subset + CRBN AlphaFold
python -m duetscreen setup-data \
  --bindingdb-split bindingdb \
  --zinc-target-count 200000

# 2) Train DTI models
python -m duetscreen train --model moltrans
python -m duetscreen train --model drugban
python -m duetscreen train --model graphdta

# 3) Screen CRBN and aggregate top-10k
python -m duetscreen screen --protein data/targets/crbn.fasta
python -m duetscreen aggregate --topk 10000
```

## Custom target DTI screening
Provide a FASTA and a ZINC SMILES list. Use `--out-prefix` to keep results separate.
```bash
# Example: CP (ceruloplasmin)
python -m duetscreen screen \
  --protein data/targets/ceruloplasmin_P00450.fasta \
  --zinc-path data/zinc22/purchasable_druglike.smi \
  --out-prefix cp_p00450

python -m duetscreen aggregate --topk 10000 --prefix cp_p00450
```

## Ligand vs proteome (human)
Score a single ligand against the human proteome (DTI).
```bash
python -m duetscreen screen-targets \
  --ligand-name rilpivirine \
  --proteome human \
  --out-prefix rilpivirine_human \
  --chunk-size 512

python -m duetscreen aggregate-targets --prefix rilpivirine_human --topk 1000
```

## Docking setup (GPU)
```bash
# OpenMM + RDKit + PDBFixer + cudnn for docking/MMGBSA
bash scripts/setup_docking_env.sh

# Install P2Rank, GNINA, DiffDock
bash scripts/install_docking_tools.sh
export PATH="$(pwd)/third_party/docking_tools/bin:$PATH"
export DIFFDOCK_DIR="$(pwd)/third_party/DiffDock"
export DIFFDOCK_CONDA_ENV="diffdock"
```

Pocket prediction:
- Default: **P2Rank**
- To force fpocket: `export DUETSCREEN_POCKET_TOOL=fpocket`
- To disable P2Rank (fallback box): `export DUETSCREEN_DISABLE_P2RANK=1`

### Docking (AlphaFold only)
```bash
python -m duetscreen dock \
  --uniprot Q96SW2 \
  --ligands data/results/top_intersection_10000.csv \
  --ligands-format csv \
  --smiles-column smiles \
  --pockets 2 \
  --dockers gnina
```

### Docking (PDB + AlphaFold dual run)
One command runs **PDB** and **AlphaFold** and writes two docking folders.
```bash
# Example: CP with PDB 4ENZ (protein-name maps to UniProt P00450)
python -m duetscreen dock \
  --protein-name CP \
  --pdb-id 4ENZ \
  --ligands data/results/cp_p00450_top_intersection_10000.csv \
  --ligands-format csv \
  --smiles-column smiles \
  --pockets 2 \
  --dockers gnina
```

### Docking with a cleaned PDB (recommended)
```bash
# Clean PDB / select chain
python scripts/prep_receptor_pdb.py \
  --in data/targets/4ENZ.pdb \
  --out data/targets/4ENZ_prepped.pdb \
  --keep-chain A

python -m duetscreen dock \
  --receptor-pdb data/targets/4ENZ_prepped.pdb \
  --ligands data/results/cp_p00450_top_intersection_10000.csv \
  --ligands-format csv \
  --smiles-column smiles \
  --pockets 2 \
  --dockers gnina
```

## Hierarchical screening (fast coarse-to-fine)
Use structural similarity to reduce scoring from ~100M ligands to a staged workflow.
```bash
# Ideal 5-layer preset (10k → 100k → 1M → 10M → 100M reps)
bash scripts/hierarchy_ideal_5layer.sh
```
For custom flows, use:
`duetscreen hierarchy-build`, `hierarchy-select`, `hierarchy-expand`.

## Useful scripts
- `scripts/download_alphafold_human.py`: bulk AlphaFold download (human)
- `scripts/add_positive_control.py`: inject a positive-control ligand
- `scripts/monitor_sdf_eta.py`: live docking progress
- `scripts/optimize_pruning*.py`: pruning strategy search

## Outputs
DTI:
- Model checkpoints: `data/models/`
- Per-model scores: `data/results/*_scores.csv`
- Aggregated top-k: `data/results/*_top_intersection_*.csv`

Docking:
- Work dir: `data/docking/<out_prefix>/`
- GNINA scores: `data/docking/<out_prefix>/gnina/gnina_scores.csv`
- Final merged ranks: `data/docking/<out_prefix>/docking_ranked.csv`

## Tips
- Set GPU explicitly for GNINA: `CUDA_VISIBLE_DEVICES=0`, `GNINA_DEVICE=0`.
- If GNINA fails with CUDA/cudnn errors, ensure `cudnn` is installed in the docking env.
- If P2Rank is missing or slow, switch to fpocket or fallback box.

## One-shot
```bash
python -m duetscreen run-all --topk 10000
```
