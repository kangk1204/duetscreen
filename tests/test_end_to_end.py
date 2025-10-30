import csv
import json
from pathlib import Path

from hypervs1000.config import load_config
from hypervs1000.pipeline import (
    run_aggregate,
    run_docking,
    run_dti,
    run_mmgbsa,
    run_prep,
    run_report,
    run_validate,
)


def _write_inputs(path: Path) -> None:
    rows = [
        {"id": "CRBN", "type": "protein", "value": "MMDKEVQKSSSRTPSYQPGVSQSVE"},
        {"id": "LENALIDOMIDE", "type": "ligand", "value": "O=C1C(N(C2=CC=CC=C2)C3=CC=CC=C13)C(=O)N"},
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "type", "value"])
        writer.writeheader()
        writer.writerows(rows)


def _write_config(path: Path, input_path: Path, workdir: Path) -> None:
    config = {
        "pipeline": {
            "chunk_size": 1,
            "num_workers": 1,
            "devices": [0],
            "simulator": True,
            "dti_top_k": 3,
            "docking_top_k": 2,
            "mmgbsa_top_k": 2,
            "consensus_constant": 10,
            "stage_weights": {"dti": 0.5, "docking": 0.3, "mmgbsa": 0.2},
        },
        "inputs": {"sequences": str(input_path)},
        "library": {
            "proteins": [
                {"id": "IKZF1", "sequence": "MPLGKKAKLPEKKAPVTPQLPQLQ"},
                {"id": "IKZF3", "sequence": "MKRLILPSEADVEQIPKPKKKPL"},
            ],
            "ligands": [
                {"id": "LENALIDOMIDE", "smiles": "O=C1C(N(C2=CC=CC=C2)C3=CC=CC=C13)C(=O)N"},
                {"id": "POMALIDOMIDE", "smiles": "O=C1NC(C(=O)N)C2=CC=CC=C2N1"},
            ],
        },
        "paths": {
            "workdir": str(workdir),
            "manifest": str(workdir / "MANIFEST.json"),
            "reports": str(workdir / "reports"),
        },
    }
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")


def test_end_to_end_pipeline(tmp_path):
    input_csv = tmp_path / "inputs.csv"
    _write_inputs(input_csv)
    workdir = tmp_path / "workspace"
    config_path = tmp_path / "config.yaml"
    _write_config(config_path, input_csv, workdir)

    config = load_config(config_path)
    run_validate(config)
    run_prep(config)
    run_dti(config)
    run_docking(config)
    run_mmgbsa(config)
    aggregate_path = run_aggregate(config)
    report_path = run_report(config)

    assert aggregate_path.exists()
    assert report_path.exists()

    report_json = json.loads((workdir / "reports" / "report.json").read_text(encoding="utf-8"))
    assert report_json["inputs"]
    for entry in report_json["inputs"]:
        assert entry["partners"]
        scores = entry["partners"][0]["scores"]
        assert scores["dti"] is not None
        assert "consensus_score" in entry["partners"][0]

    final_txt = (workdir / "reports" / "report.txt").read_text(encoding="utf-8")
    assert "HyperVS1000 Report" in final_txt
