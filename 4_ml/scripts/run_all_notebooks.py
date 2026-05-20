"""Execute all ML notebooks in order (requires jupyter + nbconvert)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

NOTEBOOKS = [
    "01_eda.ipynb",
    "02_feature_engineering.ipynb",
    "03_model_training.ipynb",
    "04_shap_explainability.ipynb",
    "05_model_selection_report.ipynb",
]


def main() -> int:
    ml_root = Path(__file__).resolve().parents[1]
    notebook_dir = ml_root / "notebooks"
    failed = []
    for name in NOTEBOOKS:
        path = notebook_dir / name
        print(f"\n{'=' * 60}\nExecuting {name}\n{'=' * 60}", flush=True)
        cmd = [
            sys.executable,
            "-m",
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            str(path),
            "--output",
            name,
            "--ExecutePreprocessor.timeout=3600",
        ]
        result = subprocess.run(cmd, cwd=str(ml_root))
        if result.returncode != 0:
            failed.append(name)
    if failed:
        print(f"\nFailed notebooks: {failed}", file=sys.stderr)
        return 1
    print("\nAll notebooks executed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
