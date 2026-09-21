from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.orchestration_experiment import run_orchestration_experiment

if __name__ == "__main__":
    df = run_orchestration_experiment(ROOT / "results")
    print(df.groupby("mode")[["success", "tested_candidates", "probe_messages", "unsupported_replies"]].mean())
