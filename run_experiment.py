from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.experiment import run_experiment

if __name__ == "__main__":
    df = run_experiment(ROOT / "results")
    print(df.groupby(["strategy", "k"])[["row_coverage", "information_loss", "relative_sum_error"]].mean())
