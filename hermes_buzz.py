"""Command-line entry point for the optional Hermes and Buzz runtime adapter."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.hermes_buzz_adapter import main


if __name__ == "__main__":
    main()
