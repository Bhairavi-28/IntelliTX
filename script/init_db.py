"""Create the schema (and seed rows). WARNING: drops and recreates the IntelliTX tables.

    python -m scripts.init_db            # schema + seed
    python -m scripts.init_db --no-seed
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backend import database  # noqa: E402

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-seed", action="store_true")
    a = ap.parse_args()
    database.init_schema(with_seed=not a.no_seed)
    ok, ver = database.ping()
    print(f"schema created on MySQL {ver}")
