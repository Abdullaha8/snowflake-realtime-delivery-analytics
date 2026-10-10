"""Score LIVE orders with the default ETA model version (same code path as the DAG's score_eta).

python ml/score.py
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "airflow"))
warnings.filterwarnings("ignore", module="snowflake.connector.vendored.requests")

import sf  # noqa: E402
from _env import load_dotenv  # noqa: E402
from include.fooddel.eta_scoring import score_new_orders  # noqa: E402


def main() -> int:
    load_dotenv()
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE ROLE FOODDEL_TRANSFORM")
        cur.execute("USE WAREHOUSE WH_TRANSFORM")
        print(score_new_orders(cur, os.environ["SNOWFLAKE_DATABASE"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
