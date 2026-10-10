"""Backtest the default ETA model INSIDE Snowflake and check it matches the offline numbers.

Scores the TEST split with MODEL(ML.ETA_XGB, <default>)!PREDICT in the warehouse, then
compares the in-warehouse MAE with the TEST MAE that ml/train.py recorded offline. They must
agree (same model, same rows), proving the registry model behaves exactly like the local one.

    python ml/backtest.py
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
from include.fooddel.eta_scoring import default_version, score_new_orders  # noqa: E402

TOLERANCE = 0.01  # minutes


def main() -> int:
    load_dotenv()
    db = os.environ["SNOWFLAKE_DATABASE"]
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE ROLE FOODDEL_TRANSFORM")
        cur.execute("USE WAREHOUSE WH_TRANSFORM")
        mv = default_version(cur, db)
        if mv is None:
            print("no promoted model: run `make ml-train` first")
            return 1
        result = score_new_orders(cur, db, splits=("TEST",))
        print(f"scored {result['scored']:,} TEST orders in-warehouse with {mv['version']}")
        cur.execute(
            f"SELECT COUNT(*), AVG(ABS(P.PREDICTED_ETA_MIN - F.ACTUAL_DELIVERY_MIN)),"
            f" AVG(ABS(P.PROMISED_ETA_MIN - F.ACTUAL_DELIVERY_MIN))"
            f" FROM {db}.ML.ETA_PREDICTIONS AS P"
            f" JOIN {db}.ML.ML_ETA_FEATURES AS F ON P.ORDER_ID = F.ORDER_ID"
            f" WHERE F.SPLIT = 'TEST' AND F.ACTUAL_DELIVERY_MIN IS NOT NULL AND P.MODEL_VERSION = %s",
            (mv["version"],),
        )
        n, wh_mae, promised_mae = cur.fetchone()
        cur.execute(
            f"SELECT TEST_MAE FROM {db}.ML.MODEL_VERSIONS WHERE VERSION_NAME = %s", (mv["version"],)
        )
        offline_mae = cur.fetchone()[0]
    diff = abs(float(wh_mae) - float(offline_mae))
    print(f"  delivered TEST orders  {n:,}")
    print(f"  in-warehouse MAE       {float(wh_mae):.4f}")
    print(f"  offline (train.py) MAE {float(offline_mae):.4f}   diff {diff:.4f}")
    print(f"  promised ETA MAE       {float(promised_mae):.4f}")
    ok = diff <= TOLERANCE
    print("PASS: registry model reproduces the offline result" if ok else "FAIL: mismatch")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
