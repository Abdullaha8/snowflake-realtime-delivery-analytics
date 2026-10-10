"""In-warehouse ETA scoring with the Model Registry model (pure SQL, no ML libraries needed).

Used by the fooddel_pipeline DAG (score_eta task) and by `make ml-score` / ml/backtest.py.
The default model version, its input columns and output field come from ML.MODEL_VERSIONS,
which ml/train.py writes when it logs a version.
"""

from __future__ import annotations

MODEL = "ETA_XGB"


def _rows(cur) -> list[dict]:  # noqa: ANN001
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def default_version(cur, db: str) -> dict | None:  # noqa: ANN001
    """The registry's default version joined with its feature list (None if no model yet)."""
    try:
        cur.execute(f"SHOW VERSIONS IN MODEL {db}.ML.{MODEL}")
    except Exception:  # noqa: BLE001 - model not created yet
        return None
    versions = _rows(cur)
    default = next(
        (v["name"] for v in versions if str(v["is_default_version"]).lower() == "true"), None
    )
    if default is None:
        return None
    cur.execute(
        f"SELECT FEATURES, OUTPUT_COLUMN FROM {db}.ML.MODEL_VERSIONS"
        " WHERE MODEL_NAME = %s AND VERSION_NAME = %s",
        (MODEL, default),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {"version": default, "features": row[0].split(","), "output": row[1]}


def scoring_sql(db: str, version: str, features: list[str], output: str, splits: list[str]) -> str:
    args = ", ".join(f"F.{c}" for c in features)
    split_list = ", ".join(f"'{s}'" for s in splits)
    return (
        f"INSERT INTO {db}.ML.ETA_PREDICTIONS"
        " (ORDER_ID, MODEL_NAME, MODEL_VERSION, PREDICTED_ETA_MIN, PROMISED_ETA_MIN)"
        f" SELECT F.ORDER_ID, '{MODEL}', '{version}',"
        f' MODEL({db}.ML.{MODEL}, {version})!PREDICT({args}):"{output}"::FLOAT,'
        " F.PROMISED_ETA_MIN"
        f" FROM {db}.ML.ML_ETA_FEATURES AS F"
        f" WHERE F.SPLIT IN ({split_list})"
        f" AND NOT EXISTS (SELECT 1 FROM {db}.ML.ETA_PREDICTIONS AS P"
        f" WHERE P.ORDER_ID = F.ORDER_ID AND P.MODEL_VERSION = '{version}')"
    )


def score_new_orders(cur, db: str, splits: tuple[str, ...] = ("LIVE",)) -> dict:  # noqa: ANN001
    """Score every order in `splits` not yet scored by the default version."""
    mv = default_version(cur, db)
    if mv is None:
        return {"scored": 0, "version": None, "note": "no promoted model yet"}
    cur.execute(scoring_sql(db, mv["version"], mv["features"], mv["output"], list(splits)))
    scored = cur.fetchone()[0]
    return {"scored": int(scored), "version": mv["version"]}
