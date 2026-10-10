"""Train the ETA model, log it to the Snowflake Model Registry, promote it only if it wins.

    python ml/train.py                         # all features
    python ml/train.py --drop ZONE_ORDERS_PREV_30M,RESTAURANT_ORDERS_PREV_30M   # a challenger

1. Pull ML.ML_ETA_FEATURES (delivered orders, TRAIN/VALID/TEST) with Snowpark - a few MB.
2. Grid-search a small XGBoost grid on TRAIN, pick the best by VALID MAE, refit on TRAIN+VALID.
3. Score TEST once; compare with three baselines (promised ETA, rule ETA, zone x hour mean).
4. Log the sklearn Pipeline (one-hot + XGBoost) to the registry as a new version with metrics.
5. Champion/challenger: make it the default version only if its TEST MAE beats the current one.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
warnings.filterwarnings("ignore", module="snowflake.connector.vendored.requests")

import pandas as pd  # noqa: E402
from _env import load_dotenv  # noqa: E402
from features import (  # noqa: E402
    MODEL_NAME,
    PARAM_GRID,
    TARGET,
    baselines,
    build_pipeline,
    feature_columns,
    regression_metrics,
    should_promote,
)


def snowpark_session():  # noqa: ANN201
    from snowflake.snowpark import Session

    return Session.builder.configs(
        {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_ADMIN_USER"],
            "private_key_file": str(ROOT / os.environ["SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH"]),
            "role": "FOODDEL_TRANSFORM",
            "warehouse": "WH_TRANSFORM",
            "database": os.environ["SNOWFLAKE_DATABASE"],
            "schema": "ML",
            "session_parameters": {"QUERY_TAG": "fooddel-ml-train"},
        }
    ).create()


def load_frames(session) -> dict:  # noqa: ANN001
    from snowflake.snowpark.functions import col

    df = (
        session.table("ML_ETA_FEATURES")
        .filter(col("SPLIT").isin("TRAIN", "VALID", "TEST") & col(TARGET).is_not_null())
        .to_pandas()
    )
    return {s: df[df["SPLIT"] == s].reset_index(drop=True) for s in ("TRAIN", "VALID", "TEST")}


def next_version(reg) -> tuple[str, str | None]:  # noqa: ANN001
    """('V<n+1>', current default version name or None)."""
    try:
        model = reg.get_model(MODEL_NAME)
    except Exception:  # noqa: BLE001 - first version
        return "V1", None
    names = [v.version_name for v in model.versions()]
    n = max(int(v[1:]) for v in names if v[1:].isdigit()) + 1
    return f"V{n}", model.default.version_name


def main(argv: list[str]) -> int:
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--drop", default="", help="comma-separated features to leave out")
    args = p.parse_args(argv)
    numeric, categorical = feature_columns([c for c in args.drop.split(",") if c])
    inputs = numeric + categorical

    session = snowpark_session()
    t0 = time.time()
    frames = load_frames(session)
    train, valid, test = frames["TRAIN"], frames["VALID"], frames["TEST"]
    print(
        f"rows: train={len(train):,} valid={len(valid):,} test={len(test):,} ({time.time() - t0:.1f}s)"
    )

    best = None
    for params in PARAM_GRID:
        pipe = build_pipeline(numeric, categorical, params).fit(train[inputs], train[TARGET])
        mae = regression_metrics(valid[TARGET], pipe.predict(valid[inputs]))["mae"]
        print(f"  depth={params['max_depth']} trees={params['n_estimators']}: valid MAE {mae:.3f}")
        if best is None or mae < best[0]:
            best = (mae, params)
    valid_mae, params = best

    train_valid = pd.concat([train, valid], ignore_index=True)
    pipe = build_pipeline(numeric, categorical, params).fit(
        train_valid[inputs], train_valid[TARGET]
    )
    test_pred = pipe.predict(test[inputs])
    m = regression_metrics(test[TARGET], test_pred)
    base = baselines(train_valid, test)
    print(f"\nTEST (Oct 1-9, {len(test):,} delivered orders)")
    print(
        f"  model             MAE {m['mae']:.3f}  p90|err| {m['p90_abs_err']:.3f}  bias {m['bias']:+.3f}"
    )
    print(f"  promised ETA      MAE {base['baseline_promised_mae']:.3f}")
    print(f"  rule (promise-12) MAE {base['baseline_rule_mae']:.3f}")
    print(f"  zone x hour mean  MAE {base['baseline_zone_hour_mae']:.3f}")

    importances = sorted(
        zip(pipe.named_steps["prep"].get_feature_names_out(), pipe.named_steps["xgb"].feature_importances_, strict=True),
        key=lambda t: -t[1],
    )  # fmt: skip
    print("  top features:", ", ".join(f"{n}={v:.2f}" for n, v in importances[:6]))

    from snowflake.ml.registry import Registry

    reg = Registry(
        session=session, database_name=os.environ["SNOWFLAKE_DATABASE"], schema_name="ML"
    )
    version, champion = next_version(reg)
    metrics = {
        "test_mae": m["mae"], "test_p90_abs_err": m["p90_abs_err"], "test_bias": m["bias"],
        "valid_mae": valid_mae, **base, "params": params, "features": inputs,
    }  # fmt: skip
    import sklearn
    import xgboost

    mv = reg.log_model(
        pipe,
        model_name=MODEL_NAME,
        version_name=version,
        sample_input_data=test[inputs].head(100),
        conda_dependencies=[
            f"xgboost=={xgboost.__version__}",
            f"scikit-learn=={sklearn.__version__}",
        ],
        target_platforms=["WAREHOUSE"],
        metrics=metrics,
        comment=f"ETA XGBoost pipeline; test MAE {m['mae']:.3f}; features: {','.join(inputs)}",
    )
    output = mv.show_functions()[0]["signature"].outputs[0].name
    champion_mae = None
    if champion:
        champion_mae = reg.get_model(MODEL_NAME).version(champion).show_metrics().get("test_mae")
    promote = should_promote(m["mae"], champion_mae)
    if promote:
        reg.get_model(MODEL_NAME).default = version

    session.sql(
        "INSERT INTO MODEL_VERSIONS (MODEL_NAME, VERSION_NAME, TRAIN_ROWS, TEST_ROWS, VALID_MAE,"
        " TEST_MAE, TEST_P90_ABS_ERR, TEST_BIAS, BASELINE_PROMISED_MAE, BASELINE_RULE_MAE,"
        " BASELINE_ZONE_HOUR_MAE, PROMOTED, FEATURES, OUTPUT_COLUMN, PARAMS)"
        " SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?)",
        params=[
            MODEL_NAME, version, len(train_valid), len(test), valid_mae, m["mae"], m["p90_abs_err"],
            m["bias"], base["baseline_promised_mae"], base["baseline_rule_mae"],
            base["baseline_zone_hour_mae"], promote, ",".join(inputs), output, json.dumps(params),
        ],
    ).collect()  # fmt: skip
    verdict = (
        "PROMOTED to default"
        if promote
        else f"kept {champion} as default (its test MAE {champion_mae:.3f} is better)"
    )
    print(f"\nlogged {MODEL_NAME} {version} (output field {output!r}): {verdict}")
    session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
