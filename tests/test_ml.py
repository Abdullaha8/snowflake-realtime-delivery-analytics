"""ETA model logic, offline: features, metrics, baselines, promotion rule, scoring SQL."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ml"))
sys.path.insert(0, str(ROOT / "airflow"))

import features as F  # noqa: E402
from include.fooddel.eta_scoring import scoring_sql  # noqa: E402

# Columns that only exist after the order is placed: using any of them would be leakage.
POST_PLACEMENT = {
    "ACCEPTED_TS", "DRIVER_ASSIGNED_TS", "FOOD_READY_TS", "PICKED_UP_TS", "DELIVERED_TS",
    "ACTUAL_PREP_MIN", "ACTUAL_DELIVERY_MIN", "DRIVER_ID", "ASSIGN_ATTEMPTS", "ORDER_STATUS",
    "SLA_BREACHED", "MINUTES_VS_PROMISE", "PROMISED_ETA_MIN",
}  # fmt: skip


def test_model_inputs_are_known_at_placement():
    inputs = set(F.NUMERIC) | set(F.CATEGORICAL)
    assert not inputs & POST_PLACEMENT
    assert F.TARGET not in inputs


def test_dbt_feature_model_provides_every_input():
    sql = (ROOT / "dbt" / "models" / "ml" / "ml_eta_features.sql").read_text()
    for col in [*F.NUMERIC, *F.CATEGORICAL, F.TARGET, "PLACED_HOUR_IST", "SPLIT"]:
        assert col in sql, col


def test_feature_columns_drop_and_validation():
    numeric, categorical = F.feature_columns(["DISTANCE_KM", "CUISINE"])
    assert "DISTANCE_KM" not in numeric and categorical == ["ZONE_ID"]
    with pytest.raises(ValueError, match="unknown"):
        F.feature_columns(["NOT_A_FEATURE"])


def test_regression_metrics():
    m = F.regression_metrics([10, 20, 30], [12, 18, 30])
    assert m["mae"] == pytest.approx(4 / 3)
    assert m["bias"] == pytest.approx(0.0)
    assert m["rmse"] == pytest.approx(np.sqrt(8 / 3))


def test_zone_hour_baseline_with_fallback():
    train = pd.DataFrame(
        {"ZONE_ID": ["A", "A", "B"], "PLACED_HOUR_IST": [12, 12, 20], F.TARGET: [30.0, 40.0, 50.0]}
    )
    other = pd.DataFrame({"ZONE_ID": ["A", "B", "C"], "PLACED_HOUR_IST": [12, 20, 1]})
    assert F.zone_hour_baseline(train, other).tolist() == [35.0, 50.0, 40.0]  # C -> global mean


def test_promotion_is_strictly_better_only():
    assert F.should_promote(4.0, None)
    assert F.should_promote(4.0, 4.5)
    assert not F.should_promote(4.5, 4.5)
    assert not F.should_promote(5.6, 4.3)


def test_pipeline_learns_on_toy_data():
    rng = np.random.default_rng(0)
    n = 400
    df = pd.DataFrame({c: rng.normal(size=n) for c in F.NUMERIC})
    df["ZONE_ID"] = rng.choice(["A", "B"], n)
    df["CUISINE"] = rng.choice(["X", "Y"], n)
    y = 30 + 5 * df["DISTANCE_KM"] + np.where(df["ZONE_ID"] == "A", 3, -3)
    params = {**F.PARAM_GRID[0], "n_estimators": 50}
    pipe = F.build_pipeline(F.NUMERIC, F.CATEGORICAL, params).fit(df, y)
    pred = pipe.predict(df.assign(ZONE_ID="UNSEEN"))  # unknown categories must not crash
    assert len(pred) == n
    assert (
        F.regression_metrics(y, pipe.predict(df))["mae"]
        < F.regression_metrics(y, np.full(n, y.mean()))["mae"]
    )


def test_scoring_sql_is_idempotent_and_versioned():
    sql = scoring_sql("FOODDEL_DEV", "V3", ["DISTANCE_KM", "ZONE_ID"], "output_feature_0", ["LIVE"])
    assert (
        'MODEL(FOODDEL_DEV.ML.ETA_XGB, V3)!PREDICT(F.DISTANCE_KM, F.ZONE_ID):"output_feature_0"'
        in sql
    )
    assert "NOT EXISTS" in sql and "P.MODEL_VERSION = 'V3'" in sql
    assert "F.SPLIT IN ('LIVE')" in sql
