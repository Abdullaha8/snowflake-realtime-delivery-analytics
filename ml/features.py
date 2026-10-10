"""ETA model definition: inputs, baselines, metrics, pipeline and promotion rule.

Pure Python (pandas/sklearn/xgboost, no Snowflake) so every piece is unit-tested offline.
Features are computed in SQL by dbt (dbt/models/ml/ml_eta_features.sql); this module only
decides which columns the model sees and how it is judged.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

MODEL_NAME = "ETA_XGB"
TARGET = "ACTUAL_DELIVERY_MIN"
# Every input is known at the moment the order is placed (no pickup/delivery information).
NUMERIC = [
    "HOUR_SIN",
    "HOUR_COS",
    "DAY_OF_WEEK",
    "IS_WEEKEND",
    "IS_MONSOON",
    "IS_RAINING",
    "SURGE_FEE_INR",
    "RESTAURANT_AVG_PREP_MIN",
    "RESTAURANT_RATING",
    "PRICE_BAND",
    "ITEM_LINES",
    "SUBTOTAL_INR",
    "DISTANCE_KM",
    "ZONE_ORDERS_PREV_30M",
    "RESTAURANT_ORDERS_PREV_30M",
]
CATEGORICAL = ["ZONE_ID", "CUISINE"]
# The platform's promise = rule estimate + this fixed buffer (simulator engine.PROMISE_BUFFER_MIN).
PROMISE_BUFFER_MIN = 12
# Small, explainable grid; the best on VALID is refit on TRAIN+VALID and scored once on TEST.
PARAM_GRID = [
    {"max_depth": d, "n_estimators": n, "learning_rate": 0.05, "subsample": 0.8,
     "colsample_bytree": 0.8, "min_child_weight": 5}
    for d in (4, 6) for n in (300, 600)
]  # fmt: skip


def feature_columns(drop: list[str] | None = None) -> tuple[list[str], list[str]]:
    drop = set(drop or [])
    unknown = drop - set(NUMERIC) - set(CATEGORICAL)
    if unknown:
        raise ValueError(f"unknown features to drop: {sorted(unknown)}")
    return [c for c in NUMERIC if c not in drop], [c for c in CATEGORICAL if c not in drop]


def build_pipeline(numeric: list[str], categorical: list[str], params: dict) -> Pipeline:
    """One-hot encoding travels inside the model, so SQL inference can pass raw zone/cuisine."""
    pre = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), categorical)],
        remainder="passthrough",
        verbose_feature_names_out=False,
    )
    model = XGBRegressor(objective="reg:absoluteerror", random_state=42, n_jobs=4, **params)
    return Pipeline([("prep", pre), ("xgb", model)])


def regression_metrics(y_true, y_pred) -> dict[str, float]:
    err = np.asarray(y_pred, dtype=float) - np.asarray(y_true, dtype=float)
    return {
        "mae": float(np.mean(np.abs(err))),
        "p90_abs_err": float(np.percentile(np.abs(err), 90)),
        "bias": float(np.mean(err)),
        "rmse": float(np.sqrt(np.mean(err**2))),
    }


def zone_hour_baseline(train: pd.DataFrame, other: pd.DataFrame) -> np.ndarray:
    """Mean delivery time per (zone, IST hour) learned on train; global mean as fallback."""
    means = train.groupby(["ZONE_ID", "PLACED_HOUR_IST"])[TARGET].mean()
    keys = list(zip(other["ZONE_ID"], other["PLACED_HOUR_IST"], strict=True))
    fallback = float(train[TARGET].mean())
    return np.array([means.get(k, fallback) for k in keys], dtype=float)


def baselines(train: pd.DataFrame, test: pd.DataFrame) -> dict[str, float]:
    y = test[TARGET]
    return {
        "baseline_promised_mae": regression_metrics(y, test["PROMISED_ETA_MIN"])["mae"],
        "baseline_rule_mae": regression_metrics(y, test["PROMISED_ETA_MIN"] - PROMISE_BUFFER_MIN)[
            "mae"
        ],
        "baseline_zone_hour_mae": regression_metrics(y, zone_hour_baseline(train, test))["mae"],
    }


def should_promote(candidate_test_mae: float, champion_test_mae: float | None) -> bool:
    """Champion/challenger: the new version becomes default only if strictly better on TEST."""
    return champion_test_mae is None or candidate_test_mae < champion_test_mae
