# ADR 0006: ETA model, Snowflake Model Registry and MAE tracking

- **Status:** Accepted (Phase 6, 2026-10-10)
- **Scope:** `dbt/models/ml/`, `ml/`, `snowflake/09_ml.sql`, `airflow/include/fooddel/eta_scoring.py`, DAG task `score_eta`

## Context

The platform promises each customer an ETA from a fixed rule: average prep time + travel
estimate + a 12-minute buffer. It ignores load, weather and restaurant behaviour. The business
wants a better prediction, a way to version and serve it inside Snowflake, and ongoing
accuracy tracking that tells someone when it degrades.

## Decisions

### 1. Features in SQL (dbt), only what's known at placement
`dbt/models/ml/ml_eta_features.sql`, one row per order:

| Group | Features |
|---|---|
| Time | cyclical hour (sin/cos), day of week, weekend, monsoon month |
| Context | raining, surge fee, zone |
| Restaurant (point-in-time, from the SCD2 dimension) | average prep, rating, price band, cuisine |
| Order | item lines, subtotal |
| Geography | straight-line distance from coordinates (`ST_DISTANCE`) |
| Real-time load | orders placed in the same zone / restaurant in the **previous 30 minutes** (`RANGE BETWEEN INTERVAL '30 MINUTES' PRECEDING`, current row excluded) |

**No leakage**, enforced three ways:
- A unit test asserts no model input is a post-placement column (pickup, delivery, actual prep, driver, status, and the promise itself).
- Distance comes from coordinates, not from the delivered event.
- A dbt singular test recomputes the load feature with a strict "earlier orders only" self-join on 500 sampled orders, and must match exactly.

### 2. Time-based split
Train Jul 1 – Sep 15, valid Sep 16 – 30, test Oct 1 – 9, by IST placement date (dbt vars). LIVE is everything after. A random split would train on orders placed after the ones it's tested on.

### 3. Model: scikit-learn Pipeline (one-hot → XGBoost), L1 objective
- **One-hot encoding lives inside the logged model**, so SQL inference passes raw `ZONE_ID`/`CUISINE` strings; `handle_unknown="ignore"` keeps a new zone from crashing scoring.
- **Grid:** a small 4-point grid (depth 4/6 × 300/600 trees), choose by VALID MAE, refit on TRAIN+VALID, score TEST **once**.
- **`reg:absoluteerror`** optimises MAE directly. It predicts the median, so with right-skewed delivery times the bias is slightly negative (−0.6 min). For a customer-facing *promise* you'd use a higher quantile; for *accuracy tracking* the median is the right target.

### 4. Train locally, register and serve in Snowflake
- **Training is local and free.** The feature table (~210k delivered rows, a few MB) is pulled with Snowpark and trained on the laptop.
- **The model is logged to the Snowflake Model Registry** (`ML.ETA_XGB`), with metrics, features and parameters on each version.
- **Inference runs in the warehouse** from SQL: `MODEL(ML.ETA_XGB, V1)!PREDICT(...)`. Airflow needs no ML libraries.
- **The catch: version pins.** Packages must exist in Snowflake's Anaconda channel for warehouse inference, so versions were picked from both PyPI (at least two weeks old) and `INFORMATION_SCHEMA.PACKAGES`. xgboost **3.2.0** was used instead of PyPI's 3.4.1, which isn't in the channel. The Anaconda terms were already acknowledged.

*Rejected:* pushdown training in a stored procedure (more packaging and credits for the same result at this size). Snowflake Notebooks or ML Jobs would be the next step at larger scale.

### 5. Champion/challenger in the registry
Every `make ml-train` logs a new version (V1, V2, …) and writes a row to `ML.MODEL_VERSIONS`. It becomes the **default version only if its TEST MAE is strictly better** than the current default's. Scoring always uses the default, so a worse model can never reach production by accident. Verified: V2, without load and distance features (MAE 5.57), was logged and **not** promoted.

### 6. Scoring and accuracy tracking
- **`score_eta`** (an Airflow task after data quality) scores LIVE orders not yet scored by the default version. Its `NOT EXISTS` makes reruns idempotent, and every prediction records its model version.
- **`ML.V_ETA_ACCURACY_DAILY`** reports MAE, bias and p90 error per day and version, next to the promised-ETA and rule MAE.
- **`ML.ALERT_ETA_DRIFT`** (daily at 06:00 IST, suspended by default) calls `OPS.RAISE_ALERT` when the latest day with at least 50 deliveries has MAE > 1.3 × the version's test MAE.

## Results
| | MAE (min) |
|---|---|
| **ETA model V1, TEST (Oct 1–9, 20,201 delivered)** | **4.29** (p90 abs err 9.0, bias −0.6) |
| Platform promised ETA | 6.45 |
| Rule estimate (promise − 12) | 7.62 |
| Zone × hour mean | 7.53 |
| V1 in-warehouse backtest (same rows) | 4.2934, identical to offline 4.2934 |
| V1 on LIVE orders (Oct 10, 339 delivered) | 4.49 vs promised 6.12; drift alert evaluated CONDITION_FALSE |

- **The model is 33% more accurate than the current promise.**
- **Feature importance:** distance (0.34) and restaurant prep time (0.30) dominate, then surge fee (0.09, a proxy for load) and time of day.

## Trade-offs
- **No driver-supply feature.** The backfill has no GPS pings, so idle drivers can't be a feature yet. The surge fee partly carries that signal.
- **Fixed split dates.** Comparing a new version to the champion is only fair on the same TEST window. Moving the window means re-scoring the champion first.
- **Retraining is manual** (`make ml-train`). A scheduled retrain belongs in P8 once there's more live history.
- **The rule baseline is worse than the padded promise**, because the rule underestimates (actual > rule on average). The buffer partly corrects that bias; the model removes most of it.
