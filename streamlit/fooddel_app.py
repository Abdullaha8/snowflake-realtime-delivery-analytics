"""fooddel - Pune food delivery analytics (Streamlit in Snowflake).

Owned by FOODDEL_BI and run on WH_BI: the app can only read governed data (GOLD, MARTS_SECURE
with PII masked, ML, OPS). Queries are cached for 10 minutes, the same as the GOLD lag, so
opening tabs does not keep the warehouse busy.
"""

from queries import QUERIES
from snowflake.snowpark.context import get_active_session

import streamlit as st

st.set_page_config(page_title="fooddel - Pune", layout="wide")
session = get_active_session()
DB = session.get_current_database().strip('"')


@st.cache_data(ttl=600, show_spinner=False)
def q(name: str):
    return session.sql(QUERIES[name].format(db=DB)).to_pandas()


st.title("fooddel - Pune food delivery analytics")
live, business, eta, health = st.tabs(["Live ops", "Business KPIs", "ETA model", "Pipeline health"])

with live:
    surge = q("surge_latest")
    at_risk = q("at_risk")
    c1, c2, c3 = st.columns(3)
    c1.metric("Zones under surge (>1.0x)", int((surge["SURGE_MULTIPLIER"] > 1).sum()))
    c2.metric("Highest surge", f"{surge['SURGE_MULTIPLIER'].max():.2f}x" if len(surge) else "-")
    c3.metric("Open orders past promise", len(at_risk))
    st.subheader("Surge signal - latest 5-minute bucket per zone")
    st.bar_chart(surge, x="ZONE_ID", y="SURGE_MULTIPLIER")
    st.dataframe(surge, hide_index=True, use_container_width=True)
    st.subheader("SLA breaches by zone - last 7 days of data")
    st.bar_chart(q("sla_by_zone"), x="ZONE_ID", y="SLA_BREACHES")
    st.subheader("Orders at risk (stream time)")
    st.dataframe(at_risk, hide_index=True, use_container_width=True)

with business:
    kpi = q("kpi_daily")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Orders", f"{int(kpi['ORDERS'].sum()):,}")
    c2.metric("GMV (INR)", f"{int(kpi['GMV_INR'].sum()):,}")
    # AOV = GMV / delivered orders (GMV only counts delivered orders)
    c3.metric("AOV (INR)", f"{kpi['GMV_INR'].sum() / max(1, kpi['DELIVERED'].sum()):,.0f}")
    c4.metric(
        "SLA breach % (latest day)", f"{kpi['SLA_BREACH_PCT'].iloc[-1]:.1f}%" if len(kpi) else "-"
    )
    st.subheader("Daily GMV and orders")
    st.line_chart(kpi, x="DATE_DAY", y=["GMV_INR"])
    st.line_chart(kpi, x="DATE_DAY", y=["ORDERS"])
    st.subheader("Cohort retention (% of cohort active N months after first order)")
    cohorts = q("cohorts")
    st.dataframe(
        cohorts.pivot(
            index="COHORT", columns="MONTHS_SINCE_FIRST_ORDER", values="RETENTION_PCT"
        ).style.format("{:.1f}", na_rep=""),
        use_container_width=True,
    )
    st.subheader("Top restaurants - last 30 days (prep delay, reject rate)")
    st.dataframe(q("top_restaurants"), hide_index=True, use_container_width=True)

with eta:
    acc = q("eta_accuracy")
    versions = q("model_versions")
    if len(acc):
        latest = acc.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("Model MAE (latest day)", f"{latest['MODEL_MAE']:.2f} min")
        c2.metric("Promised-ETA MAE", f"{latest['PROMISED_MAE']:.2f} min")
        c3.metric("Improvement", f"{100 * (1 - latest['MODEL_MAE'] / latest['PROMISED_MAE']):.0f}%")
        st.subheader("Daily MAE: model vs the platform's promised ETA")
        st.line_chart(acc, x="DATE_DAY", y=["MODEL_MAE", "PROMISED_MAE"])
    st.subheader("Model versions (champion/challenger)")
    st.dataframe(versions, hide_index=True, use_container_width=True)

with health:
    st.subheader("Last pipeline runs (Airflow fooddel_pipeline)")
    st.dataframe(q("pipeline_runs"), hide_index=True, use_container_width=True)
    st.subheader("Data-quality checks - latest run")
    st.dataframe(q("dq_latest"), hide_index=True, use_container_width=True)
    st.subheader("Latency: producer -> Snowflake (s) and GOLD freshness (s)")
    st.dataframe(q("latency"), hide_index=True, use_container_width=True)
    st.caption(
        "Accelerated replays are excluded. A large value is a catch-up after the connector was "
        "stopped (events waited in Kafka), not steady-state latency."
    )
    st.subheader("Alerts fired")
    st.dataframe(q("alerts"), hide_index=True, use_container_width=True)

st.caption("Data refreshes every 10 minutes (GOLD target lag). PII is masked for this role.")
