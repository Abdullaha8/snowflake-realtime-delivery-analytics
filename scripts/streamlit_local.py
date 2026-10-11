"""Run the Streamlit in Snowflake app locally, against the real account, as FOODDEL_BI.

Inside Snowflake the app gets its session from `get_active_session()`; here we build a Snowpark
session from .env, switch to the app owner's role, and hand it to the app the same way.

    streamlit run scripts/streamlit_local.py      (or: make streamlit-local)
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "streamlit")]


@st.cache_resource
def _session():  # noqa: ANN202 - snowpark session
    from _env import load_dotenv

    load_dotenv()
    import sf
    from snowflake.snowpark import Session

    session = Session.builder.configs({"connection": sf.connect()}).create()
    for stmt in ("USE ROLE FOODDEL_BI", "USE SECONDARY ROLES NONE", "USE WAREHOUSE WH_BI"):
        session.sql(stmt).collect()
    session.sql("ALTER SESSION SET QUERY_TAG = 'streamlit-local'").collect()
    import os

    session.use_database(os.environ.get("SNOWFLAKE_DATABASE", "FOODDEL_DEV"))
    return session


import snowflake.snowpark.context as _ctx  # noqa: E402

_ctx.get_active_session = _session
runpy.run_path(str(ROOT / "streamlit" / "fooddel_app.py"), run_name="__main__")
