"""Snowflake helper: key-pair auth from .env, runs the numbered SQL scripts.

python scripts/sf.py keygen                 generate admin + connector RSA key pairs in secrets/
python scripts/sf.py run FILE [FILE ...]    run SQL files (${SNOWFLAKE_*} placeholders filled from .env)
python scripts/sf.py verify                 run snowflake/90_verify_bronze.sql and print results
python scripts/sf.py load-backfill [DIR]    PUT data/backfill JSONL to the stage, then COPY INTO BRONZE
python scripts/sf.py suspend                suspend all project warehouses
"""

from __future__ import annotations

import os
import re
import sys
import warnings
from pathlib import Path

from _env import ROOT, load_dotenv

SECRETS = ROOT / "secrets"
SQL_DIR = ROOT / "snowflake"
WAREHOUSES = ("WH_INGEST", "WH_TRANSFORM", "WH_BI")
BACKFILL_TOPICS = ("fooddel.orders.v1", "fooddel.reference.v1", "fooddel.driver_locations.v1")
PLACEHOLDER = re.compile(r"\$\{(SNOWFLAKE_[A-Z_]+)\}")
# snowflake-connector vendors its own requests and warns about urllib3 versions: harmless noise
warnings.filterwarnings("ignore", module="snowflake.connector.vendored.requests")


# -- keys ----------------------------------------------------------------------------------
def keygen() -> int:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    SECRETS.mkdir(exist_ok=True)
    bodies = {}
    for name in ("admin", "connector"):
        priv_path = SECRETS / f"{name}_rsa_key.p8"
        if priv_path.exists():
            print(f"keeping existing {priv_path.relative_to(ROOT)}")
            key = serialization.load_pem_private_key(priv_path.read_bytes(), password=None)
        else:
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            priv_path.write_bytes(
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            )
            print(f"wrote {priv_path.relative_to(ROOT)}")
        pub = key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        priv = priv_path.read_text()
        bodies[name] = (_pem_body(pub.decode()), _pem_body(priv))

    print("\n1) In Snowsight, as your own user, run once (lets scripts/sf.py log in as you):")
    print(f"   ALTER USER <your_user> SET RSA_PUBLIC_KEY='{bodies['admin'][0]}';")
    print("\n2) Add to .env:")
    print("   SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH=secrets/admin_rsa_key.p8")
    print(f"   SNOWFLAKE_CONNECTOR_PUBLIC_KEY={bodies['connector'][0]}")
    print(f"   SNOWFLAKE_CONNECTOR_PRIVATE_KEY={bodies['connector'][1]}")
    return 0


def _pem_body(pem: str) -> str:
    return "".join(line for line in pem.splitlines() if not line.startswith("-----"))


# -- connection ----------------------------------------------------------------------------
def connect():  # noqa: ANN201 - snowflake connection
    import snowflake.connector

    missing = [
        k
        for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ADMIN_USER", "SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH")
        if not os.environ.get(k)
    ]
    if missing:
        sys.exit(f"missing in .env: {', '.join(missing)} (see .env.example)")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_ADMIN_USER"],
        private_key_file=str(ROOT / os.environ["SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH"]),
        role=os.environ.get("SNOWFLAKE_ADMIN_ROLE", "ACCOUNTADMIN"),
        session_parameters={"QUERY_TAG": "fooddel-scripts"},
    )


def render(sql: str) -> str:
    def sub(m: re.Match) -> str:
        value = os.environ.get(m.group(1))
        if value is None:
            sys.exit(f"SQL needs ${{{m.group(1)}}} but it is not set in .env")
        return value

    return PLACEHOLDER.sub(sub, sql)


def _print_cursor(cur) -> None:  # noqa: ANN001
    if not cur.description:
        return
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    widths = [
        max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(cols)
    ]
    widths = [min(w, 60) for w in widths]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    for r in rows:
        print("  " + " | ".join(str(v)[:60].ljust(w) for v, w in zip(r, widths, strict=True)))


def run_files(paths: list[str], show_results: bool = False) -> int:
    with connect() as conn:
        for p in paths:
            path = Path(p)
            print(f"-- {path.name}")
            for cur in conn.execute_string(render(path.read_text(encoding="utf-8"))):
                first = " ".join(cur.query.split())[:90]
                if show_results and cur.description:
                    print(f"\n{first}")
                    _print_cursor(cur)
                else:
                    print(f"   ok  {first}")
    return 0


# -- operations ----------------------------------------------------------------------------
def suspend() -> int:
    from snowflake.connector.errors import ProgrammingError

    with connect() as conn, conn.cursor() as cur:
        for wh in WAREHOUSES:
            try:
                cur.execute(f"ALTER WAREHOUSE IF EXISTS {wh} SUSPEND")
                print(f"{wh}: suspended")
            except ProgrammingError as e:
                if "cannot be suspended" in str(e) or "Invalid state" in str(e):
                    print(f"{wh}: already suspended")
                else:
                    raise
    return 0


def load_backfill(data_dir: str = "data/backfill") -> int:
    base = (ROOT / data_dir).resolve()
    db = os.environ.get("SNOWFLAKE_DATABASE", "FOODDEL_DEV")
    with connect() as conn, conn.cursor() as cur:
        for topic in BACKFILL_TOPICS:
            for part in sorted((base / topic).glob("dt=*")):
                src = part.as_posix()
                cur.execute(
                    f"PUT 'file://{src}/*.jsonl*' '@{db}.BRONZE.BACKFILL_STAGE/{topic}/{part.name}/' "
                    "AUTO_COMPRESS=FALSE OVERWRITE=FALSE PARALLEL=8"
                )
                print(f"PUT {topic}/{part.name}: {len(cur.fetchall())} file(s)")
    return run_files([str(SQL_DIR / "04_backfill_copy.sql")], show_results=True)


def main(argv: list[str]) -> int:
    load_dotenv()
    if not argv:
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "keygen":
        return keygen()
    if cmd == "run" and rest:
        return run_files(rest)
    if cmd == "verify":
        return run_files([str(SQL_DIR / "90_verify_bronze.sql")], show_results=True)
    if cmd == "load-backfill":
        return load_backfill(*rest)
    if cmd == "suspend":
        return suspend()
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
