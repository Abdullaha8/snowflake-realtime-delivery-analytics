# ADR 0007: Governance (Standard edition) and cost tuning

- **Status:** Accepted (Phase 7, 2026-10-10)
- **Scope:** `snowflake/10_governance.sql`, `dbt/models/secure/`, `dbt/macros/governance.sql`, `scripts/governance_check.py`, `scripts/qa_clone.py`, `scripts/cost_report.py`

## Context

`DIM_CUSTOMER` holds PII (name, email, phone). The business wants three things:
- zone managers see only their zones
- support staff see clear-text PII
- everyone else sees it masked

It also wants safe testing and recovery, and a clear view of what the platform costs.

**Constraint found on day 1:** this trial account is **Standard edition**. Snowflake rejected `GRANT CREATE MASKING POLICY` with *"Unsupported feature"* (391811). Masking policies, row access policies and tag-based masking all need Enterprise.

## Decision: secure views as the only door into MARTS

| Option | Verdict |
|---|---|
| Masking + row access policies (tag-based) | the native choice, but Enterprise-only and unavailable here |
| Secure views with role-aware logic | **chosen**: works on Standard, the same guarantees, built by dbt |
| Copy tables per audience | rejected: duplicates data and drifts |

- **Schema.** `MARTS_SECURE` holds a secure view for **every** mart; a test enforces "one view per mart". A secure view hides its definition and blocks optimizer-based inference.
- **PII masking:** `dbt/macros/governance.sql` (`mask_email`, `mask_phone`, `mask_name`). Clear text only when `IS_ROLE_IN_SESSION('FOODDEL_PII_READER')` or the engineering role is active. Masked output: `w***@gmail.com`, `*********4279`, `W***`.
- **Row filtering:** `zone_filter()` lets company-wide roles (BI, and PII_READER through inheritance) see all zones. Zone managers see only zones mapped to their role in **`GOVERNANCE.ZONE_ACCESS`**, so giving a manager a new zone is an `INSERT`, not a code change.
- **No back door:** consumer roles (BI, PII_READER, OPS_WEST, OPS_EAST) have **no** privileges on MARTS. P4's future grants for BI were revoked, and `10_governance.sql` revokes idempotently.
- **Governance as code:** dbt builds the views (`+secure: true`) and re-applies their `grants` on every build. A full rebuild of MARTS was verified to keep governance intact.

**Roles added:**

| Role | Sees |
|---|---|
| `FOODDEL_PII_READER` | inherits BI, plus clear-text PII |
| `FOODDEL_OPS_WEST` | its 6 zones only, PII masked |
| `FOODDEL_OPS_EAST` | its 6 zones only, PII masked |

**Testing gotcha:** users can have default secondary roles, which make every granted role "in session". `governance_check.py` runs each role with `USE SECONDARY ROLES NONE`.

## Zero-copy clone and Time Travel
- `make qa-clone` creates `FOODDEL_QA` as a clone of DEV in about **10 s**, sharing storage until something changes.
- Cloned Dynamic Tables and Alerts are suspended immediately (12 objects), because otherwise they'd start refreshing and spending credits.
- **Demo:** delete 19,308 Kothrud orders in the clone, then restore them with `INSERT … SELECT … BEFORE (STATEMENT => '<delete query id>')`. Row count back to 231,423, DEV untouched.
- `make qa-drop` removes the clone.

## Cost
`make cost-report` writes `docs/cost_report.md` from `ACCOUNT_USAGE`:
- credits by service, warehouse and workload (query tag)
- Dynamic Table refresh outcomes
- storage by schema

Totals across all phases: **1.03 of 20 credits**, of which Snowpipe Streaming 0.0003 and serverless alerts 0.0018. The report lists the tunings already in place, two findings (`COMPUTE_WH` usage; untagged Airflow queries, now tagged `airflow`) and next steps.

## Verification
| Check | Result |
|---|---|
| `governance_check.py` (5 roles + west/east partition) | **6/6 PASS** |
| …after a full `dbt build` | still 6/6 (81/81 dbt nodes) |
| clone + Time Travel restore | PASS |

## Trade-offs
- **Views protect only the views.** Engineers (TRANSFORM) can read raw MARTS/SILVER/BRONZE PII. On Enterprise, masking policies would protect the tables themselves at every layer. The migration path is the same tag/policy design described in this ADR's history.
- **Zone mapping uses `CURRENT_ROLE()`**, so a zone manager must make their OPS role primary. Company-wide access uses `IS_ROLE_IN_SESSION` (hierarchy-aware).
- **Skipped:** auto-classification (`SYSTEM$CLASSIFY`) and a dbt build against the clone. Both are useful but low value for the time, and the clone already proves isolation.
