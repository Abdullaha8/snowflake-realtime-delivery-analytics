"""Generate dbt seeds from the simulator's zone map (single source of truth).

python scripts/gen_zone_seeds.py      writes dbt/seeds/zones.csv and dbt/seeds/zone_cells.csv
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "dbt" / "seeds"


def render() -> dict[str, str]:
    from simulator.zones import ZoneMap

    zone_map = ZoneMap.load()
    zones, cells = io.StringIO(), io.StringIO()
    zw = csv.writer(zones, lineterminator="\n")
    zw.writerow(
        [
            "ZONE_ID",
            "ZONE_NAME",
            "ZONE_PROFILE",
            "DEMAND_WEIGHT",
            "CENTER_LAT",
            "CENTER_LNG",
            "H3_CENTER_R8",
            "H3_CELL_COUNT",
        ]
    )
    cw = csv.writer(cells, lineterminator="\n")
    cw.writerow(["H3_R8", "ZONE_ID"])
    for z in zone_map.zones:
        zw.writerow(
            [z.zone_id, z.name, z.profile, z.weight, z.lat, z.lng, z.center_cell, len(z.cells)]
        )
        for cell in z.cells:
            cw.writerow([cell, z.zone_id])
    return {"zones.csv": zones.getvalue(), "zone_cells.csv": cells.getvalue()}


def main() -> int:
    SEEDS.mkdir(parents=True, exist_ok=True)
    for name, text in render().items():
        (SEEDS / name).write_text(text, encoding="utf-8", newline="\n")
        print(f"wrote dbt/seeds/{name} ({text.count(chr(10)) - 1} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
