"""Pune delivery zones on the H3 grid (resolution 8)."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import h3
import yaml

H3_RES = 8
ZONE_RING_K = 2
ROAD_FACTOR = 1.3  # road distance ~= 1.3x straight-line distance in a city
POINT_JITTER_M = 150  # < inradius of a res-8 cell (~460 m), so jittered points stay in-cell


@dataclass(frozen=True)
class Zone:
    zone_id: str
    name: str
    lat: float
    lng: float
    profile: str
    weight: float
    center_cell: str
    cells: tuple[str, ...]


class ZoneMap:
    """Named zones plus a deterministic cell -> zone lookup."""

    def __init__(self, zones: list[Zone]) -> None:
        self.zones = zones
        self.by_id = {z.zone_id: z for z in zones}
        self.cell_to_zone = {c: z.zone_id for z in zones for c in z.cells}

    @classmethod
    def load(cls, path: Path | None = None, ring_k: int = ZONE_RING_K) -> ZoneMap:
        if path is None:
            text = resources.files("simulator.data").joinpath("pune_zones.yaml").read_text()
        else:
            text = Path(path).read_text()
        raw = yaml.safe_load(text)["zones"]

        # Every zone claims grid_disk(centre, k). Where disks overlap, the cell goes to the
        # closest centre (grid distance, then great-circle distance as a tie-break).
        centers = {z["zone_id"]: h3.latlng_to_cell(z["lat"], z["lng"], H3_RES) for z in raw}
        claims: dict[str, tuple[int, float, str]] = {}
        for z in raw:
            zid, center = z["zone_id"], centers[z["zone_id"]]
            for cell in h3.grid_disk(center, ring_k):
                rank = (
                    h3.grid_distance(center, cell),
                    h3.great_circle_distance(h3.cell_to_latlng(cell), (z["lat"], z["lng"])),
                    zid,
                )
                if cell not in claims or rank < claims[cell]:
                    claims[cell] = rank

        zones = []
        for z in raw:
            owned = tuple(sorted(c for c, rank in claims.items() if rank[2] == z["zone_id"]))
            zones.append(
                Zone(
                    zone_id=z["zone_id"],
                    name=z["name"],
                    lat=z["lat"],
                    lng=z["lng"],
                    profile=z["profile"],
                    weight=float(z["weight"]),
                    center_cell=centers[z["zone_id"]],
                    cells=owned,
                )
            )
        return cls(zones)

    def zone_of_cell(self, cell: str) -> str | None:
        return self.cell_to_zone.get(cell)

    def zone_of_point(self, lat: float, lng: float) -> str | None:
        return self.cell_to_zone.get(h3.latlng_to_cell(lat, lng, H3_RES))

    def random_point(self, zone_id: str, rng: random.Random) -> tuple[float, float, str]:
        """A random (lat, lng, cell) inside the zone: random cell + small jitter."""
        cell = rng.choice(self.by_id[zone_id].cells)
        clat, clng = h3.cell_to_latlng(cell)
        r = POINT_JITTER_M * math.sqrt(rng.random())
        theta = rng.uniform(0, 2 * math.pi)
        lat = clat + (r * math.cos(theta)) / 111_320
        lng = clng + (r * math.sin(theta)) / (111_320 * math.cos(math.radians(clat)))
        lat, lng = round(lat, 6), round(lng, 6)
        return lat, lng, h3.latlng_to_cell(lat, lng, H3_RES)

    def nearest_zones(self, zone_id: str, n: int) -> list[str]:
        z = self.by_id[zone_id]
        others = sorted(
            (h3.great_circle_distance((z.lat, z.lng), (o.lat, o.lng)), o.zone_id)
            for o in self.zones
            if o.zone_id != zone_id
        )
        return [zid for _, zid in others[:n]]


def straight_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    return h3.great_circle_distance(a, b, unit="km")


def road_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    return straight_km(a, b) * ROAD_FACTOR


def cell_of(lat: float, lng: float) -> str:
    return h3.latlng_to_cell(lat, lng, H3_RES)
