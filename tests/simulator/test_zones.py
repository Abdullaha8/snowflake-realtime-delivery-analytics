import random

import h3
import pytest

from simulator.zones import H3_RES, ZoneMap


@pytest.fixture(scope="module")
def zone_map() -> ZoneMap:
    return ZoneMap.load()


def test_twelve_pune_zones_with_cells(zone_map):
    assert len(zone_map.zones) == 12
    for z in zone_map.zones:
        assert len(z.cells) >= 7, z.zone_id
        assert z.center_cell in z.cells
        assert all(h3.get_resolution(c) == H3_RES for c in z.cells)


def test_each_cell_belongs_to_exactly_one_zone(zone_map):
    all_cells = [c for z in zone_map.zones for c in z.cells]
    assert len(all_cells) == len(set(all_cells))
    assert len(zone_map.cell_to_zone) == len(all_cells)


def test_zones_are_in_pune(zone_map):
    for z in zone_map.zones:
        lat, lng = h3.cell_to_latlng(z.center_cell)
        assert 18.40 < lat < 18.70 and 73.65 < lng < 74.05


def test_random_points_stay_in_their_zone(zone_map):
    rng = random.Random(0)
    for z in zone_map.zones:
        for _ in range(200):
            lat, lng, cell = zone_map.random_point(z.zone_id, rng)
            assert h3.latlng_to_cell(lat, lng, H3_RES) == cell
            assert zone_map.zone_of_cell(cell) == z.zone_id


def test_nearest_zones(zone_map):
    near = zone_map.nearest_zones("KOREGAON_PARK", 2)
    assert "KOREGAON_PARK" not in near
    assert "KALYANI_NAGAR" in near
