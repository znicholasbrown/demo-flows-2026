"""Tests for the pure atlas generator behind the_cartographer."""

from collections import Counter

import pytest
from prefect.types.names import validate_valid_asset_key

from the_cartographer.atlas import LAYERS, REALMS, generate_atlas, select_subset

SEED = 4910


@pytest.fixture(scope="module")
def atlas():
    return generate_atlas(seed=SEED, asset_count=1000)


def longest_path(atlas):
    """Length in edges of the longest path, walking upstream edges."""
    depth = {}
    for asset in atlas.assets:
        depth[asset.index] = 1 + max((depth[u] for u in asset.upstream), default=-1)
    return max(depth.values())


def test_generates_exactly_the_requested_number_of_unique_assets(atlas):
    keys = [asset.key for asset in atlas.assets]

    assert len(keys) == 1000
    assert len(set(keys)) == 1000


def test_a_smaller_scale_is_an_exact_prefix_of_a_larger_one(atlas):
    smaller = generate_atlas(seed=SEED, asset_count=200)

    assert smaller.assets == atlas.assets[:200]


def test_every_upstream_edge_points_to_a_lower_index(atlas):
    for asset in atlas.assets:
        assert all(0 <= upstream < asset.index for upstream in asset.upstream), asset.key


def test_every_key_is_a_valid_prefect_asset_key_in_a_known_realm(atlas):
    for asset in atlas.assets:
        validate_valid_asset_key(asset.key)
        assert asset.key.startswith(f"{asset.realm}://")
        assert asset.realm in REALMS


def test_assets_spread_across_every_realm_and_layer(atlas):
    realm_counts = Counter(asset.realm for asset in atlas.assets)
    layer_counts = Counter(asset.layer for asset in atlas.assets)

    assert set(realm_counts) == set(REALMS)
    assert set(layer_counts) == set(range(len(LAYERS)))
    for layer, count in layer_counts.items():
        assert count >= 50, f"layer {layer} has only {count} assets"


def test_edge_count_tracks_the_edge_factor(atlas):
    edges = sum(len(asset.upstream) for asset in atlas.assets)
    assert 3000 <= edges <= 5000, edges

    sparser = generate_atlas(seed=SEED, asset_count=1000, edge_factor=2.0)
    sparser_edges = sum(len(asset.upstream) for asset in sparser.assets)
    assert sparser_edges < edges


def test_a_few_hub_assets_fan_out_to_many_downstreams(atlas):
    downstream = Counter(u for asset in atlas.assets for u in asset.upstream)

    assert downstream.most_common(1)[0][1] >= 50


def test_the_graph_contains_long_chains(atlas):
    assert longest_path(atlas) >= 30


def test_generation_is_deterministic_per_seed(atlas):
    again = generate_atlas(seed=SEED, asset_count=1000)
    other = generate_atlas(seed=SEED + 1, asset_count=1000)

    assert again.assets == atlas.assets
    assert [a.key for a in other.assets] != [a.key for a in atlas.assets]


def test_every_asset_carries_display_properties_and_a_tool(atlas):
    for asset in atlas.assets:
        assert asset.name
        assert asset.description
        assert asset.owners
        assert asset.url.startswith("https://")
        assert asset.tool


def test_subset_selection_is_stable_across_scales(atlas):
    larger = generate_atlas(seed=SEED, asset_count=3000)

    small = select_subset(atlas, fraction=0.05)
    large = select_subset(larger, fraction=0.05)

    assert 20 <= len(small) <= 80, len(small)
    assert small == [asset for asset in large if asset.index < 1000]
    assert select_subset(atlas, fraction=1.0) == list(atlas.assets)
