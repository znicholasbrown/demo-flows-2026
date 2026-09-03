"""Tests for the pure parts of the_cartographer flow module."""

from prefect.tasks import MaterializingTask

from the_cartographer.atlas import generate_atlas
from the_cartographer.the_cartographer import batched, build_materialize_task, should_fail

SEED = 4910


def test_build_materialize_task_declares_asset_dependencies_and_tool():
    atlas = generate_atlas(seed=SEED, asset_count=100)
    asset = next(a for a in atlas.assets if len(a.upstream) >= 2)

    task = build_materialize_task(atlas, asset)

    assert isinstance(task, MaterializingTask)
    assert [a.key for a in task.assets] == [asset.key]
    assert task.assets[0].properties.name == asset.name
    assert task.assets[0].properties.owners == list(asset.owners)
    assert [a.key for a in task.asset_deps] == atlas.upstream_keys(asset)
    assert task.materialized_by == asset.tool
    assert task.name.startswith("materialize-")


def test_should_fail_is_deterministic_and_tracks_the_rate():
    flagged = [i for i in range(1000) if should_fail(SEED, i, 0.05)]

    assert flagged == [i for i in range(1000) if should_fail(SEED, i, 0.05)]
    assert 20 <= len(flagged) <= 80, len(flagged)
    assert not any(should_fail(SEED, i, 0.0) for i in range(1000))


def test_batched_keeps_order_and_bounds_size():
    items = list(range(450))

    chunks = list(batched(items, 200))

    assert [len(chunk) for chunk in chunks] == [200, 200, 50]
    assert [item for chunk in chunks for item in chunk] == items
