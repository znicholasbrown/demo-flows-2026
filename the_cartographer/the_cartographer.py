"""
the_cartographer — charts a large, tangled asset atlas into a workspace.

A load-generation flow for the assets catalog and asset graph. One run
materializes up to ``asset_count`` assets from a deterministic atlas (see
``atlas.py``) spread across thirteen realms (``s3://``, ``motherduck://``,
``snowflake://``, ``sql://``, ``dbt://``, ``powerbi://`` ...), with roughly
``edge_factor`` upstream edges per asset, a few hub sources that fan out to
hundreds of downstreams, and chains whose depth grows with scale.

Re-running never invents assets. The atlas is prefix-stable: with the same
``seed``, a run at 1,000 assets materializes exactly the first thousand of a
run at 5,000. Scale up to add assets; scale down to re-materialize the ones
that already exist. ``subset`` re-materializes a deterministic fraction of
the prefix (for ongoing activity), and ``failure_rate`` fails a
deterministic fraction of those on purpose so failed materializations show
up too.

Each asset is materialized by its own dynamically built ``@materialize``
task that declares the asset (with name, description, owners, url), its
upstream assets through ``asset_deps``, and the realm's tool through ``by``.
Tasks receive plain data, never futures, so the only lineage Prefect sees
is the one the atlas declares. One full seed at the default scale is about
1,000 task runs and 4,700 asset events.

Smoke test for CLOUD-4910: point the CLI at a fresh workspace
(``prefect cloud workspace set``), deploy this file's ``prefect.yaml``
(``prefect deploy --prefect-file the_cartographer/prefect.yaml --all`` from
the repo root), run ``the-cartographer-seed``, then open the workspace's
assets page. Run ``the-cartographer-survey`` a few times to populate the
Active and Inactive tabs and the status badges.
"""

from __future__ import annotations

import argparse
import random
import re
import time
from itertools import islice
from typing import Iterable, Iterator, TypeVar

from prefect import flow, get_run_logger
from prefect.assets import Asset, AssetProperties, add_asset_metadata, materialize
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import MaterializingTask

from the_cartographer.atlas import Atlas, AtlasAsset, generate_atlas, select_subset

T = TypeVar("T")

MAX_WORKERS = 16


def should_fail(seed: int, index: int, failure_rate: float) -> bool:
    """Deterministically flag ``failure_rate`` of assets, independent of scale."""
    return random.Random(f"{seed}:fail:{index}").random() < failure_rate


def batched(items: Iterable[T], size: int) -> Iterator[list[T]]:
    iterator = iter(items)
    while chunk := list(islice(iterator, size)):
        yield chunk


def materialize_asset(key: str, index: int, fail: bool, work_seconds: float) -> dict:
    """The body of every materialization task: a little metadata, maybe a failure."""
    if work_seconds:
        time.sleep(work_seconds)
    if fail:
        raise RuntimeError(f"Materialization of {key} failed on purpose (failure_rate)")
    rows = random.Random(f"rows:{key}:{time.time_ns() // 3_600_000_000_000}").randint(100, 5_000_000)
    add_asset_metadata(key, {"rows": rows, "atlas_index": index})
    return {"key": key, "rows": rows}


def build_materialize_task(atlas: Atlas, asset: AtlasAsset) -> MaterializingTask:
    """A `@materialize` task for one atlas asset, with its lineage declared up front."""
    leaf = asset.key.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"[^a-z0-9]+", "-", leaf.lower()).strip("-") or str(asset.index)
    return materialize(
        Asset(
            key=asset.key,
            properties=AssetProperties(
                name=asset.name,
                description=asset.description,
                owners=list(asset.owners),
                url=asset.url,
            ),
        ),
        by=asset.tool,
        asset_deps=atlas.upstream_keys(asset),
        name=f"materialize-{slug}",
        description=f"Materialize {asset.key} ({len(asset.upstream)} upstream assets) with {asset.tool}",
        cache_policy=NO_CACHE,
        persist_result=False,
        retries=0,
    )(materialize_asset)


@flow(
    name="the-cartographer",
    description="Materialize a large, prefix-stable, multi-realm asset atlas for catalog load testing",
    task_runner=ThreadPoolTaskRunner(max_workers=MAX_WORKERS),
)
def the_cartographer(
    asset_count: int = 1000,
    seed: int = 4910,
    edge_factor: float = 4.0,
    subset: float = 1.0,
    failure_rate: float = 0.0,
    batch_size: int = 200,
    work_seconds: float = 0.0,
) -> dict:
    """
    Materialize the first ``asset_count`` assets of the atlas for ``seed``.

    Args:
        asset_count: How many assets the atlas holds. Same seed + larger count = same assets plus more.
        seed: Atlas seed. Change it to get a different, unrelated atlas.
        edge_factor: Target upstream edges per asset (about 4 means about 4,000 edges per 1,000 assets).
        subset: Fraction of the atlas to materialize this run (1.0 seeds everything).
        failure_rate: Fraction of the chosen assets whose materialization fails on purpose.
        batch_size: Tasks submitted per batch; each batch is waited on before the next.
        work_seconds: Sleep inside every task, to stretch a run out if needed.
    """
    logger = get_run_logger()

    atlas = generate_atlas(seed=seed, asset_count=asset_count, edge_factor=edge_factor)
    chosen = select_subset(atlas, subset)
    total_edges = sum(len(asset.upstream) for asset in atlas.assets)
    chosen_edges = sum(len(asset.upstream) for asset in chosen)

    logger.info(
        "Atlas %s: %d assets, %d edges. Materializing %d assets (%d edges, subset=%.2f, failure_rate=%.2f) "
        "in batches of %d with %d workers.",
        seed, asset_count, total_edges, len(chosen), chosen_edges, subset, failure_rate, batch_size, MAX_WORKERS,
    )

    succeeded = 0
    failed = 0
    started = time.monotonic()

    for number, batch in enumerate(batched(chosen, batch_size), start=1):
        futures = [
            build_materialize_task(atlas, asset).submit(
                asset.key,
                asset.index,
                should_fail(seed, asset.index, failure_rate),
                work_seconds,
            )
            for asset in batch
        ]
        wait(futures)

        batch_failed = sum(1 for future in futures if not future.state.is_completed())
        failed += batch_failed
        succeeded += len(futures) - batch_failed
        logger.info(
            "Batch %d: %d materialized, %d failed (%d/%d done, %.0fs elapsed)",
            number, len(futures) - batch_failed, batch_failed, succeeded + failed, len(chosen),
            time.monotonic() - started,
        )

    summary = {
        "seed": seed,
        "asset_count": asset_count,
        "edges": total_edges,
        "materialized": succeeded,
        "failed": failed,
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }
    logger.info("Charted the atlas: %s", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the_cartographer without a deployment.")
    parser.add_argument("--asset-count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=4910)
    parser.add_argument("--edge-factor", type=float, default=4.0)
    parser.add_argument("--subset", type=float, default=1.0)
    parser.add_argument("--failure-rate", type=float, default=0.0)
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()
    the_cartographer(
        asset_count=args.asset_count,
        seed=args.seed,
        edge_factor=args.edge_factor,
        subset=args.subset,
        failure_rate=args.failure_rate,
        batch_size=args.batch_size,
    )
