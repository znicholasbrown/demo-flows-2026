"""
the_dowser — THROWAWAY spike flow. Probes a data source through a
``SqlAlchemyConnector`` block to test one theory: a Prefect block can stand in
as the proxy for a data source, so the same flow code yields a different,
correctly attributed access outcome purely by swapping the block name.

Probe ladder — each rung is its own task so its state shows in the UI:

1. ``load``     ``SqlAlchemyConnector.load(block_name)``. Never touches the
                database; only proves the block document is readable.
2. ``connect``  ``SELECT 1`` through the block. First real contact.
3. ``read``     ``SELECT COUNT(*) FROM deals``. Proves the schema is there.
4. ``write``    ``INSERT`` one probe row into ``deals``, then delete it.

The flow catches every rung's state and keeps climbing, so the matrix is
always complete. It files a markdown artifact keyed by block name (so each
data source keeps one "latest health" artifact in the UI), then ends
Completed if every rung passed and Failed otherwise.

Blocks (all ``sqlalchemy-connector``, built by ``.tmp/dowser/make_blocks.py``
against SQLite fixtures in ``.tmp/dowser/``):

    dowser-healthy        seeded file, everything works
    dowser-unreachable    path inside a directory that does not exist
    dowser-readonly       same seeded file opened with ``mode=ro``
    dowser-wrong-schema   valid but empty database, no ``deals`` table
    dowser-not-a-db       a plain text file

Run ``python the_dowser.py`` to probe all five in sequence, or pass block
names as arguments to probe a subset.
"""

from datetime import datetime, timezone

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE
from prefect.states import Completed, Failed, State
from prefect_sqlalchemy import SqlAlchemyConnector

DEFAULT_BLOCKS = [
    "dowser-healthy",
    "dowser-unreachable",
    "dowser-readonly",
    "dowser-wrong-schema",
    "dowser-not-a-db",
]

PROBE_ROW_ID = "D-9999"
RUNGS = ["load", "connect", "read", "write"]


# ---------------------------------------------------------------------------
# Rungs
# ---------------------------------------------------------------------------

@task(name="load", cache_policy=NO_CACHE)
def rung_load(block_name: str) -> SqlAlchemyConnector:
    """Load the block document. This proves nothing about the database."""
    db = SqlAlchemyConnector.load(block_name)
    get_run_logger().info("Loaded %s -> %s", block_name, db._rendered_url.render_as_string(hide_password=True))
    return db


@task(name="connect", cache_policy=NO_CACHE)
def rung_connect(db: SqlAlchemyConnector) -> str:
    """First real contact with the data source."""
    row = db.fetch_one("SELECT 1")
    return f"SELECT 1 returned {tuple(row)}"


@task(name="read", cache_policy=NO_CACHE)
def rung_read(db: SqlAlchemyConnector) -> str:
    """Read from the table the flow expects to exist."""
    row = db.fetch_one("SELECT COUNT(*) FROM deals")
    return f"deals has {row[0]} rows"


@task(name="write", cache_policy=NO_CACHE)
def rung_write(db: SqlAlchemyConnector) -> str:
    """Insert one probe row, then remove it so the fixture stays clean."""
    db.execute(
        "INSERT INTO deals (id, account, amount_usd, status) "
        "VALUES (:id, 'Dowser probe', 1, 'probe')",
        parameters={"id": PROBE_ROW_ID},
    )
    db.execute("DELETE FROM deals WHERE id = :id", parameters={"id": PROBE_ROW_ID})
    return "inserted and deleted one probe row"


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _describe(state: State | None) -> tuple[str, str]:
    """Turn a rung's final state into (outcome, detail) for the matrix."""
    if state is None:
        return "skipped", "earlier rung failed"
    if state.is_completed():
        value = state.result()
        if isinstance(value, SqlAlchemyConnector):
            # Never put the block repr in an artifact: a URL-string
            # connection_info would show its password in clear text.
            value = value._rendered_url.render_as_string(hide_password=True)
        return "pass", str(value)
    err = state.result(raise_on_failure=False)
    first_line = str(err).splitlines()[0] if str(err) else ""
    return "FAIL", f"`{type(err).__name__}`: {first_line}"


def _render_matrix(block_name: str, outcomes: dict[str, tuple[str, str]]) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"# Dowsing report: `{block_name}`",
        "",
        f"Probed at {stamp}.",
        "",
        "| Rung | Outcome | Detail |",
        "| --- | --- | --- |",
    ]
    for rung in RUNGS:
        outcome, detail = outcomes[rung]
        lines.append(f"| {rung} | {outcome} | {detail} |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="the-dowser",
    description=(
        "Probes a data source through a SqlAlchemyConnector block: load, connect, "
        "read, write. Files a per-block markdown artifact with the outcome matrix "
        "and fails the run if any rung fails."
    ),
)
def the_dowser(block_name: str = "dowser-healthy") -> State:
    logger = get_run_logger()
    logger.info("Dowsing %s", block_name)

    states: dict[str, State | None] = {rung: None for rung in RUNGS}
    db: SqlAlchemyConnector | None = None

    states["load"] = rung_load(block_name, return_state=True)
    if states["load"].is_completed():
        db = states["load"].result()
        states["connect"] = rung_connect(db, return_state=True)
        states["read"] = rung_read(db, return_state=True)
        states["write"] = rung_write(db, return_state=True)

    outcomes = {rung: _describe(state) for rung, state in states.items()}
    for rung, (outcome, detail) in outcomes.items():
        logger.info("%-8s %-7s %s", rung, outcome, detail)

    create_markdown_artifact(
        key=block_name,
        markdown=_render_matrix(block_name, outcomes),
        description=f"Latest dowsing result for block `{block_name}`.",
    )

    if db is not None:
        db.close()

    failed = [rung for rung, (outcome, _) in outcomes.items() if outcome != "pass"]
    if failed:
        return Failed(message=f"{block_name}: failed at {', '.join(failed)}")
    return Completed(message=f"{block_name}: all rungs passed")


if __name__ == "__main__":
    import sys

    names = sys.argv[1:] or DEFAULT_BLOCKS
    summary = []
    for name in names:
        state = the_dowser(block_name=name, return_state=True)
        summary.append(f"{name:22s} {state.type.value:10s} {state.message}")
    print("\n" + "\n".join(summary))
