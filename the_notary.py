"""
the_notary — a Prefect flow that pulls records, asks a human to certify one,
and then transforms the approved record.

Tests suspend_flow_run / wait_for_input with a boolean-checkbox approval schema.
Uses the cacheable-suspend pattern: tasks before the suspend are cached on INPUTS
so they are replayed rather than re-executed when the flow is rescheduled on resume.
"""

import asyncio
import random
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task
from prefect.cache_policies import INPUTS
from prefect.flow_runs import suspend_flow_run
from prefect.input import RunInput
from prefect.runtime import flow_run
from pydantic import Field


# ---------------------------------------------------------------------------
# Human-input schema
# ---------------------------------------------------------------------------

class RecordApproval(RunInput):
    """Shown in the Prefect UI as a form the reviewer must fill out."""

    confirmed: bool = Field(
        default=False,
        description=(
            "✅ Check this box to certify the record above is accurate "
            "and approved for downstream processing."
        ),
    )
    reviewer_notes: str = Field(
        default="",
        description="Optional: add any notes or caveats about this record.",
    )


# ---------------------------------------------------------------------------
# Simulated source data
# ---------------------------------------------------------------------------

_FAKE_RECORDS = [
    {
        "id": "REC-001",
        "name": "Acme Corp",
        "amount": 142_500.00,
        "currency": "USD",
        "status": "pending_review",
        "created_at": "2026-06-01T09:12:00Z",
        "tags": ["enterprise", "q2"],
    },
    {
        "id": "REC-002",
        "name": "Globex Industries",
        "amount": 87_300.50,
        "currency": "USD",
        "status": "pending_review",
        "created_at": "2026-06-03T14:45:00Z",
        "tags": ["smb", "q2", "upsell"],
    },
    {
        "id": "REC-003",
        "name": "Initech Solutions",
        "amount": 230_000.00,
        "currency": "USD",
        "status": "pending_review",
        "created_at": "2026-06-10T08:00:00Z",
        "tags": ["enterprise", "new-logo"],
    },
    {
        "id": "REC-004",
        "name": "Umbrella Ltd",
        "amount": 55_750.00,
        "currency": "USD",
        "status": "pending_review",
        "created_at": "2026-06-12T11:30:00Z",
        "tags": ["smb"],
    },
    {
        "id": "REC-005",
        "name": "Soylent Green Corp",
        "amount": 310_000.00,
        "currency": "USD",
        "status": "pending_review",
        "created_at": "2026-06-14T16:20:00Z",
        "tags": ["enterprise", "q2", "strategic"],
    },
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch-records", cache_policy=INPUTS, description="Simulate a SELECT from the deals table")
def fetch_records(limit: int = 5) -> list[dict]:
    logger = get_run_logger()
    logger.info("Querying database for pending_review records (limit=%d)…", limit)

    # Simulate network/query latency in spirit — we just return the fixture data
    records = _FAKE_RECORDS[:limit]
    logger.info("Fetched %d records.", len(records))
    return records


@task(name="select-record", cache_policy=INPUTS, description="Pick the record that needs human sign-off")
def select_record(records: list[dict], record_id: str | None = None) -> dict:
    logger = get_run_logger()

    if record_id:
        matches = [r for r in records if r["id"] == record_id]
        if not matches:
            raise ValueError(f"Record {record_id!r} not found in fetched set.")
        record = matches[0]
    else:
        record = random.choice(records)

    logger.info("Selected record %s (%s) for review.", record["id"], record["name"])
    return record


@task(name="transform-record", description="Enrich and finalise an approved record")
def transform_record(record: dict, approval: RecordApproval) -> dict:
    logger = get_run_logger()
    logger.info("Transforming record %s…", record["id"])

    transformed = {
        **record,
        # Promote status now that a human has signed off
        "status": "approved",
        "approved_at": datetime.now(timezone.utc).isoformat(),
        # Normalise amount to integer cents to avoid float drift downstream
        "amount_cents": int(record["amount"] * 100),
        # Attach reviewer metadata
        "reviewer_notes": approval.reviewer_notes or None,
        # Classify deal size
        "deal_tier": (
            "enterprise" if record["amount"] >= 100_000
            else "mid-market" if record["amount"] >= 50_000
            else "smb"
        ),
    }

    logger.info(
        "Record %s transformed → tier=%s, amount_cents=%d",
        transformed["id"],
        transformed["deal_tier"],
        transformed["amount_cents"],
    )
    return transformed


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="the-notary",
    description=(
        "Pulls pending records from the DB, surfaces one for human certification, "
        "then transforms the approved record and returns it."
    ),
)
async def the_notary(
    record_id: str | None = None,
    fetch_limit: int = 5,
) -> dict:
    """
    Parameters
    ----------
    record_id:
        Pin a specific record by ID; omit to pick one at random.
    fetch_limit:
        How many records to pull from the simulated database.
    """
    logger = get_run_logger()

    # ── Task 1: pull records ────────────────────────────────────────────────
    records = fetch_records(limit=fetch_limit)

    # ── Task 2: select the one that needs a human eye ───────────────────────
    record = select_record(records, record_id=record_id)

    # ── Suspend: ask a human to certify the record ─────────────────────────
    # suspend_flow_run releases infrastructure immediately; the flow is
    # rescheduled from the top when input is submitted.  The key prevents the
    # suspend from firing a second time on that re-run.
    logger.info("Suspending for human approval of record %s…", record["id"])

    record_summary = "\n".join(f"  {k}: {v}" for k, v in record.items())

    approval: RecordApproval = await suspend_flow_run(
        wait_for_input=RecordApproval.with_initial_data(
            confirmed=False,
            description=(
                f"## 📋 Record Certification Required\n\n"
                f"A record has been selected for processing. "
                f"Please review the details below and check the box to approve.\n\n"
                f"```\n{record_summary}\n```\n\n"
                f"> **Note:** Leaving the checkbox unchecked will abort the flow."
            ),
        ),
        key=f"notary-approval-{flow_run.id}",
    )

    if not approval.confirmed:
        raise RuntimeError(
            f"Record {record['id']} was NOT approved by the reviewer. "
            "Flow aborted."
        )

    logger.info(
        "Record %s approved.%s",
        record["id"],
        f" Notes: {approval.reviewer_notes}" if approval.reviewer_notes else "",
    )

    # ── Task 3: transform the approved record ───────────────────────────────
    result = transform_record(record, approval)

    logger.info("Flow complete. Returning transformed record %s.", result["id"])
    return result


# ---------------------------------------------------------------------------
# Entrypoint (local dev without a deployment)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(the_notary())
