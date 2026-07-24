"""
the_informant — a Prefect flow that opens a case file, pauses to take a tip
from an anonymous informant via a schema deliberately built to exercise every
type of input the Prefect schemas service can render, and then files whatever
report comes back.

`date_observed` is the only required field (a native `date`), since every tip
needs a "when" to be actionable. Every other field is optional and each one
targets a distinct JSON-schema/type: `datetime`, plain `str`, constrained
`int`, constrained `float`, `bool`, a native `Enum`, a `Literal` enum, a
`list[str]` array, a `UUID`, a `Decimal`, and a nested `BaseModel` (object).

`informant_alias` also demonstrates the schemas service's "variable select"
capability: its initial value is a `{"__prefect_kind": "workspace_variable"}`
placeholder pointing at a workspace Variable, so the Resume form opens with
that field already toggled to "pull from a Variable" instead of free text.
Prefect resolves this placeholder server-side (via the same hydration path
used for deployment parameters) when the form is submitted.

Tests pause_flow_run / wait_for_input with a RunInput schema that mixes a
required field with a broad sweep of optional, well-typed ones.
"""

import asyncio
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from prefect import flow, get_run_logger, task
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput
from prefect.runtime import flow_run
from prefect.variables import Variable
from pydantic import BaseModel, Field


# The workspace Variable that backs the `informant_alias` variable-select field.
DEFAULT_ALIAS_VARIABLE_NAME = "the-informant-default-alias"


# ---------------------------------------------------------------------------
# Supporting types
# ---------------------------------------------------------------------------

class TipCategory(str, Enum):
    """A native Python Enum — renders as a single-select dropdown."""

    FRAUD = "fraud"
    THEFT = "theft"
    CORRUPTION = "corruption"
    SAFETY = "safety"
    OTHER = "other"


class ContactInfo(BaseModel):
    """A nested object field, so the schema also exercises `type: object`."""

    phone: Optional[str] = Field(default=None, description="Best phone number to reach you.")
    email: Optional[str] = Field(default=None, description="Best email to reach you.")
    preferred_time: Optional[str] = Field(
        default=None,
        description="Preferred time of day for follow-up, e.g. 'evenings'.",
    )


# ---------------------------------------------------------------------------
# Human-input schema — one type per field, all optional except the required date.
# ---------------------------------------------------------------------------

class InformantTip(RunInput):
    """Shown in the Prefect UI as the intake form for an anonymous tip."""

    # date — required
    date_observed: date = Field(
        description="The date the activity was observed. Required — every tip needs a 'when'.",
    )

    # datetime — optional
    observed_at: Optional[datetime] = Field(
        default=None,
        description="The precise date and time it happened, if you can pin it down.",
    )

    # str — optional; pre-set to a "variable select" (see with_initial_data below)
    informant_alias: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=50,
        description=(
            "A codename to sign your tip with. Defaults to a shared workspace "
            "Variable — pick a different Variable, or switch back to free text."
        ),
    )

    # int — optional, constrained
    priority_level: Optional[int] = Field(
        default=None,
        ge=1,
        le=10,
        description="How urgent is this, on a scale of 1 (whenever) to 10 (act now)?",
    )

    # float — optional, constrained
    confidence_score: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Your confidence in this tip, from 0.0 (a rumor) to 1.0 (certain).",
    )

    # bool — optional
    stay_anonymous: Optional[bool] = Field(
        default=None,
        description="Check this box if you do not want to be contacted for follow-up.",
    )

    # native Enum — optional
    tip_category: Optional[TipCategory] = Field(
        default=None,
        description="What kind of activity does this tip concern?",
    )

    # Literal enum — optional
    risk_level: Optional[Literal["low", "medium", "high", "critical"]] = Field(
        default=None,
        description="How much risk does this situation pose right now?",
    )

    # list/array — optional
    tags: Optional[list[str]] = Field(
        default=None,
        max_length=10,
        description="Freeform keywords or tags to help route this tip.",
    )

    # UUID — optional
    related_case_id: Optional[uuid.UUID] = Field(
        default=None,
        description="If this relates to an existing case, provide its case UUID.",
    )

    # Decimal — optional
    reward_requested: Optional[Decimal] = Field(
        default=None,
        ge=0,
        description="Reward amount requested for this tip, in USD (if any).",
    )

    # nested object — optional
    contact_info: Optional[ContactInfo] = Field(
        default=None,
        description="How to reach you, if you're open to follow-up questions.",
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    name="ensure-alias-variable",
    description="Make sure the workspace Variable behind the alias variable-select exists",
)
def ensure_default_alias_variable() -> str:
    logger = get_run_logger()

    Variable.set(DEFAULT_ALIAS_VARIABLE_NAME, "Deep Throat", overwrite=True)

    logger.info("Ensured workspace Variable %r is set for the alias field.", DEFAULT_ALIAS_VARIABLE_NAME)
    return DEFAULT_ALIAS_VARIABLE_NAME


@task(name="open-case-file", description="Open a fresh case file for an incoming tip")
def open_case_file() -> dict:
    logger = get_run_logger()

    case = {
        "case_id": f"CASE-{str(flow_run.id)[:8].upper()}" if flow_run.id else "CASE-UNKNOWN",
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "status": "awaiting_tip",
    }

    logger.info("Opened case file %s at %s.", case["case_id"], case["opened_at"])
    return case


@task(name="file-report", description="Fold the informant's tip into the case file")
def file_report(case: dict, tip: InformantTip) -> dict:
    logger = get_run_logger()

    fields_provided = sum(
        1 for value in tip.model_dump().values() if value not in (None, "")
    )

    report = {
        **case,
        "status": "tip_received" if fields_provided > 0 else "no_information_given",
        "closed_at": datetime.now(timezone.utc).isoformat(),
        "fields_provided": fields_provided,
        "tip": tip.model_dump(),
    }

    logger.info(
        "Case %s updated → status=%s, fields_provided=%d/12.",
        report["case_id"],
        report["status"],
        fields_provided,
    )
    return report


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="the-informant",
    description=(
        "Opens a case file, pauses to take an anonymous tip via a 12-field intake "
        "form that exercises every input type in the schemas service (only the "
        "observation date is required), then files whatever information comes back."
    ),
)
async def the_informant() -> dict:
    logger = get_run_logger()

    # ── Task 1: open the case file ─────────────────────────────────────────
    case = open_case_file()

    # ── Task 1b: make sure the alias variable-select has something to point at ──
    alias_variable_name = ensure_default_alias_variable()

    # ── Pause: take the tip from the informant ─────────────────────────────
    # pause_flow_run keeps infrastructure alive and blocks in place until the
    # form is submitted (or the flow times out).
    logger.info("Pausing to take a tip for case %s…", case["case_id"])

    tip: InformantTip = await pause_flow_run(
        wait_for_input=InformantTip.with_initial_data(
            description=(
                f"## 🕵️ Anonymous Tip Intake — Case `{case['case_id']}`\n\n"
                f"Share whatever you're comfortable with. All fields are optional "
                f"**except the observation date**, which is required.\n"
            ),
            # Pre-toggle `informant_alias` to "variable select" mode, pointed at
            # a real workspace Variable. Prefect resolves this placeholder into
            # the Variable's value when the form is submitted.
            informant_alias={
                "__prefect_kind": "workspace_variable",
                "variable_name": alias_variable_name,
            },
        ),
    )

    # ── Log exactly what the informant submitted ────────────────────────────
    # model_dump(mode="json") gives JSON-serialisable values (e.g. `date` -> ISO
    # string) so the payload logs cleanly instead of showing Python reprs.
    tip_data = tip.model_dump(mode="json")
    logger.info("Received tip input for case %s: %s", case["case_id"], tip_data)
    for field_name, value in tip_data.items():
        logger.info("  %-16s = %r", field_name, value)

    # ── Task 2: file whatever came back ─────────────────────────────────────
    report = file_report(case, tip)

    logger.info("Flow complete. Case %s closed as %s.", report["case_id"], report["status"])
    return report


# ---------------------------------------------------------------------------
# Entrypoint (local dev without a deployment)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(the_informant())
