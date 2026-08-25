"""
the_gatekeeper — a Prefect flow that walks a visitor through a configurable
sequence of human-in-the-loop checkpoints ("gates"), one per HITL mode that
Prefect supports.

Each gate exercises a distinct capability:

- ``nod``          plain ``pause_flow_run()`` with no input — just the Resume button.
- ``form``         ``pause_flow_run(wait_for_input=<RunInput subclass>)`` — a small
                   structured form (the-informant covers schema *breadth*; this
                   gate covers the mode).
- ``password``     ``pause_flow_run(wait_for_input=str)`` — a bare type, auto-wrapped
                   into a one-field "value" form; returns the raw string.
- ``credentials``  ``pause_flow_run(wait_for_input=<plain BaseModel>)`` — Prefect
                   auto-upgrades the model to a RunInput subclass.
- ``strict``       a RunInput with a ``model_validator`` and a retry loop: a wrong
                   answer raises ValidationError and the gate re-pauses with the
                   error shown in the form description. The watchword is "friend".
- ``patience``     ``pause_flow_run(timeout=<short>)`` — resume quickly to pass, or
                   let it expire to test the FlowPauseTimeout failure path.
- ``siesta``       ``suspend_flow_run()`` with no input — releases infrastructure;
                   on resume the flow is rescheduled from the top.
- ``linger``       stays Running in short cached task ticks so you can exercise
                   UI Pause/Suspend or out-of-process ``suspend_flow_run(flow_run_id=…)``
                   / ``resume_flow_run(…)`` against a live run.
- ``parley``       loops on ``receive_input(str)`` and answers each message with
                   ``.respond()`` — drive it with the companion ``the-courier`` flow
                   (defined below) or ``prefect.input.send_input``.

Replay mechanics: every gate uses a deterministic pause key
(``gate-<position>-<name>``), so when a suspend reschedules the flow from the
top, already-answered pauses no-op and reload their stored input instead of
pausing again. The linger ticks are cached on INPUTS for the same reason. If
you combine ``parley`` with ``siesta``, put parley after the siesta so the
conversation is not replayed. Note: async flows hit a Prefect bug where a
replayed pause discards its stored input and returns None — this flow
reloads the input itself (see ``_reload_pause_input``).

Run ``python the_gatekeeper.py`` to serve both flows as deployments from one
process — no work pool or worker needed. Serving registers the same
deployment names as prefect.yaml, so whichever registration ran last (serve
vs ``prefect deploy``) decides how runs execute.
"""

import math
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from prefect import flow, get_run_logger, task
from prefect.cache_policies import INPUTS
from prefect.exceptions import FlowPauseTimeout
from prefect.flow_runs import pause_flow_run, suspend_flow_run
from prefect.input import RunInput, receive_input, send_input
from prefect.input.run_input import keyset_from_base_key, run_input_subclass_from_type
from prefect.runtime import flow_run
from pydantic import BaseModel, Field, ValidationError, model_validator
from uuid import UUID


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

class Gate(str, Enum):
    """One checkpoint per human-in-the-loop mode Prefect supports."""

    NOD = "nod"
    FORM = "form"
    PASSWORD = "password"
    CREDENTIALS = "credentials"
    STRICT = "strict"
    PATIENCE = "patience"
    SIESTA = "siesta"
    LINGER = "linger"
    PARLEY = "parley"


DEFAULT_GATES = [
    Gate.NOD,
    Gate.FORM,
    Gate.PASSWORD,
    Gate.CREDENTIALS,
    Gate.STRICT,
    Gate.SIESTA,
]

LINGER_TICK_SECONDS = 5
PARLEY_CLOSING_WORD = "done"


# ---------------------------------------------------------------------------
# Human-input schemas
# ---------------------------------------------------------------------------

class GatePass(RunInput):
    """The `form` gate — a small RunInput subclass rendered as a form."""

    visitor_name: str = Field(
        default="traveler",
        description="Who seeks passage through the gate.",
    )
    purpose: Literal["trade", "diplomacy", "sightseeing", "other"] = Field(
        default="trade",
        description="Why you wish to enter.",
    )
    escort_required: bool = Field(
        default=False,
        description="Check this box to request an armed escort inside the walls.",
    )


class TravelPapers(BaseModel):
    """The `credentials` gate — a plain BaseModel Prefect auto-upgrades to a RunInput."""

    traveler_name: str = Field(
        default="traveler",
        description="The name written on your papers.",
    )
    origin: str = Field(
        default="the borderlands",
        description="Where your journey began.",
    )
    escorts: int = Field(
        default=0,
        ge=0,
        description="How many escorts travel with you.",
    )


class Watchword(RunInput):
    """The `strict` gate — custom validation with a retry loop. Answer: "friend"."""

    watchword: str = Field(
        default="",
        description="Speak the watchword to pass. Wrong answers are rejected and re-asked.",
    )

    @model_validator(mode="after")
    def check_watchword(self) -> "Watchword":
        if self.watchword.strip().lower() != "friend":
            raise ValueError(
                f"{self.watchword!r} is not the watchword. Speak 'friend' and enter."
            )
        return self


# ---------------------------------------------------------------------------
# Replay workaround
# ---------------------------------------------------------------------------

async def _reload_pause_input(input_type: type, key: str):
    """Reload a keyed pause's stored input directly from the API.

    Works around a Prefect bug (present in 3.7.4): when a keyed pause with
    ``wait_for_input`` replays after a suspend rescheduled the flow,
    ``apause_flow_run`` loads the stored input but discards it and returns
    None — the sync engine returns it (compare the ``state.is_running()``
    branches in ``prefect/flow_runs.py``). The keyset is deterministic
    (state name "paused" + the pause key), so we can load the response
    ourselves.
    """
    keyset = keyset_from_base_key(f"paused-{key}")
    return await run_input_subclass_from_type(input_type).aload(keyset)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    name="linger-tick",
    cache_policy=INPUTS,
    description="One short tick of the linger gate; cached so replays skip completed ticks",
)
def linger_tick(tick: int, total_ticks: int, tick_seconds: int, run_marker: str) -> str:
    logger = get_run_logger()
    logger.info(
        "Lingering at the gate (tick %d/%d, %ds each)…", tick + 1, total_ticks, tick_seconds
    )
    time.sleep(tick_seconds)
    return f"{run_marker}-tick-{tick}"


@task(name="close-the-gate", description="File the ledger of every checkpoint the visitor cleared")
def close_the_gate(ledger: list[dict]) -> dict:
    logger = get_run_logger()

    for entry in ledger:
        logger.info(
            "Gate %d (%s): %s", entry["position"], entry["gate"], entry["detail"]
        )

    return {
        "gates_cleared": len(ledger),
        "ledger": ledger,
        "closed_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# The gatekeeper
# ---------------------------------------------------------------------------

@flow(
    name="the-gatekeeper",
    description=(
        "Walks a visitor through a configurable sequence of human-in-the-loop "
        "gates — pauses with and without input, every input flavor, validation "
        "retries, timeouts, suspends, an out-of-process window, and a two-way "
        "send_input/receive_input parley."
    ),
)
async def the_gatekeeper(
    gates: list[Gate] = DEFAULT_GATES,
    pause_timeout_seconds: int = 3600,
    patience_timeout_seconds: int = 30,
    linger_seconds: int = 120,
    parley_timeout_seconds: int = 120,
) -> dict:
    """
    Run the visitor through the requested gates, in order.

    Args:
        gates: Which checkpoints to run, in order.

            - `nod` — pause with no input; just press Resume
            - `form` — pause with a small structured form
            - `password` — pause asking for one string
            - `credentials` — pause with a plain pydantic-model form
            - `strict` — pause that rejects wrong answers and re-asks; the
              watchword is `friend`
            - `patience` — pause with a short timeout; let it expire to
              test the failure path
            - `siesta` — suspend with no input; frees infrastructure,
              reschedules on resume
            - `linger` — stay Running for a while so you can pause or
              suspend this run from outside
            - `parley` — listen for messages and reply; pair with the
              `the-courier` deployment
        pause_timeout_seconds: How long each interactive gate waits for a
            human before failing the run.
        patience_timeout_seconds: Timeout for the patience gate. Keep it
            short — letting it expire is the point.
        linger_seconds: How long the linger gate keeps the run in a Running
            state, in 5-second ticks.
        parley_timeout_seconds: How long the parley gate waits for each
            message before closing. Send 'done' to close it politely.
    """
    logger = get_run_logger()
    ledger: list[dict] = []

    logger.info(
        "The gatekeeper stands ready. %d gate(s) to clear: %s. Flow run id: %s",
        len(gates),
        ", ".join(g.value for g in gates),
        flow_run.id,
    )

    for position, gate in enumerate(gates):
        # Deterministic key: if a suspend reschedules the flow from the top,
        # already-answered pauses with this key no-op and reload their input.
        key = f"gate-{position}-{gate.value}"

        if gate is Gate.NOD:
            logger.info(
                "Gate %d (nod): pausing with no input — press Resume in the UI.", position
            )
            await pause_flow_run(
                timeout=pause_timeout_seconds, poll_interval=5, key=key
            )
            detail = "resumed with the plain Resume button"

        elif gate is Gate.FORM:
            logger.info(
                "Gate %d (form): pausing with a RunInput form.", position
            )
            gate_pass = await pause_flow_run(
                wait_for_input=GatePass.with_initial_data(
                    description=(
                        f"## 🛂 Gate {position}: papers, please\n\n"
                        "State your name and business to pass. All fields have "
                        "defaults — submitting as-is also works."
                    ),
                ),
                timeout=pause_timeout_seconds,
                poll_interval=5,
                key=key,
            )
            if gate_pass is None:  # replayed pause after a suspend — see _reload_pause_input
                gate_pass = await _reload_pause_input(GatePass, key)
            detail = (
                f"pass issued to {gate_pass.visitor_name!r} for {gate_pass.purpose}"
                + (" (with escort)" if gate_pass.escort_required else "")
            )

        elif gate is Gate.PASSWORD:
            logger.info(
                "Gate %d (password): pausing with wait_for_input=str — the UI "
                "renders a single 'value' field; any string passes.",
                position,
            )
            answer = await pause_flow_run(
                wait_for_input=str,
                timeout=pause_timeout_seconds,
                poll_interval=5,
                key=key,
            )
            if answer is None:  # replayed pause after a suspend — see _reload_pause_input
                answer = await _reload_pause_input(str, key)
            detail = f"password {answer!r} accepted"

        elif gate is Gate.CREDENTIALS:
            logger.info(
                "Gate %d (credentials): pausing with a plain BaseModel, which "
                "Prefect auto-upgrades to a RunInput.",
                position,
            )
            papers = await pause_flow_run(
                wait_for_input=TravelPapers,
                timeout=pause_timeout_seconds,
                poll_interval=5,
                key=key,
            )
            if papers is None:  # replayed pause after a suspend — see _reload_pause_input
                papers = await _reload_pause_input(TravelPapers, key)
            detail = (
                f"papers stamped for {papers.traveler_name!r} of {papers.origin!r} "
                f"(+{papers.escorts} escorts)"
            )

        elif gate is Gate.STRICT:
            base_description = (
                f"## 🗝️ Gate {position}: the watchword\n\n"
                "Speak the watchword to pass. A wrong answer re-opens this form "
                "with the rejection shown. *(Psst — it's `friend`.)*"
            )
            attempt = 0
            description = base_description
            while True:
                try:
                    word = await pause_flow_run(
                        wait_for_input=Watchword.with_initial_data(
                            description=description
                        ),
                        timeout=pause_timeout_seconds,
                        poll_interval=5,
                        # Each attempt needs its own key, or the retry pause
                        # would no-op and reload the rejected answer.
                        key=f"{key}-attempt-{attempt}",
                    )
                    if word is None:  # replayed pause after a suspend — see _reload_pause_input
                        word = await _reload_pause_input(
                            Watchword, f"{key}-attempt-{attempt}"
                        )
                    break
                except ValidationError as exc:
                    attempt += 1
                    reason = exc.errors()[0]["msg"]
                    logger.warning(
                        "Gate %d (strict): attempt %d rejected — %s", position, attempt, reason
                    )
                    description = (
                        base_description
                        + f"\n\n⚠️ **Attempt {attempt} rejected:** {reason}"
                    )
            logger.info(
                "Gate %d (strict): watchword %r accepted.", position, word.watchword
            )
            detail = f"watchword accepted after {attempt + 1} attempt(s)"

        elif gate is Gate.PATIENCE:
            logger.info(
                "Gate %d (patience): pausing with a %ds timeout — resume quickly "
                "to pass, or let it expire to test the timeout failure path.",
                position,
                patience_timeout_seconds,
            )
            try:
                await pause_flow_run(
                    timeout=patience_timeout_seconds, poll_interval=2, key=key
                )
            except FlowPauseTimeout:
                logger.error(
                    "Gate %d (patience): the gate has closed — failing the run. "
                    "This is the expected outcome when testing timeout expiry.",
                    position,
                )
                raise
            detail = "resumed before the timeout"

        elif gate is Gate.SIESTA:
            logger.info(
                "Gate %d (siesta): suspending — infrastructure is released, and "
                "resuming reschedules the flow from the top (earlier gates "
                "replay from their stored inputs).",
                position,
            )
            await suspend_flow_run(key=key)
            detail = "woke from suspension; the run was rescheduled from the top"

        elif gate is Gate.LINGER:
            total_ticks = max(1, math.ceil(linger_seconds / LINGER_TICK_SECONDS))
            logger.info(
                "Gate %d (linger): staying Running for ~%ds in %d ticks. Try the "
                "UI Pause/Suspend buttons, or from Python: "
                "suspend_flow_run(flow_run_id=UUID('%s')) then resume_flow_run(…).",
                position,
                total_ticks * LINGER_TICK_SECONDS,
                total_ticks,
                flow_run.id,
            )
            for tick in range(total_ticks):
                # Cached on INPUTS: after an out-of-process suspend + resume,
                # completed ticks replay instantly and the wait continues
                # where it left off. Suspension takes effect between ticks.
                linger_tick(
                    tick=tick,
                    total_ticks=total_ticks,
                    tick_seconds=LINGER_TICK_SECONDS,
                    run_marker=f"{flow_run.id}-{key}",
                )
            detail = f"lingered for {total_ticks} tick(s) without being detained"

        elif gate is Gate.PARLEY:
            logger.info(
                "Gate %d (parley): listening for messages (up to %ds of silence "
                "each). Run the the-courier deployment with "
                "gatekeeper_flow_run_id=%s, or send_input(…) from Python. "
                "Send %r to close the parley.",
                position,
                parley_timeout_seconds,
                flow_run.id,
                PARLEY_CLOSING_WORD,
            )
            receiver = receive_input(
                str,
                timeout=parley_timeout_seconds,
                poll_interval=2,
                with_metadata=True,
            )
            heard = 0
            closed_by_word = False
            async for message in receiver:
                heard += 1
                text = message.value
                sender = message.metadata.sender
                logger.info(
                    "Gate %d (parley): heard %r from %s.",
                    position,
                    text,
                    sender or "an unsigned note",
                )
                if sender and sender.startswith("prefect.flow-run."):
                    await message.arespond(f"The gatekeeper acknowledges {text!r}.")
                else:
                    logger.info(
                        "Gate %d (parley): no reply address (input was not sent "
                        "by a flow run) — not responding.",
                        position,
                    )
                if text.strip().lower() == PARLEY_CLOSING_WORD:
                    closed_by_word = True
                    break
            detail = (
                f"parley closed after {heard} message(s) "
                + ("by the closing word" if closed_by_word else "by silence")
            )

        else:  # pragma: no cover — the enum is exhaustive
            raise ValueError(f"Unknown gate: {gate}")

        logger.info("Gate %d (%s) cleared: %s.", position, gate.value, detail)
        ledger.append({"position": position, "gate": gate.value, "detail": detail})

    result = close_the_gate(ledger)
    logger.info("All %d gate(s) cleared. The visitor may pass.", len(gates))
    return result


# ---------------------------------------------------------------------------
# The courier — companion flow for the parley gate
# ---------------------------------------------------------------------------

@flow(
    name="the-courier",
    description=(
        "Rides to a the-gatekeeper run standing at its parley gate, delivers "
        "messages with send_input, and waits for each reply with receive_input."
    ),
)
async def the_courier(
    gatekeeper_flow_run_id: UUID,
    messages: list[str] = ["hail, gatekeeper", "any news from the wall?", PARLEY_CLOSING_WORD],
    reply_wait_seconds: int = 30,
) -> dict:
    """
    Deliver each message to the gatekeeper, then wait for its reply.

    Args:
        gatekeeper_flow_run_id: Flow run ID of a the-gatekeeper run currently
            at its parley gate. The gatekeeper logs this ID when the parley
            opens.
        messages: Messages to deliver, in order. End with 'done' to close the
            parley politely.
        reply_wait_seconds: How long to wait for a reply to each message.
            0 means fire and forget.
    """
    logger = get_run_logger()
    replies: list[str] = []

    receiver = (
        receive_input(str, timeout=reply_wait_seconds, poll_interval=1)
        if reply_wait_seconds
        else None
    )

    for message in messages:
        await send_input(message, flow_run_id=gatekeeper_flow_run_id)
        logger.info("Delivered %r to the gatekeeper.", message)

        if receiver is not None:
            try:
                reply = await receiver.anext()
            except TimeoutError:
                logger.warning(
                    "No reply within %ds for %r — riding on.",
                    reply_wait_seconds,
                    message,
                )
            else:
                replies.append(reply)
                logger.info("The gatekeeper replied: %r", reply)

    logger.info(
        "Errand complete: %d message(s) delivered, %d reply(ies) received.",
        len(messages),
        len(replies),
    )
    return {"delivered": messages, "replies": replies}


# ---------------------------------------------------------------------------
# Entrypoint — serve both flows as deployments from one process
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from prefect import serve

    # One serve process handles both flows: trigger runs from the UI, and a
    # courier run can talk to a gatekeeper run standing at its parley gate.
    # These are the same deployment names as prefect.yaml — serving
    # re-registers them as process-based deployments, and `prefect deploy`
    # switches them back to the work pool.
    serve(
        the_gatekeeper.to_deployment(name="the-gatekeeper"),
        the_courier.to_deployment(name="the-courier"),
    )
