"""
the_agitator: stirs up the crowd until the automations give out.

A load-generation flow for testing the automation ACTION rate limit in
Prefect Cloud: 1,000 actions per 5 minutes per account, shared by every
automation in every workspace of the account, enforced as a leaky bucket
that drains at 3.33 actions per second. When a firing is denied, Prefect
Cloud disables the automation that fired and emits
``prefect-cloud.automation.action.disabled``.

One run:

- emits ``agitator.crowd.shouted`` ``shouts`` times, spread evenly over
  ``burst_seconds`` (0 for as fast as the events client allows), split
  across ``rallies`` concurrent task runs. Each shout carries its own
  resource (``agitator.shout.<run>.<rally>.<n>``), the rally as a related
  resource, and ``follows`` the previous shout in its rally;
- in the last fifth of each rally, emits ``agitator.bystander.waved``
  ``bystander_waves`` times, spaced out, while shouts are still flowing;
- ends with ``agitator.rally.dispersed`` carrying the totals.

Bucket math. With the defaults (1,500 shouts over 60 seconds) the bucket
admits about 1,000 + 3.33 x 60 = 1,200 firings and denies about 300. The
bystander has to fire while the bucket is full, which is why the waves
land inside the burst: once the shouts stop, the bucket admits again
within a second. In steady overflow more than 90% of firings are denied,
so three waves make the bystander's disable near certain.

Automations to create in the UI before running (the flow logs this too):

1. "Sink": any trigger that never matches (for example a custom event
   named ``agitator.never``) and any action. It exists only to be paused.
2. "Riot control": custom event trigger, reactive, expect
   ``agitator.crowd.shouted``, threshold 1, within 0 seconds; action
   "Pause an automation" targeting Sink. Fires once per shout.
3. "Bystander": custom event trigger, reactive, expect
   ``agitator.bystander.waved``, threshold 1, within 0 seconds; action
   "Pause an automation" targeting Sink. Fires once per wave.

Expected result: Riot control is disabled by Prefect Cloud partway through
the burst, and Bystander is disabled by one of its waves even though it
fired only a handful of times. Both should show the auto-disabled callout
and badge. Sink collects one ``updated`` event per admitted firing.

Events rate limit. 1,500 events in 60 seconds is 1,500 per minute, under
the Starter tier's logs + events bucket (2,800 per minute) and far under
Pro. On a Hobby account (2,000 per minute) keep ``burst_seconds`` at 45 or
more for 1,500 shouts.
"""

import random
import time

from prefect import flow, get_run_logger, task
from prefect.events import emit_event
from prefect.runtime import flow_run

SHOUT_EVENT = "agitator.crowd.shouted"
WAVE_EVENT = "agitator.bystander.waved"
DISPERSED_EVENT = "agitator.rally.dispersed"

ACTION_LIMIT = 1_000
ACTION_LIMIT_WINDOW_SECONDS = 300
DRAIN_PER_SECOND = ACTION_LIMIT / ACTION_LIMIT_WINDOW_SECONDS

# The fraction of each rally after which the bystander starts waving.
WAVES_START_AT = 0.8

SLOGANS = (
    "What do we want? Fewer actions! When do we want them? Per interval!",
    "Hey hey, ho ho, this leaky bucket has to go!",
    "No firing without draining!",
    "One shout, one action!",
    "Whose bucket? Our bucket!",
    "We will not be throttled!",
    "Three point three three a second is not enough!",
    "Down with the five minute window!",
)

SETUP_NOTES = """
Automations this run expects (create them in the UI first):
  1. "Sink"          trigger: custom event "agitator.never" (never matches); any action
  2. "Riot control"  trigger: custom event "agitator.crowd.shouted", reactive, threshold 1, within 0
                     action: Pause an automation -> Sink
  3. "Bystander"     trigger: custom event "agitator.bystander.waved", reactive, threshold 1, within 0
                     action: Pause an automation -> Sink
Expect Riot control and Bystander to be disabled by Prefect Cloud during the burst.
"""


def _rally_resource(run_id: str, index: int) -> dict:
    return {
        "prefect.resource.id": f"agitator.rally.{run_id}.{index}",
        "prefect.resource.role": "rally",
        "prefect.resource.name": f"Rally {index}",
    }


def _expected_denials(shouts: int, burst_seconds: float) -> int:
    """How many firings the account's bucket should deny for this burst."""
    admitted = ACTION_LIMIT + DRAIN_PER_SECOND * burst_seconds
    return max(0, shouts - int(admitted))


def _wave_positions(shouts: int, waves: int) -> set[int]:
    """Shout indexes (1-based) at which a bystander waves: spread across the
    last fifth of the rally so every wave lands while shouts still flow."""
    if waves <= 0 or shouts <= 0:
        return set()
    start = max(1, int(shouts * WAVES_START_AT))
    span = max(1, shouts - start)
    return {min(shouts, start + (k * span) // waves) for k in range(waves)}


@task(name="rally-the-crowd", task_run_name="rally-{index}")
def rally_the_crowd(
    index: int,
    run_id: str,
    shouts: int,
    burst_seconds: float,
    bystander_waves: int,
    task_seed: int | None,
) -> dict:
    """Lead one rally: shout on schedule, wave the bystander near the end."""
    logger = get_run_logger()
    rng = random.Random(task_seed)
    rally = _rally_resource(run_id, index)
    pace = burst_seconds / shouts if shouts > 0 and burst_seconds > 0 else 0.0
    waves_at = _wave_positions(shouts, bystander_waves)
    report_every = max(1, shouts // 10)

    logger.info(
        "Rally %d begins: %d shouts over %.1f s (%.2f s apart), %d bystander wave(s).",
        index,
        shouts,
        burst_seconds,
        pace,
        len(waves_at),
    )

    started = time.monotonic()
    last_shout = None
    waved = 0

    for n in range(1, shouts + 1):
        slogan = rng.choice(SLOGANS)
        last_shout = emit_event(
            event=SHOUT_EVENT,
            resource={
                "prefect.resource.id": f"agitator.shout.{run_id}.{index}.{n}",
                "prefect.resource.name": slogan,
            },
            related=[rally],
            payload={"sequence": n, "rally": index, "slogan": slogan},
            follows=last_shout,
        )

        if n in waves_at:
            waved += 1
            emit_event(
                event=WAVE_EVENT,
                resource={
                    "prefect.resource.id": f"agitator.bystander.{run_id}.{index}.{waved}",
                    "prefect.resource.name": "A bystander",
                },
                related=[rally],
                payload={"wave": waved, "at_shout": n, "rally": index},
            )
            logger.info("A bystander waves (wave %d) at shout %d of %d.", waved, n, shouts)

        if n % report_every == 0 or n == shouts:
            elapsed = time.monotonic() - started
            logger.info("Rally %d: %d/%d shouts after %.1f s.", index, n, shouts, elapsed)

        if pace > 0:
            # Hold to the schedule regardless of how long each emit took.
            wake_at = started + n * pace
            remaining = wake_at - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

    seconds = round(time.monotonic() - started, 1)
    logger.info("Rally %d disperses after %.1f s: %d shouts, %d wave(s).", index, seconds, shouts, waved)
    return {"rally": index, "shouts": shouts, "waves": waved, "seconds": seconds}


@flow(
    name="the-agitator",
    log_prints=True,
    description=(
        "Emits a burst of custom events to push a fire-on-every-event "
        "automation past the account's action rate limit (1,000 actions per "
        "5 minutes), and waves a bystander event during the burst so a "
        "second, quiet automation is disabled as collateral. Pair with the "
        "three automations described in the module docstring."
    ),
)
def the_agitator(
    shouts: int = 1500,
    burst_seconds: float = 60.0,
    rallies: int = 1,
    bystander_waves: int = 3,
    seed: int | None = None,
) -> dict:
    """
    Parameters
    ----------
    shouts:
        Total ``agitator.crowd.shouted`` events across all rallies. Each
        one should fire the "Riot control" automation once. Defaults to
        1,500, about 300 more than the bucket admits in 60 seconds.
    burst_seconds:
        How long each rally takes to shout its share; 0 emits as fast as
        the events client allows. Defaults to 60.
    rallies:
        Concurrent task runs that split the shouts. Defaults to 1.
    bystander_waves:
        ``agitator.bystander.waved`` events per rally, spread across the
        last fifth of the burst while shouts still flow. Defaults to 3.
    seed:
        Seed for slogan selection, for reproducible runs. Defaults to None.
    """
    logger = get_run_logger()
    run_id = flow_run.id or "local"
    shouts = max(1, shouts)
    rallies = max(1, rallies)
    bystander_waves = max(0, bystander_waves)

    logger.info(SETUP_NOTES)
    logger.info(
        "Bucket math: capacity %d, drain %.2f/s. %d shouts over %.0f s should "
        "admit about %d firings and deny about %d.",
        ACTION_LIMIT,
        DRAIN_PER_SECOND,
        shouts,
        burst_seconds,
        min(shouts, shouts - _expected_denials(shouts, burst_seconds)),
        _expected_denials(shouts, burst_seconds),
    )
    if _expected_denials(shouts, burst_seconds) == 0:
        logger.warning(
            "This burst stays under the action limit. Raise shouts or lower "
            "burst_seconds if you want automations to be disabled."
        )

    share, remainder = divmod(shouts, rallies)
    futures = [
        rally_the_crowd.submit(
            index=i + 1,
            run_id=run_id,
            shouts=share + (1 if i < remainder else 0),
            burst_seconds=burst_seconds,
            bystander_waves=bystander_waves,
            task_seed=None if seed is None else seed + i,
        )
        for i in range(rallies)
    ]
    stats = [future.result() for future in futures]

    totals = {
        "run_id": run_id,
        "rallies": rallies,
        "shouts": sum(s["shouts"] for s in stats),
        "waves": sum(s["waves"] for s in stats),
        "burst_seconds": burst_seconds,
        "expected_denials": _expected_denials(shouts, burst_seconds),
    }

    emit_event(
        event=DISPERSED_EVENT,
        resource={
            "prefect.resource.id": f"agitator.rally.{run_id}",
            "prefect.resource.name": f"Rally {run_id}",
        },
        payload=totals,
    )

    print(
        f"The crowd disperses: {totals['shouts']} shouts and {totals['waves']} "
        f"bystander wave(s) across {rallies} rall{'y' if rallies == 1 else 'ies'}. "
        "Now open the Automations page: Riot control and Bystander should be "
        "disabled by Prefect Cloud, and their pages should say why."
    )
    return totals


# ---------------------------------------------------------------------------
# Entrypoint: serves the flow as a deployment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_agitator.serve(name="default")
