"""
the_matryoshka — three nested async flows, one long nap.

The outer flow awaits a middle flow, the middle flow awaits an inner flow, and
the inner flow does the only real work: it sleeps. Set the nap length once, at
the top, and it is passed all the way down. Handy for demoing how Prefect
displays deep subflow trees and long-running runs in the UI.
"""

import asyncio

from prefect import flow, get_run_logger

# Default nap length: 10 minutes.
DEFAULT_SLEEP_SECONDS = 600


# ---------------------------------------------------------------------------
# Innermost flow — does the sleeping
# ---------------------------------------------------------------------------

@flow(
    name="the-matryoshka-core",
    description="The innermost doll. Sleeps for the requested number of seconds.",
)
async def the_matryoshka_core(sleep_seconds: int) -> int:
    """
    Parameters
    ----------
    sleep_seconds:
        How long to sleep, in seconds.
    """
    logger = get_run_logger()

    logger.info("Core reached. Sleeping for %d seconds.", sleep_seconds)
    await asyncio.sleep(sleep_seconds)

    logger.info("Awake after %d seconds.", sleep_seconds)
    return sleep_seconds


# ---------------------------------------------------------------------------
# Middle flow — awaits the core
# ---------------------------------------------------------------------------

@flow(
    name="the-matryoshka-shell",
    description="The middle doll. Opens the core flow and waits for it.",
)
async def the_matryoshka_shell(sleep_seconds: int) -> int:
    """
    Parameters
    ----------
    sleep_seconds:
        How long the core flow must sleep, in seconds.
    """
    logger = get_run_logger()

    logger.info("Shell opened. Handing %d seconds to the core.", sleep_seconds)
    slept = await the_matryoshka_core(sleep_seconds)

    logger.info("Core returned. It slept for %d seconds.", slept)
    return slept


# ---------------------------------------------------------------------------
# Top-level flow
# ---------------------------------------------------------------------------

@flow(
    name="the-matryoshka",
    description=(
        "Nests two subflows inside itself and awaits each one. The innermost "
        "flow sleeps for a configurable time (10 minutes by default)."
    ),
)
async def the_matryoshka(sleep_seconds: int = DEFAULT_SLEEP_SECONDS) -> int:
    """
    Parameters
    ----------
    sleep_seconds:
        How long the innermost flow must sleep, in seconds. Defaults to 600
        (10 minutes).
    """
    logger = get_run_logger()

    logger.info("Opening the outer doll with a %d second nap.", sleep_seconds)
    slept = await the_matryoshka_shell(sleep_seconds)

    logger.info("All dolls closed. Total sleep: %d seconds.", slept)
    return slept


# ---------------------------------------------------------------------------
# Entrypoint — serves the top-level flow as a deployment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_matryoshka.serve(name="default")
