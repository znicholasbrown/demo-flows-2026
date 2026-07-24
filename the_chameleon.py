"""
the_chameleon — a task-less Prefect flow that ends in whatever state you tell it.

After a short pause it returns a custom State, letting you drive the flow run's
final state type and display name from parameters. Handy for demoing how Prefect
surfaces custom-named terminal states (Completed/Failed/Cancelled/…) in the UI.
"""

import time

from prefect import flow, get_run_logger
from prefect.client.schemas.objects import StateType
from prefect.states import State

# How long to "think" before settling into the requested state.
NAP_SECONDS = 5


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="the-chameleon",
    description=(
        "Sleeps briefly, then blends into whatever final state you ask for — "
        "returns a custom State whose type and name come from its parameters."
    ),
)
def the_chameleon(
    state_type: StateType = StateType.COMPLETED,
    state_name: str = "Finished",
) -> State:
    """
    Parameters
    ----------
    state_type:
        The terminal state type to assume, drawn from Prefect's StateType
        enum (COMPLETED, FAILED, CANCELLED, …). Renders as a dropdown in the UI.
    state_name:
        The custom display name for the state (defaults to "Finished", a
        custom name for an otherwise-Completed state).
    """
    logger = get_run_logger()

    logger.info("Blending in… settling into %s in %d seconds.", state_type.name, NAP_SECONDS)
    time.sleep(NAP_SECONDS)

    logger.info("Done. Returning custom state %r (type=%s).", state_name, state_type.name)
    return State(type=state_type, name=state_name)


# ---------------------------------------------------------------------------
# Entrypoint (local dev without a deployment)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_chameleon()
