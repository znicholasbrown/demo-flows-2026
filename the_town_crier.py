"""
the_town_crier — all the news that is fit to shout, at volume.

A load-generation flow for testing log and event ingestion. One run cries
a configurable number of proclamations across several districts (one task
run per district, submitted concurrently) and produces:

- LOTS of logs. With the defaults (3 districts × 60 proclamations, debug
  included) a run emits roughly 1,300 log records — about seven per
  proclamation, plus flow-level ceremony. Volume scales linearly with
  ``districts`` and ``proclamations_per_district``.
- Logs at EVERY level. Each proclamation draws from a weighted news pool:
  DEBUG mutterings, INFO decrees, WARNING portents, ERROR calamities, and
  CRITICAL disasters. Once per district the crier drops the scroll, which
  produces a real ``logger.exception`` traceback. The flow forces the run
  loggers down to DEBUG (see ``include_debug``) so debug records reach
  the API even when ``PREFECT_LOGGING_LEVEL`` keeps its default of INFO.
- ASCII formatting throughout: a masthead banner, box-drawn proclamation
  scrolls alternating between Unicode (``╔═╗``) and pure-ASCII (``+=+``)
  styles, progress bars in both styles, a district tree, a bell, one
  deliberately LONG single-line legal preamble every 25th proclamation
  (the line-wrapping case), and a final box-drawn summary table.
- Custom events, following the Prefect event grammar (resource + action):
  ``town-crier.day.opened``; ``town-crier.bell.rung`` per district;
  ``town-crier.proclamation.issued`` for every proclamation, chained with
  ``follows`` within each district and carrying the district and crier as
  related resources; a crowd reaction whose event name varies with the
  news (``town-crier.crowd.cheered`` / ``.grumbled`` / ``.gasped`` /
  ``.panicked`` / ``.shrugged``); and ``town-crier.day.ended`` with the
  day's totals. Roughly two events per proclamation — about 370 with the
  defaults. Set ``proclamations_per_district=170`` to clear 1,000 events
  in a single run.
"""

import logging
import random
import re
import time
from datetime import datetime

from prefect import flow, get_run_logger, task
from prefect.events import emit_event

TOWN = "Bellhaven"

DISTRICTS = (
    "Market Square",
    "Harbor Ward",
    "Cathedral Green",
    "Miller's Cross",
    "The Shambles",
    "Weavers' Row",
    "Castle Approach",
    "Tanners' Reach",
)

CRIERS = (
    "Barnaby Thorne",
    "Old Wat",
    "Mistress Alys",
    "Cedric the Loud",
    "Humphrey Bellows",
)

LEVEL_ORDER = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
LEVELS = {name: getattr(logging, name) for name in LEVEL_ORDER}
LEVEL_WEIGHTS = (22, 40, 18, 13, 7)

NEWS = {
    "DEBUG": (
        "The pigeons have relocated to the south belfry.",
        "Mrs. Fairweather's cat is stuck in the same tree as yesterday.",
        "The butcher has rearranged his sausages by length.",
        "A cart wheel squeaks on Bell Lane. The wheelwright has been notified.",
        "The night watchman reports nothing. Again.",
        "Three geese loitered by the well. Dispersed without incident.",
    ),
    "INFO": (
        "By order of the mayor: the market shall open an hour early on Saturday.",
        "The bakers' guild announces a surplus of rye. Prices halved until sundown.",
        "A wedding at the chapel this Sunday. All are invited. Bring your own chair.",
        "The travelling players arrive Thursday with a new tragedy. It is said to be quite funny.",
        "The well on Cooper Street has been cleaned. The water is once again water.",
        "New cobblestones on the High Street. Admire them responsibly.",
    ),
    "WARNING": (
        "Storm clouds gather over the western fields. Secure your haystacks.",
        "A pickpocket works the market crowd. Guard your purses.",
        "The bridge on Miller Lane wobbles more than usual. Cross it briskly.",
        "The ale at the Crooked Goat is stronger than advertised. Pace yourselves.",
        "Wolves heard in the north woods. Shepherds, count your flock twice.",
    ),
    "ERROR": (
        "The granary roof has partially collapsed. The grain is mostly fine. Mostly.",
        "A fire in the tannery! Contained, but the smell will linger for days.",
        "The tax collector's ledger is missing. The tax collector is also missing.",
        "The ferry has run aground on the mud bank. Passengers are advised to wade.",
        "The wine shipment from the coast arrived as vinegar.",
    ),
    "CRITICAL": (
        "A DRAGON has been sighted over the eastern ridge! This is not a drill!",
        "The dam is failing! All millers to the sluice gates! NOW!",
        "PLAGUE SHIP in the harbor! No one boards, no one disembarks!",
        "The castle keep is ABLAZE! Every bucket to the walls!",
    ),
}

REACTIONS = {
    "DEBUG": (
        "The crowd, such as it is, nods.",
        "A dog barks approvingly.",
        "Someone at the back asks the crier to speak up, then leaves.",
    ),
    "INFO": (
        "Scattered applause.",
        "The crowd murmurs with cautious interest.",
        "A cheer from the bakers' stall, for unrelated reasons.",
    ),
    "WARNING": (
        "Uneasy muttering spreads through the square.",
        "Purses are clutched. Haystacks are glanced at.",
        "The shepherds exchange a long look and start counting.",
    ),
    "ERROR": (
        "Gasps! Someone drops a cabbage.",
        "The crowd surges toward the notice board for details.",
        "The tavern empties into the street to hear it twice.",
    ),
    "CRITICAL": (
        "SCREAMING. General chaos. The fishmonger flees, fish and all.",
        "The square empties in under a minute. A single shoe remains.",
        "Church bells answer from three parishes at once.",
    ),
}

# The event name for the crowd's reaction varies with the news.
REACTION_EVENTS = {
    "DEBUG": "town-crier.crowd.shrugged",
    "INFO": "town-crier.crowd.cheered",
    "WARNING": "town-crier.crowd.grumbled",
    "ERROR": "town-crier.crowd.gasped",
    "CRITICAL": "town-crier.crowd.panicked",
}

SMALL_PRINT = (
    "(the small print: subject to change by decree, weather, or whim)",
    "(the crier is not responsible for the weather herein described)",
    "(complaints may be lodged with the clerk, who is out)",
    "(this proclamation supersedes the one it contradicts)",
)

# One deliberately long single-line record — the line-wrapping case.
LEGAL_PREAMBLE = (
    "Whereas the council, being assembled and mostly awake, has resolved, "
    "decreed, ordained, proclaimed, and otherwise caused to be cried the "
    "foregoing in every square, lane, yard, wharf, and alley of the town; "
    "and whereas no person shall claim ignorance thereof, the same having "
    "been shouted at considerable personal cost to the crier's voice; now "
    "therefore let all persons govern themselves accordingly, on pain of a "
    "fine not exceeding two geese, or one goose of unusual size, the "
    "schedule of which is posted upon the door of the guildhall, behind "
    "the notice about the missing ladder."
)

BELL = r"""
      __
     /  \
    /    \
   |      |
   |______|
    \____/
     (__)      DING! DONG! DING! DONG!
"""


# ---------------------------------------------------------------------------
# ASCII helpers
# ---------------------------------------------------------------------------

def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _let_debug_through() -> None:
    """Drop the run loggers to DEBUG so debug records reach the API even
    when PREFECT_LOGGING_LEVEL keeps its default of INFO. The API handler
    itself is level 0, so the logger level is the only gate."""
    for name in ("prefect.flow_runs", "prefect.task_runs"):
        logging.getLogger(name).setLevel(logging.DEBUG)


def _masthead() -> str:
    fill = ("-._.-=" * 8)[:46]
    return "\n".join(
        [
            ".-=~=-." + " " * 46 + ".-=~=-.",
            "(__  _)" + fill + "(__  _)",
            "(_ ___)" + "T H E   T O W N   C R I E R".center(46) + "(_ ___)",
            "(__  _)" + "all the news that is fit to shout".center(46) + "(__  _)",
            "(_ ___)" + fill + "(_ ___)",
            "`-=~=-'" + " " * 46 + "`-=~=-'",
        ]
    )


def _scroll(title: str, lines: list[str], style: str) -> str:
    """A proclamation, posted. Unicode box-drawing or pure ASCII."""
    width = max(len(title), *(len(line) for line in lines)) + 2
    if style == "unicode":
        top, divider, bottom = f"╔{'═' * width}╗", f"╟{'─' * width}╢", f"╚{'═' * width}╝"
        edge = "║"
    else:
        top, divider, bottom = f"+{'=' * width}+", f"+{'-' * width}+", f"+{'=' * width}+"
        edge = "|"

    def row(text: str) -> str:
        return f"{edge} {text.ljust(width - 2)} {edge}"

    return "\n".join([top, row(title), divider, *[row(line) for line in lines], bottom])


def _bar(done: int, total: int, style: str) -> str:
    filled = round(20 * done / total)
    if style == "unicode":
        bar = "█" * filled + "░" * (20 - filled)
    else:
        bar = "#" * filled + "-" * (20 - filled)
    return f"[{bar}] {done}/{total} proclamations cried"


def _summary_table(stats: list[dict]) -> str:
    headers = ["District", "Cried", *LEVEL_ORDER, "Loudest"]
    rows = [
        [
            s["district"],
            str(s["proclamations"]),
            *(str(s["levels"][level]) for level in LEVEL_ORDER),
            f"{s['loudest_db']} dB",
        ]
        for s in stats
    ]
    widths = [
        max(len(header), *(len(row[i]) for row in rows))
        for i, header in enumerate(headers)
    ]

    def rule(left: str, mid: str, right: str) -> str:
        return left + mid.join("─" * (w + 2) for w in widths) + right

    def render(cells: list[str]) -> str:
        padded = [
            cells[0].ljust(widths[0]),
            *(cell.rjust(w) for cell, w in zip(cells[1:], widths[1:])),
        ]
        return "│ " + " │ ".join(padded) + " │"

    return "\n".join(
        [
            rule("┌", "┬", "┐"),
            render(headers),
            rule("├", "┼", "┤"),
            *[render(row) for row in rows],
            rule("└", "┴", "┘"),
        ]
    )


# ---------------------------------------------------------------------------
# The rounds — one task per district
# ---------------------------------------------------------------------------

@task(name="cry-the-district", task_run_name="cry-{district}")
def cry_the_district(
    district: str,
    crier: str,
    proclamations: int,
    pace_seconds: float,
    include_debug: bool,
    task_seed: int | None,
) -> dict:
    """Walk one district and cry every proclamation in the satchel."""
    logger = get_run_logger()
    if include_debug:
        _let_debug_through()

    rng = random.Random(task_seed)
    district_slug, crier_slug = _slug(district), _slug(crier)
    counts = {name: 0 for name in LEVEL_ORDER}

    def shout(level_name: str, message: str) -> None:
        if logger.isEnabledFor(LEVELS[level_name]):
            counts[level_name] += 1
        logger.log(LEVELS[level_name], message)

    dropped_at = rng.randint(1, proclamations)
    loudest, crowd_total = 0, 0
    last_issued = None

    shout(
        "INFO",
        f"{crier} arrives at {district} with {proclamations} "
        "proclamations in the satchel.",
    )

    for n in range(1, proclamations + 1):
        style = "unicode" if n % 2 else "ascii"
        category = rng.choices(LEVEL_ORDER, weights=LEVEL_WEIGHTS, k=1)[0]
        headline = rng.choice(NEWS[category])
        decibels = rng.randint(62, 124)
        crowd = rng.randint(3, 300)
        loudest = max(loudest, decibels)
        crowd_total += crowd

        shout(
            "DEBUG",
            f"{crier} mounts the steps ({district}, proclamation {n} of "
            f"{proclamations}).",
        )

        dash = "—" if style == "unicode" else "--"
        shout(
            "INFO",
            "\n"
            + _scroll(
                f"PROCLAMATION No. {n:03d} {dash} {district}",
                [headline, f"issued by {crier}, {decibels} dB"],
                style,
            ),
        )
        shout("INFO", f"OYEZ! OYEZ! OYEZ! ({decibels} dB, crowd of {crowd})")
        shout(category, headline)

        for _ in range(rng.randint(0, 2)):
            shout("DEBUG", rng.choice(SMALL_PRINT))
        if n % 25 == 0:
            shout("INFO", LEGAL_PREAMBLE)

        reaction = rng.choice(REACTIONS[category])
        shout(category if LEVELS[category] >= logging.WARNING else "INFO", reaction)

        if n == dropped_at:
            try:
                raise RuntimeError(
                    f"The scroll for proclamation {n} slipped into the mud "
                    f"of {district}."
                )
            except RuntimeError:
                logger.exception(
                    "The crier dropped the scroll. Recovering with as much "
                    "dignity as possible."
                )
                counts["ERROR"] += 1

        shout("DEBUG", _bar(n, proclamations, style))

        proclamation_id = f"town-crier.proclamation.{district_slug}.{n:03d}"
        last_issued = emit_event(
            event="town-crier.proclamation.issued",
            resource={
                "prefect.resource.id": proclamation_id,
                "prefect.resource.name": headline[:80],
                "town-crier.category": category.lower(),
            },
            related=[
                {
                    "prefect.resource.id": f"town-crier.district.{district_slug}",
                    "prefect.resource.role": "district",
                    "prefect.resource.name": district,
                },
                {
                    "prefect.resource.id": f"town-crier.crier.{crier_slug}",
                    "prefect.resource.role": "crier",
                    "prefect.resource.name": crier,
                },
            ],
            payload={
                "sequence": n,
                "category": category.lower(),
                "decibels": decibels,
                "crowd_size": crowd,
            },
            follows=last_issued,
        )
        emit_event(
            event=REACTION_EVENTS[category],
            resource={
                "prefect.resource.id": f"town-crier.crowd.{district_slug}",
                "prefect.resource.name": f"The crowd at {district}",
            },
            related=[
                {
                    "prefect.resource.id": proclamation_id,
                    "prefect.resource.role": "proclamation",
                },
                {
                    "prefect.resource.id": f"town-crier.district.{district_slug}",
                    "prefect.resource.role": "district",
                    "prefect.resource.name": district,
                },
            ],
            payload={"reaction": reaction, "crowd_size": crowd},
            follows=last_issued,
        )

        if pace_seconds > 0:
            time.sleep(pace_seconds)

    shout(
        "INFO",
        f"Rounds complete in {district}: "
        + ", ".join(f"{level}×{counts[level]}" for level in LEVEL_ORDER)
        + f". Loudest cry: {loudest} dB.",
    )

    return {
        "district": district,
        "crier": crier,
        "proclamations": proclamations,
        "levels": counts,
        "loudest_db": loudest,
        "crowd_total": crowd_total,
        "records": sum(counts.values()),
    }


# ---------------------------------------------------------------------------
# The flow — one very loud day
# ---------------------------------------------------------------------------

@flow(
    name="the-town-crier",
    log_prints=True,
    description=(
        "Cries a configurable flood of proclamations across several "
        "districts — ASCII banners and box-drawn scrolls, logs at every "
        "level from DEBUG to CRITICAL (traceback included), and roughly "
        "two custom events per proclamation. Built to generate more than "
        "1,000 log records per run for load-testing log and event "
        "ingestion."
    ),
)
def the_town_crier(
    districts: int = 3,
    proclamations_per_district: int = 60,
    pace_seconds: float = 0.05,
    include_debug: bool = True,
    seed: int | None = None,
) -> dict:
    """
    Parameters
    ----------
    districts:
        How many districts get a crier (one concurrent task run each).
        Defaults to 3.
    proclamations_per_district:
        Proclamations cried per district, ~7 log records and ~2 events
        each. Defaults to 60 (~1,300 log records, ~370 events per run).
    pace_seconds:
        Sleep between proclamations, to spread the volume over time.
        0 for a burst. Defaults to 0.05.
    include_debug:
        Force the run loggers to DEBUG so debug records reach the API.
        False leaves the configured level (drops ~40% of the volume).
        Defaults to True.
    seed:
        Seed for the day's randomness, for reproducible runs. Defaults
        to None (every day's news is different).
    """
    logger = get_run_logger()
    if include_debug:
        _let_debug_through()

    rng = random.Random(seed)
    day = datetime.now().strftime("%Y-%m-%d")

    names = []
    for i in range(districts):
        base = DISTRICTS[i % len(DISTRICTS)]
        lap = i // len(DISTRICTS)
        names.append(base if lap == 0 else f"{base} {lap + 1}")
    assigned_criers = [CRIERS[i % len(CRIERS)] for i in range(districts)]

    logger.info("\n%s", _masthead())

    tree = [f"The rounds today in {TOWN}:", f"└─ {TOWN}"]
    for i, (district, crier) in enumerate(zip(names, assigned_criers)):
        branch = "└─" if i == len(names) - 1 else "├─"
        tree.append(
            f"   {branch} {district} — {crier} "
            f"({proclamations_per_district} proclamations)"
        )
    logger.info("\n%s", "\n".join(tree))

    emit_event(
        event="town-crier.day.opened",
        resource={
            "prefect.resource.id": f"town-crier.day.{day}",
            "prefect.resource.name": f"Rounds of {day}",
        },
        payload={
            "town": TOWN,
            "districts": names,
            "proclamations_planned": districts * proclamations_per_district,
        },
    )

    logger.info("\n%s", BELL)
    for district in names:
        peals = rng.randint(3, 12)
        logger.info("The bell rings out over %s. (%d peals)", district, peals)
        emit_event(
            event="town-crier.bell.rung",
            resource={
                "prefect.resource.id": f"town-crier.bell.{_slug(district)}",
                "prefect.resource.name": f"The bell of {district}",
            },
            related=[
                {
                    "prefect.resource.id": f"town-crier.district.{_slug(district)}",
                    "prefect.resource.role": "district",
                    "prefect.resource.name": district,
                }
            ],
            payload={"peals": peals},
        )

    futures = [
        cry_the_district.submit(
            district=district,
            crier=crier,
            proclamations=proclamations_per_district,
            pace_seconds=pace_seconds,
            include_debug=include_debug,
            task_seed=None if seed is None else seed + i,
        )
        for i, (district, crier) in enumerate(zip(names, assigned_criers))
    ]
    stats = [future.result() for future in futures]

    logger.info("\n%s", _summary_table(stats))

    totals = {
        "town": TOWN,
        "day": day,
        "districts": len(names),
        "proclamations": sum(s["proclamations"] for s in stats),
        "task_log_records": sum(s["records"] for s in stats),
        "loudest_db": max(s["loudest_db"] for s in stats),
        "crowd_total": sum(s["crowd_total"] for s in stats),
    }

    emit_event(
        event="town-crier.day.ended",
        resource={
            "prefect.resource.id": f"town-crier.day.{day}",
            "prefect.resource.name": f"Rounds of {day}",
        },
        payload=totals,
    )

    print(
        f"The crier retires to the Crooked Goat, voice gone, "
        f"{totals['proclamations']} proclamations cried across "
        f"{totals['districts']} districts. God save the King, and mind "
        "the cabbages."
    )
    logger.info(
        "The day ends: %d task log records, loudest cry %d dB, total "
        "crowd %d souls.",
        totals["task_log_records"],
        totals["loudest_db"],
        totals["crowd_total"],
    )
    return totals


# ---------------------------------------------------------------------------
# Entrypoint — serves the flow as a deployment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_town_crier.serve(name="default")
