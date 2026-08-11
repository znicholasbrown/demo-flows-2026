"""
the_archeologist — a different field season every run, told entirely in artifacts.

The flow runs a full excavation season and produces every artifact type
Prefect supports (link, markdown, progress, table, image), split across the
two levels the API offers:

- Task-run artifacts (created inside tasks):
    * progress  — live survey progress, updated square by square
    * progress  — pottery reconstruction that stalls at a random point and
                  never reaches 100%
    * table     — a finds register per trench (one per trench, no description)
    * markdown  — the stratigraphy analysis, Harris matrix included
    * markdown  — the full field diary (a deliberately LONG document)
    * image     — the field photograph of the prize find
    * link      — the radiocarbon lab's methodology page
    * ????????  — ``the-cursed-tablet``: a corrupted find filed under the
                  same key every season, but as a DIFFERENT artifact type
                  with a different value each time. Pin its type with the
                  ``corrupted_artifact_type`` parameter to test one
                  rendering; leave it None and the curse picks. Its version
                  history in the UI flips between all five types.

- Flow-run artifacts (created in the flow body):
    * progress  — season progress, updated as each phase completes
    * image     — the aerial photograph of the site
    * table     — the master field catalog, merged from all trenches
    * markdown  — the end-of-season report
    * link      — the museum accession record for the prize find

Variability comes from two places:

1. Marvin (https://askmarvin.ai) generates the typed data — the site
   dossier, the finds, and the diary's memorable days — as validated
   Pydantic models via ``marvin.generate``. The ``marvin_model`` parameter
   picks the model as a pydantic-ai string. The default,
   ``ollama:qwen3:30b``, runs against a local Ollama server (the flow sets
   ``OLLAMA_BASE_URL`` to ``http://localhost:11434/v1`` if unset). Any
   other provider works too — for example ``openai:gpt-4o`` with
   ``OPENAI_API_KEY``, or ``anthropic:claude-sonnet-4-5`` with
   ``ANTHROPIC_API_KEY``. Pass ``marvin_model=None`` to use Marvin's own
   default (``MARVIN_AGENT_MODEL``, else ``openai:gpt-4o``).
2. Plain ``random`` varies everything else — trench count, finds per
   trench, where the pottery reconstruction stalls, sample counts, and
   which diary days matter.

If Marvin is not installed, has no key, or errors, each generation task
logs a warning and falls back to the dig house archives (seeded procedural
data), so the flow always completes. Pass ``use_marvin=False`` to skip the
LLM entirely, and ``seed`` to make a season reproducible.

Descriptions are deliberately uneven — some are two words, some are
multi-paragraph markdown, and the trench registers have none at all — to
exercise how the UI renders every description length.
"""

import logging
import os
import random
import time
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from prefect import flow, get_run_logger, task
from prefect.artifacts import (
    create_image_artifact,
    create_link_artifact,
    create_markdown_artifact,
    create_progress_artifact,
    create_table_artifact,
    update_progress_artifact,
)

AERIAL_PHOTO_URL = (
    "https://upload.wikimedia.org/wikipedia/commons/e/eb/Machu_Picchu%2C_Peru.jpg"
)
PRIZE_FIND_PHOTO_URL = (
    "https://upload.wikimedia.org/wikipedia/commons/2/23/Rosetta_Stone.JPG"
)
MUSEUM_RECORD_URL = "https://www.britishmuseum.org/collection/object/Y_EA24"
RADIOCARBON_LAB_URL = "https://en.wikipedia.org/wiki/Radiocarbon_dating"

# Pydantic-ai model string. The default runs on a local Ollama server.
DEFAULT_MARVIN_MODEL = "ollama:qwen3:30b"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434/v1"

# Marvin's streamer warns on every thinking delta from qwen-style models.
logging.getLogger("marvin.engine.streaming").setLevel(logging.ERROR)

CONDITION_RANK = {"Fragmentary": 0, "Good": 1, "Intact": 2, "Excellent": 3}


# ---------------------------------------------------------------------------
# The typed data a season is made of
# ---------------------------------------------------------------------------

class StratumRecord(BaseModel):
    """One layer in the tell, surface first."""

    label: str = Field(description="Roman numeral; I is the surface stratum")
    period: str = Field(description="Archaeological period, e.g. 'Iron Age II'")
    depth_range_m: str = Field(description="Depth below datum, e.g. '0.6 – 1.8'")
    notes: str = Field(description="One short field observation")


class SiteDossier(BaseModel):
    """The invented site this season excavates."""

    site_name: str = Field(description="Evocative fictional site name, 2-3 words")
    region: str = Field(description="Fictional-but-plausible region, lowercase phrase")
    backstory: str = Field(description="2-3 sentences of site history")
    headline_event: str = Field(
        description="One clause describing what the second stratum shows, "
        "e.g. 'a destruction layer of ash runs through every trench'"
    )
    strata: list[StratumRecord] = Field(description="3 to 5 strata, surface first")


class Find(BaseModel):
    """One registered small find."""

    object_name: str = Field(description="Short object name, e.g. 'Bronze fibula'")
    material: str
    period: str = Field(description="Period matching one of the site's strata")
    stratum: str = Field(description="Stratum label the find came from")
    condition: Literal["Fragmentary", "Good", "Intact", "Excellent"]
    field_note: str = Field(description="One-sentence note from the trench supervisor")


class DiaryEntry(BaseModel):
    """One memorable day in the director's diary."""

    weather: str = Field(description="Short weather note, lowercase")
    entry: str = Field(description="2-3 wry sentences in a field director's voice")


# ---------------------------------------------------------------------------
# The dig house archives — fallback data when Marvin cannot run
# ---------------------------------------------------------------------------

FALLBACK_SITES = [
    ("Tel Qanara", "the upper Wadi Ashar"),
    ("Kavros Ridge", "the southern Aegean terraces"),
    ("Qasr el-Dabaa", "the western oasis road"),
    ("Bryn Morfa", "the drowned coastal marshes"),
    ("Hisar Tepe", "the Anatolian lake plain"),
]

FALLBACK_PERIOD_COLUMNS = [
    ["Hellenistic", "Iron Age II", "Late Bronze Age", "Chalcolithic"],
    ["Byzantine", "Roman", "Hellenistic", "Persian"],
    ["Abbasid", "Umayyad", "Byzantine", "Nabataean"],
    ["Iron Age I", "Late Bronze Age", "Middle Bronze Age", "Early Bronze Age"],
]

FALLBACK_EVENTS = [
    "a destruction layer of ash and collapsed mudbrick runs through every trench",
    "a flood horizon of sterile silt seals the earliest floors",
    "an abandonment surface — rooms swept clean, doorways blocked from outside",
    "a burnt granary yielded carbonized grain by the kilo",
]

FALLBACK_STRATUM_NOTES = [
    "Plow-disturbed topsoil",
    "Ash and collapsed mudbrick",
    "Domestic floors, tabun ovens",
    "Midden deposit, dense with bone",
    "Cobbled surface, heavily worn",
    "Pit cuts from later occupation",
    "Wind-blown silt, nearly sterile",
]

FALLBACK_OBJECTS = [
    ("Potsherd, red slip ware", "Ceramic"),
    ("Bronze fibula", "Bronze"),
    ("Spindle whorl", "Stone"),
    ("Obsidian blade core", "Obsidian"),
    ("Clay tablet, inscribed", "Clay"),
    ("Amphora handle, stamped", "Ceramic"),
    ("Basalt grinding stone", "Basalt"),
    ("Faience bead cluster", "Faience"),
    ("Iron sickle blade", "Iron"),
    ("Loom weight", "Clay"),
    ("Carnelian seal, drilled", "Carnelian"),
    ("Bone needle", "Bone"),
    ("Terracotta figurine", "Terracotta"),
    ("Bronze arrowhead", "Bronze"),
    ("Scarab amulet", "Steatite"),
    ("Silver hoard fragment", "Silver"),
]

FALLBACK_FIELD_NOTES = [
    "Lifted whole in a soil block.",
    "Found beneath a fallen wall stone.",
    "Sieve find; exact locus approximate.",
    "In situ on a plaster floor.",
    "From the ash lens at the stratum boundary.",
    "Refits with a fragment from last season.",
]

MISSING_PIECES = [
    "two rim sherds",
    "the base ring",
    "a third of the shoulder",
    "one handle stub",
]

# --- the cursed tablet -----------------------------------------------------

CURSED_KEY = "the-cursed-tablet"
CURSED_TYPES = ("link", "markdown", "table", "progress", "image")
CURSE_TABLET_URL = "https://en.wikipedia.org/wiki/Curse_tablet"
CURSED_GLYPHS = "𒀭𒂗𒆠𒈗𒁹𐤀𐤁𐤂𐤃𐤄𐊀𐊁𐊂ᚠᚢᚦᚨ"
CURSED_OMENS = (
    "the harvest belongs to the one who buried this",
    "whoever reads the third line reads it aloud",
    "the river will take back what the kiln has kept",
    "count the sherds again; one has been added",
    "the name of the city is not the name of the city",
    "this line was blank yesterday",
)
CURSED_EPIGRAPHERS = (
    "Dr. Halloway",
    "Prof. Ibsen",
    "M. Okonkwo",
    "the registrar, reluctantly",
    "a visiting scholar who left early",
)
CURSED_LINK_TEXTS = (
    "Do not read the third line aloud (lab reference)",
    "Comparanda: tablets that bite back (lab reference)",
    "Handling guidance for inscribed objects (lab reference)",
)


def _imagine(target, n, instructions, fallback, logger, use_marvin, model):
    """Generate n instances of a Pydantic model with Marvin, or fall back
    to the dig house archives when Marvin cannot run."""
    if use_marvin:
        try:
            import marvin

            agent = None
            if model:
                if model.startswith("ollama:"):
                    os.environ.setdefault("OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL)
                agent = marvin.Agent(name="the-archeologist", model=model)
            items = marvin.generate(
                target, n=n, instructions=instructions, agent=agent
            )
            logger.info(
                "Marvin imagined %d %s object(s) with %s.",
                len(items), target.__name__, model or "the default model",
            )
            return items
        except Exception as exc:
            logger.warning(
                "Marvin unavailable (%s: %s). Consulting the dig house archives instead.",
                type(exc).__name__,
                exc,
            )
    return fallback(n)


def _fallback_dossier(n: int) -> list[SiteDossier]:
    site_name, region = random.choice(FALLBACK_SITES)
    periods = random.choice(FALLBACK_PERIOD_COLUMNS)
    notes = random.sample(FALLBACK_STRATUM_NOTES, k=len(periods))
    labels = ["I", "II", "III", "IV", "V"]

    strata, top = [], 0.0
    for label, period, note in zip(labels, periods, notes):
        bottom = top + random.uniform(0.5, 1.4)
        strata.append(
            StratumRecord(
                label=label,
                period=period,
                depth_range_m=f"{top:.1f} – {bottom:.1f}",
                notes=note,
            )
        )
        top = bottom

    first_dug = random.randint(1898, 1979)
    return [
        SiteDossier(
            site_name=site_name,
            region=region,
            backstory=(
                f"{site_name} sits above {region}, first trenched in "
                f"{first_dug} and abandoned to the wind when the money ran "
                "out. The mound has waited politely ever since."
            ),
            headline_event=random.choice(FALLBACK_EVENTS),
            strata=strata,
        )
        for _ in range(n)
    ]


def _fallback_finds_factory(dossier: SiteDossier):
    objects = FALLBACK_OBJECTS.copy()
    random.shuffle(objects)

    def _fallback_finds(n: int) -> list[Find]:
        finds = []
        for _ in range(n):
            if not objects:
                objects.extend(FALLBACK_OBJECTS)
            object_name, material = objects.pop()
            stratum = random.choice(dossier.strata)
            finds.append(
                Find(
                    object_name=object_name,
                    material=material,
                    period=stratum.period,
                    stratum=stratum.label,
                    condition=random.choices(
                        ["Fragmentary", "Good", "Intact", "Excellent"],
                        weights=[4, 3, 2, 1],
                    )[0],
                    field_note=random.choice(FALLBACK_FIELD_NOTES),
                )
            )
        return finds

    return _fallback_finds


def _fallback_diary_factory(highlights: list[dict]):
    def _fallback_specials(n: int) -> list[DiaryEntry]:
        pool = [
            DiaryEntry(
                weather="clear, wind from the west",
                entry=(
                    f"The {h['object']} surfaced in {h['trench']} during the "
                    "last pass of the morning. Work in the square stopped "
                    "while it was lifted, and nobody minded."
                ),
            )
            for h in highlights
        ] + [
            DiaryEntry(
                weather="dust haze until noon",
                entry=(
                    "The lorry delivered the equipment to the wrong wadi. Day "
                    "spent carrying theodolite boxes across a landscape that "
                    "has not changed since the Chalcolithic, and feels it."
                ),
            ),
            DiaryEntry(
                weather="sandstorm",
                entry=(
                    "Trenches tarped by nine, camp shuttered by ten, everyone "
                    "re-reading last season's report by eleven. The tell has "
                    "weathered worse."
                ),
            ),
        ]
        random.shuffle(pool)
        return pool[:n]

    return _fallback_specials


# ---------------------------------------------------------------------------
# Phase 0 — the season is imagined (Marvin, or the archives)
# ---------------------------------------------------------------------------

@task
def open_the_season(use_marvin: bool, marvin_model: str | None) -> SiteDossier:
    """Invent this season's site: name, region, strata, and backstory."""
    logger = get_run_logger()

    dossier = _imagine(
        SiteDossier,
        n=1,
        instructions=(
            "Invent a fictional archaeological site for an excavation demo. "
            "Plausible but invented: do not use a real site name. Give it 3-5 "
            "strata, surface first, with consistent periods (younger on top) "
            "and increasing depth ranges. The headline_event describes what "
            "the second stratum shows."
        ),
        fallback=_fallback_dossier,
        logger=logger,
        use_marvin=use_marvin,
        model=marvin_model,
    )[0]

    logger.info(
        "This season digs %s, above %s. %d strata expected.",
        dossier.site_name, dossier.region, len(dossier.strata),
    )
    return dossier


# ---------------------------------------------------------------------------
# Phase 1 — survey (task-run progress artifact, SHORT description)
# ---------------------------------------------------------------------------

@task
def survey_site(grid_squares: int, trench_count: int) -> list[str]:
    """Walk the survey grid and pick the trenches worth opening."""
    logger = get_run_logger()

    progress_id = create_progress_artifact(
        key="survey-progress",
        progress=0.0,
        description="Walking the grid.",
    )

    for square in range(1, grid_squares + 1):
        time.sleep(0.5)
        update_progress_artifact(
            artifact_id=progress_id,
            progress=100.0 * square / grid_squares,
        )
        logger.info("Surveyed grid square %d of %d.", square, grid_squares)

    trenches = [f"T{i}" for i in range(1, trench_count + 1)]
    logger.info(
        "Survey complete. %d anomalies worth a trench: %s",
        trench_count, ", ".join(trenches),
    )
    return trenches


# ---------------------------------------------------------------------------
# Phase 2 — excavation (task-run table artifacts, NO description)
# ---------------------------------------------------------------------------

@task
def excavate_trench(
    trench: str,
    dossier: SiteDossier,
    id_prefix: str,
    start_index: int,
    use_marvin: bool,
    marvin_model: str | None,
) -> list[dict]:
    """Excavate one trench, register its finds, file the register table."""
    logger = get_run_logger()

    time.sleep(0.75)
    strata_summary = "; ".join(
        f"stratum {s.label} = {s.period}" for s in dossier.strata
    )
    finds = _imagine(
        Find,
        n=random.randint(2, 4),
        instructions=(
            f"Small finds excavated from trench {trench} at the fictional "
            f"site {dossier.site_name}. Site strata: {strata_summary}. Each "
            "find's stratum must be one of those labels and its period must "
            "match that stratum. Vary materials and conditions realistically."
        ),
        fallback=_fallback_finds_factory(dossier),
        logger=logger,
        use_marvin=use_marvin,
        model=marvin_model,
    )

    registered = [
        {
            "find_id": f"{id_prefix}-{start_index + i:03d}",
            "trench": trench,
            "object": f.object_name,
            "stratum": f.stratum,
            "material": f.material,
            "period": f.period,
            "condition": f.condition,
            "note": f.field_note,
        }
        for i, f in enumerate(finds)
    ]

    # Deliberately no description: the bare-table rendering case.
    create_table_artifact(
        key=f"finds-register-{trench.lower()}",
        table=registered,
    )

    logger.info("Trench %s closed with %d finds.", trench, len(registered))
    return registered


# ---------------------------------------------------------------------------
# Phase 3 — conservation (task-run progress artifact that NEVER reaches 100%)
# ---------------------------------------------------------------------------

@task
def reconstruct_pottery(finds: list[dict]) -> float:
    """Reassemble the season's ceramics. Something is always missing, so the
    reconstruction stalls short of 100% — at a different point each season."""
    logger = get_run_logger()

    ceramics = [
        f for f in finds
        if f["material"] in {"Ceramic", "Clay", "Terracotta", "Faience"}
    ] or finds[:1]
    victim = random.choice(ceramics)
    missing = random.choice(MISSING_PIECES)
    stall = round(random.uniform(55.0, 94.0), 1)

    progress_id = create_progress_artifact(
        key="pottery-reconstruction",
        progress=0.0,
        description=(
            f"Reassembly of the season's {len(ceramics)} ceramic finds on the "
            "conservation bench.\n\n"
            f"**Known issue:** {missing} of {victim['find_id']} "
            f"({victim['object'].lower()}) never came out of the sieve. "
            f"Unless they turn up in next season's spoil-heap re-sort, *this "
            f"artifact stalls at {stall}% forever* — which is exactly what "
            "conservation feels like."
        ),
    )

    steps = 7
    for i in range(1, steps + 1):
        time.sleep(0.4)
        update_progress_artifact(
            artifact_id=progress_id,
            progress=round(stall * i / steps, 1),
        )

    logger.warning(
        "Reconstruction stalled at %s%%. Missing: %s of %s.",
        stall, missing, victim["find_id"],
    )
    return stall


# ---------------------------------------------------------------------------
# Phase 4 — analysis (task-run markdown, image, and link artifacts)
# ---------------------------------------------------------------------------

@task
def analyze_stratigraphy(dossier: SiteDossier, trenches: list[str]) -> str:
    """Read the sections and file the stratigraphy analysis."""
    logger = get_run_logger()

    time.sleep(1)
    strata_rows = "\n".join(
        f"| {s.label} | {s.period} | {s.depth_range_m} | {s.notes} |"
        for s in dossier.strata
    )
    markdown = f"""# Stratigraphy analysis — {dossier.site_name}

Sections were drawn for trenches {", ".join(trenches)}.
{len(dossier.strata)} strata are present across the mound. In stratum II,
{dossier.headline_event}.

## Strata

| Stratum | Period | Depth (m) | Notes |
|---------|--------|-----------|-------|
{strata_rows}

## Harris matrix (simplified)

```text
        [topsoil 001]
              |
        [event 014]      <- the stratum II horizon
         /        \\
   [floor 022]  [wall 023]
         \\        /
        [fill 031]
              |
        [bedrock 040]
```

> Field journal: "The stratum II horizon runs unbroken through every
> trench. Whatever happened here, it happened everywhere at once."

## Interpretation

- [x] Confirm the horizon is a single event
- [x] Date the event — samples sent for radiocarbon dating
- [ ] Explain it — the arguments continue at dinner
"""

    create_markdown_artifact(
        key="stratigraphy-analysis",
        markdown=markdown,
        description="Section drawings, Harris matrix, and interpretation.",
    )

    logger.info(
        "Stratigraphy analysis filed. %d strata identified.", len(dossier.strata)
    )
    return f"In stratum II, {dossier.headline_event}"


@task
def photograph_prize_find(prize: dict, dossier: SiteDossier) -> str:
    """Photograph the season's prize find. LONG markdown description."""
    logger = get_run_logger()

    time.sleep(0.5)
    negative_start = random.randint(100, 900)
    create_image_artifact(
        key="prize-find-photo",
        image_url=PRIZE_FIND_PHOTO_URL,
        description=(
            f"## Field photograph — find {prize['find_id']}\n\n"
            f"{prize['object']} from Trench {prize['trench']}, stratum "
            f"{prize['stratum']}, as it sat on the registration table the "
            "evening it came out of the ground. Condition graded "
            f"*{prize['condition'].lower()}* in the field. Supervisor's note: "
            f"\"{prize['note']}\"\n\n"
            "**Photography record:**\n\n"
            "- Camera: full-frame body, 50 mm macro, f/8, ISO 100\n"
            "- Lighting: raking light from the north-west to raise the detail\n"
            "- Scale: 10 cm bar, north arrow at lower left\n"
            f"- Filed as negatives `N-{negative_start:04d}` through "
            f"`N-{negative_start + 17:04d}`\n\n"
            f"The registrar notes this is the best-preserved object "
            f"{dossier.site_name} has produced to date. Full study is "
            "scheduled for next season; see the end-of-season report for the "
            "publication plan."
        ),
    )

    logger.info("Prize find %s photographed.", prize["find_id"])
    return PRIZE_FIND_PHOTO_URL


@task
def submit_radiocarbon_samples(sample_count: int) -> str:
    """Send charcoal samples to the lab and file the chain-of-custody link."""
    logger = get_run_logger()

    time.sleep(0.5)
    create_link_artifact(
        key="radiocarbon-lab",
        link=RADIOCARBON_LAB_URL,
        link_text="Radiocarbon dating methodology (lab reference)",
        description=(
            f"{sample_count} charcoal samples from the stratum II horizon, "
            "submitted for AMS dating.\n\n"
            "**Chain of custody:**\n\n"
            "1. Lifted in foil by the trench supervisors, never handled bare\n"
            "2. Logged against locus and basket numbers in the field register\n"
            "3. Couriered to the lab with the sample manifest\n"
            "4. Results expected before the study season\n\n"
            "If the dates cluster, the stratum II event gets an absolute "
            "anchor and the whole assemblage dates with it."
        ),
    )

    logger.info("%d samples submitted for radiocarbon dating.", sample_count)
    return "samples in transit"


# ---------------------------------------------------------------------------
# Phase 5 — the field diary (task-run markdown artifact, deliberately LONG)
# ---------------------------------------------------------------------------

@task
def transcribe_field_diary(
    dossier: SiteDossier,
    highlights: list[dict],
    days: int,
    use_marvin: bool,
    marvin_model: str | None,
) -> int:
    """Type up the director's handwritten field diary — the long one.
    The memorable days are generated; the routine ones rotate."""
    logger = get_run_logger()

    special_count = min(5, days, len(highlights) + 2)
    specials = _imagine(
        DiaryEntry,
        n=special_count,
        instructions=(
            "Entries from an excavation director's field diary at the "
            f"fictional site {dossier.site_name}, above {dossier.region}. "
            "Wry, understated, first person plural. Each entry covers one "
            "memorable day. Work these actual finds into some entries: "
            + "; ".join(
                f"{h['object']} from trench {h['trench']}" for h in highlights
            )
        ),
        fallback=_fallback_diary_factory(highlights),
        logger=logger,
        use_marvin=use_marvin,
        model=marvin_model,
    )
    special_days = dict(
        zip(sorted(random.sample(range(1, days + 1), k=len(specials))), specials)
    )

    weather = (
        "clear, wind from the west",
        "dust haze until noon",
        "high heat, work stopped at one",
        "overcast and merciful",
        "cool morning, brutal afternoon",
    )
    routine = (
        "Sieving continued at the spoil heap. The volunteers have started "
        "naming the wheelbarrows, which the director takes as a sign of "
        "either high morale or heatstroke.",
        "Pottery washing all afternoon. The courtyard looks like a mosaic "
        "of drying sherds and the cook has complained twice about the "
        "basins.",
        "Section drawing in the open trenches. The string lines survived "
        "the night, which is more than can be said for the shade cloth.",
        "The total station refused to level on the first three tries. "
        "Recorded twelve elevation points out of spite once it complied.",
        "The registrar caught up on the find-card backlog and has requested, "
        "in writing, that nobody find anything tomorrow.",
        "Baulk trimming and locus photography. The morning light hit the "
        "collapse just right and even the skeptics took pictures.",
    )

    sections = [
        f"# Field diary — {dossier.site_name}",
        "",
        f"{dossier.backstory}",
        "",
        "Transcribed from the director's notebooks at the end of the "
        "season. Spelling normalized, complaints preserved.",
        "",
    ]
    for day in range(1, days + 1):
        if day in special_days:
            entry = special_days[day]
            day_weather, day_text = entry.weather, entry.entry
        else:
            day_weather = weather[day % len(weather)]
            day_text = routine[day % len(routine)]
        sections += [f"### Day {day} — {day_weather}", "", day_text, ""]
        if day % 10 == 0:
            sections += [
                f"**Decade summary, day {day}:**",
                "",
                "| Measure | Count |",
                "|---------|-------|",
                f"| Buckets sieved | {day * 31 + random.randint(-40, 40)} |",
                f"| Sherds washed | {day * 118 + random.randint(-150, 150)} |",
                f"| Find cards filed | {day * 4 + random.randint(-5, 5)} |",
                f"| Arguments about stratum II | {day // 2} |",
                "",
            ]
    sections += [
        "---",
        "",
        "> Final page, unsigned: \"The tell keeps its own schedule.\"",
        "",
    ]
    diary = "\n".join(sections)

    create_markdown_artifact(
        key="excavation-diary",
        markdown=diary,
        description=(
            f"All {days} days of the director's field diary, transcribed. "
            "Long on purpose — this is the wall-of-markdown case."
        ),
    )

    logger.info("Field diary transcribed: %d days, %d characters.", days, len(diary))
    return len(diary)


# ---------------------------------------------------------------------------
# Phase 5.5 — the cursed tablet (task-run artifact of a DIFFERENT TYPE each run)
# ---------------------------------------------------------------------------

@task
def unearth_the_cursed_tablet(manifestation: str | None) -> str:
    """File the find that will not stay what it is. Same key every season;
    a different artifact type with a different value every time."""
    logger = get_run_logger()

    manifestation = manifestation or random.choice(CURSED_TYPES)
    glyphs = "".join(random.choices(CURSED_GLYPHS, k=14))
    omen = random.choice(CURSED_OMENS)
    description = (
        "Found beneath the threshold on the last day, filed under protest. "
        f"The registrar has recorded this object as a **{manifestation}** "
        "artifact this season; it was something else last season and it "
        "will be something else next season. Check the key's version "
        "history and keep your voice down."
    )

    time.sleep(0.5)
    if manifestation == "markdown":
        create_markdown_artifact(
            key=CURSED_KEY,
            markdown=(
                f"# Reading attempt {random.randint(3, 400)}\n\n"
                f"```text\n{glyphs}\n```\n\n"
                f"Provisional translation: *\"{omen}.\"*\n\n"
                "The photograph taken this morning does not match the "
                "object on the bench, and yesterday's transcription no "
                "longer matches either. Filed as is."
            ),
            description=description,
        )
    elif manifestation == "table":
        create_table_artifact(
            key=CURSED_KEY,
            table=[
                {
                    "attempt": i + 1,
                    "epigrapher": random.choice(CURSED_EPIGRAPHERS),
                    "reading": "".join(random.choices(CURSED_GLYPHS, k=8)),
                    "translation": random.choice(CURSED_OMENS),
                    "confidence": f"{random.randint(1, 60)}%",
                }
                for i in range(random.randint(3, 6))
            ],
            description=description,
        )
    elif manifestation == "progress":
        progress_id = create_progress_artifact(
            key=CURSED_KEY,
            progress=round(random.uniform(5.0, 90.0), 1),
            description=description + " Decipherment moves in both directions.",
        )
        for _ in range(3):
            time.sleep(0.3)
            update_progress_artifact(
                artifact_id=progress_id,
                progress=round(random.uniform(5.0, 95.0), 1),
            )
    elif manifestation == "link":
        create_link_artifact(
            key=CURSED_KEY,
            link=CURSE_TABLET_URL,
            link_text=random.choice(CURSED_LINK_TEXTS),
            description=description,
        )
    elif manifestation == "image":
        seed_token = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=8))
        create_image_artifact(
            key=CURSED_KEY,
            image_url=f"https://picsum.photos/seed/{seed_token}/800/600",
            description=description + " No two photographs of it agree.",
        )
    else:
        raise ValueError(
            f"Unknown manifestation {manifestation!r}; "
            f"expected one of {CURSED_TYPES}."
        )

    logger.warning(
        "The cursed tablet manifested as a %s artifact this season.",
        manifestation,
    )
    return manifestation


# ---------------------------------------------------------------------------
# The flow — one field season, flow-run artifacts filed along the way
# ---------------------------------------------------------------------------

@flow(
    name="the-archeologist",
    description=(
        "Runs one excavation season at an invented site and files every "
        "Prefect artifact type along the way — link, markdown, progress, "
        "table, and image — as a mix of task-run and flow-run artifacts. "
        "Marvin generates the typed season data (site, finds, diary) when a "
        "model API key is available; otherwise a seeded fallback keeps every "
        "season different anyway."
    ),
)
def the_archeologist(
    grid_squares: int = 8,
    diary_days: int = 30,
    use_marvin: bool = True,
    marvin_model: str | None = DEFAULT_MARVIN_MODEL,
    corrupted_artifact_type: (
        Literal["link", "markdown", "table", "progress", "image"] | None
    ) = None,
    seed: int | None = None,
) -> dict:
    """
    Parameters
    ----------
    grid_squares:
        How many grid squares the magnetometer survey covers. Defaults to 8.
    diary_days:
        How many days the field diary covers. More days, longer markdown.
        Defaults to 30.
    use_marvin:
        Generate the season's data with Marvin. Falls back to procedural
        data on any failure. Defaults to True.
    marvin_model:
        Pydantic-ai model string for Marvin, e.g. ``ollama:qwen3:30b`` (the
        default, served by a local Ollama), ``openai:gpt-4o``, or
        ``anthropic:claude-sonnet-4-5``. Hosted providers need their API
        key in the environment. None uses Marvin's own default.
    corrupted_artifact_type:
        Pin the cursed tablet's manifestation for testing: one of link,
        markdown, table, progress, or image. Defaults to None — the curse
        picks a different type each season.
    seed:
        Seed for the random parts of the season, for reproducible runs.
        Defaults to None (every season is different).
    """
    logger = get_run_logger()
    if seed is not None:
        random.seed(seed)

    season = str(datetime.now().year)

    # Phase 0 — imagine the season.
    dossier = open_the_season(use_marvin, marvin_model)
    id_prefix = (
        "".join(w[0] for w in dossier.site_name.split() if w[:1].isalpha()).upper()
        + season[-2:]
    )
    logger.info("Season %s opens at %s.", season, dossier.site_name)

    # Flow-run artifact: season progress, updated as each phase completes.
    season_progress_id = create_progress_artifact(
        key="season-progress",
        progress=0.0,
        description=(
            f"Season {season} at {dossier.site_name}: survey, excavation, "
            "conservation, analysis, publication."
        ),
    )

    # Flow-run artifact: the aerial photograph. Two-word description.
    create_image_artifact(
        key="site-aerial-photo",
        image_url=AERIAL_PHOTO_URL,
        description="Day one.",
    )

    # Phase 1 — survey.
    trenches = survey_site(grid_squares, trench_count=random.randint(2, 4))
    update_progress_artifact(artifact_id=season_progress_id, progress=20.0)

    # Phase 2 — excavation, one task per trench.
    all_finds: list[dict] = []
    for trench in trenches:
        all_finds += excavate_trench(
            trench, dossier, id_prefix, start_index=len(all_finds) + 1,
            use_marvin=use_marvin, marvin_model=marvin_model,
        )
    update_progress_artifact(artifact_id=season_progress_id, progress=45.0)

    prize = max(
        all_finds,
        key=lambda f: (CONDITION_RANK.get(f["condition"], 0), random.random()),
    )

    # Flow-run artifact: the master field catalog. LONG markdown description.
    create_table_artifact(
        key="field-catalog",
        table=all_finds,
        description=(
            f"## Master field catalog — {dossier.site_name}, season {season}\n\n"
            f"All {len(all_finds)} registered finds across trenches "
            f"{', '.join(trenches)}, merged from the per-trench registers "
            "after the trenches closed.\n\n"
            "**Cataloging conventions:**\n\n"
            f"- Find numbers are assigned in order of registration, not "
            f"discovery — `{id_prefix}-001` was simply first to the "
            "registrar's table\n"
            "- *Stratum* records where the object sat, not when it was made\n"
            "- *Condition* is the conservator's field grade and gets revised "
            "on the bench, usually downward\n\n"
            "Objects flagged `Excellent` are stored in the finds hut under "
            "lock; everything else lives in labeled crates by trench and "
            "stratum, awaiting the study season."
        ),
    )

    # Phase 3 — conservation. This progress artifact never reaches 100%.
    stall = reconstruct_pottery(all_finds)
    update_progress_artifact(artifact_id=season_progress_id, progress=60.0)

    # Phase 4 — analysis.
    verdict = analyze_stratigraphy(dossier, trenches)
    photograph_prize_find(prize, dossier)
    submit_radiocarbon_samples(sample_count=random.randint(4, 9))
    update_progress_artifact(artifact_id=season_progress_id, progress=80.0)

    # Phase 5 — the long diary, seasoned with the best finds.
    highlights = sorted(
        all_finds, key=lambda f: CONDITION_RANK.get(f["condition"], 0), reverse=True
    )[:3]
    transcribe_field_diary(dossier, highlights, diary_days, use_marvin, marvin_model)

    # Phase 5.5 — the find nobody wants to catalog.
    manifestation = unearth_the_cursed_tablet(corrupted_artifact_type)
    update_progress_artifact(artifact_id=season_progress_id, progress=90.0)

    # Phase 6 — publication. Flow-run artifacts: report and accession record.
    periods = sorted({find["period"] for find in all_finds})
    report = f"""# End-of-season report — {dossier.site_name}, {season}

{dossier.backstory}

## Summary

| Metric | Value |
|--------|-------|
| Grid squares surveyed | {grid_squares} |
| Trenches opened | {len(trenches)} |
| Registered finds | {len(all_finds)} |
| Periods represented | {len(periods)} |
| Prize find | {prize['find_id']} ({prize['object'].lower()}) |
| Pottery reconstruction | stalled at {stall}% |
| The cursed tablet | manifested as a {manifestation} artifact |

## Headline result

{verdict}. The horizon is continuous across all trenches; radiocarbon
results are pending.

## Periods represented

{chr(10).join(f"- {period}" for period in periods)}

> Field journal, final entry: "We came for potsherds and left with a
> {prize['object'].lower()}. The tell keeps its own schedule."

## Next season

- [ ] Extend Trench {trenches[-1]} toward the presumed gate
- [ ] Full study of {prize['find_id']}
- [ ] Publish the stratum II horizon
- [ ] Re-sort the spoil heap for the missing pieces
"""
    create_markdown_artifact(
        key="season-report",
        markdown=report,
        description="Ready for the editor.",
    )

    create_link_artifact(
        key="museum-accession",
        link=MUSEUM_RECORD_URL,
        link_text=f"Museum accession record for {prize['find_id']}",
        description="Accession pending.",
    )

    update_progress_artifact(artifact_id=season_progress_id, progress=100.0)
    logger.info(
        "Season %s closes at %s: %d finds, %d trenches, prize find %s.",
        season, dossier.site_name, len(all_finds), len(trenches), prize["find_id"],
    )

    return {
        "site": dossier.site_name,
        "season": season,
        "trenches": len(trenches),
        "finds": len(all_finds),
        "prize_find": prize["find_id"],
        "pottery_stalled_at": stall,
        "cursed_manifestation": manifestation,
    }


# ---------------------------------------------------------------------------
# Entrypoint — serves the flow as a deployment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_archeologist.serve(name="default")
