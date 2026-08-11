"""
the_archeologist — one field season at Tel Qanara, told entirely in artifacts.

The flow runs a full excavation season and produces every artifact type
Prefect supports (link, markdown, progress, table, image), split across the
two levels the API offers:

- Task-run artifacts (created inside tasks):
    * progress  — live survey progress, updated square by square
    * progress  — pottery reconstruction that stalls at 87.5% and never
                  reaches 100% (two rim sherds are missing)
    * table     — a finds register per trench (one per trench, no description)
    * markdown  — the stratigraphy analysis, Harris matrix included
    * markdown  — the full 30-day field diary (a deliberately LONG document)
    * image     — the field photograph of the prize find
    * link      — the radiocarbon lab's methodology page

- Flow-run artifacts (created in the flow body):
    * progress  — season progress, updated as each phase completes
    * image     — the aerial photograph of the site
    * table     — the master field catalog, merged from all trenches
    * markdown  — the end-of-season report
    * link      — the museum accession record for the prize find

Descriptions are deliberately uneven — some are two words, some are
multi-paragraph markdown, and the trench registers have none at all — to
exercise how the UI renders every description length.
"""

import time

from prefect import flow, get_run_logger, task
from prefect.artifacts import (
    create_image_artifact,
    create_link_artifact,
    create_markdown_artifact,
    create_progress_artifact,
    create_table_artifact,
    update_progress_artifact,
)

SITE_NAME = "Tel Qanara"
SEASON = "2026"

AERIAL_PHOTO_URL = (
    "https://upload.wikimedia.org/wikipedia/commons/e/eb/Machu_Picchu%2C_Peru.jpg"
)
PRIZE_FIND_PHOTO_URL = (
    "https://upload.wikimedia.org/wikipedia/commons/2/23/Rosetta_Stone.JPG"
)
MUSEUM_RECORD_URL = "https://www.britishmuseum.org/collection/object/Y_EA24"
RADIOCARBON_LAB_URL = "https://en.wikipedia.org/wiki/Radiocarbon_dating"

# What the trowels turn up, trench by trench.
TRENCH_FINDS = {
    "t1": [
        {"find_id": "TQ26-001", "object": "Potsherd, red slip ware", "stratum": "II", "material": "Ceramic", "period": "Iron Age II", "condition": "Fragmentary"},
        {"find_id": "TQ26-002", "object": "Bronze fibula", "stratum": "II", "material": "Bronze", "period": "Iron Age II", "condition": "Good"},
        {"find_id": "TQ26-003", "object": "Spindle whorl", "stratum": "III", "material": "Stone", "period": "Late Bronze Age", "condition": "Intact"},
    ],
    "t2": [
        {"find_id": "TQ26-004", "object": "Obsidian blade core", "stratum": "IV", "material": "Obsidian", "period": "Chalcolithic", "condition": "Intact"},
        {"find_id": "TQ26-005", "object": "Clay tablet, inscribed", "stratum": "III", "material": "Clay", "period": "Late Bronze Age", "condition": "Excellent"},
        {"find_id": "TQ26-006", "object": "Amphora handle, stamped", "stratum": "I", "material": "Ceramic", "period": "Hellenistic", "condition": "Fragmentary"},
    ],
    "t3": [
        {"find_id": "TQ26-007", "object": "Basalt grinding stone", "stratum": "III", "material": "Basalt", "period": "Late Bronze Age", "condition": "Good"},
        {"find_id": "TQ26-008", "object": "Granodiorite stele, trilingual", "stratum": "II", "material": "Granodiorite", "period": "Ptolemaic (intrusive)", "condition": "Excellent"},
    ],
}

PRIZE_FIND_ID = "TQ26-008"


# ---------------------------------------------------------------------------
# Phase 1 — survey (task-run progress artifact, SHORT description)
# ---------------------------------------------------------------------------

@task
def survey_site(grid_squares: int = 8) -> list[str]:
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

    trenches = sorted(TRENCH_FINDS)
    logger.info("Survey complete. Anomalies found. Opening trenches: %s", trenches)
    return trenches


# ---------------------------------------------------------------------------
# Phase 2 — excavation (task-run table artifacts, NO description)
# ---------------------------------------------------------------------------

@task
def excavate_trench(trench: str) -> list[dict]:
    """Excavate one trench and file its finds register."""
    logger = get_run_logger()

    time.sleep(0.75)
    finds = TRENCH_FINDS[trench]

    # Deliberately no description: the bare-table rendering case.
    create_table_artifact(
        key=f"finds-register-{trench}",
        table=finds,
    )

    logger.info("Trench %s closed with %d finds.", trench.upper(), len(finds))
    return finds


# ---------------------------------------------------------------------------
# Phase 3 — conservation (task-run progress artifact that NEVER reaches 100%)
# ---------------------------------------------------------------------------

@task
def reconstruct_pottery(finds: list[dict]) -> float:
    """Reassemble the season's ceramics. Two rim sherds are missing, so this
    reconstruction stalls at 87.5% and will never finish."""
    logger = get_run_logger()

    ceramics = [f for f in finds if f["material"] == "Ceramic"]
    progress_id = create_progress_artifact(
        key="pottery-reconstruction",
        progress=0.0,
        description=(
            f"Reassembly of the season's {len(ceramics)} registered ceramic "
            "finds on the conservation bench.\n\n"
            "**Known issue:** two rim sherds of the red slip ware bowl "
            "(TQ26-001) were never recovered from the sieve. Unless they turn "
            "up in next season's spoil-heap re-sort, *this artifact will stay "
            "below 100% forever* — which is exactly what conservation feels "
            "like."
        ),
    )

    for step in (15.0, 30.0, 45.0, 60.0, 72.5, 80.0, 87.5):
        time.sleep(0.4)
        update_progress_artifact(artifact_id=progress_id, progress=step)

    logger.warning(
        "Reconstruction stalled at 87.5%. Two rim sherds missing; "
        "the bowl stays incomplete."
    )
    return 87.5


# ---------------------------------------------------------------------------
# Phase 4 — analysis (task-run markdown, image, and link artifacts)
# ---------------------------------------------------------------------------

@task
def analyze_stratigraphy(trenches: list[str]) -> str:
    """Read the sections and file the stratigraphy analysis."""
    logger = get_run_logger()

    time.sleep(1)
    markdown = f"""# Stratigraphy analysis — {SITE_NAME}, season {SEASON}

Sections were drawn for trenches {", ".join(t.upper() for t in trenches)}.
Four strata are present across the tell. Stratum II shows a burn layer in
every trench: a single destruction event.

## Strata

| Stratum | Period            | Depth (m)  | Notes                          |
|---------|-------------------|------------|--------------------------------|
| I       | Hellenistic       | 0.0 – 0.6  | Plow-disturbed topsoil         |
| II      | Iron Age II       | 0.6 – 1.8  | **Destruction layer** — ash and collapsed mudbrick |
| III     | Late Bronze Age   | 1.8 – 3.1  | Domestic floors, tabun ovens   |
| IV      | Chalcolithic      | 3.1 – 4.0  | Sterile below 4.0 m            |

## Harris matrix (simplified)

```text
        [topsoil 001]
              |
        [ash 014]        <- destruction event
         /        \\
   [floor 022]  [wall 023]
         \\        /
        [fill 031]
              |
        [bedrock 040]
```

> Field journal, day 19: "The ash line runs unbroken through all three
> trenches. Whatever happened here, it happened everywhere at once."

## Interpretation

- [x] Confirm the burn layer is a single event (charcoal lensing is continuous)
- [x] Date the event — samples sent for radiocarbon dating
- [ ] Identify the destroyer — no arrowheads recovered yet
"""

    create_markdown_artifact(
        key="stratigraphy-analysis",
        markdown=markdown,
        description="Section drawings, Harris matrix, and interpretation.",
    )

    logger.info("Stratigraphy analysis filed. Four strata identified.")
    return "Stratum II destruction layer, single event"


@task
def photograph_prize_find(find_id: str) -> str:
    """Photograph the season's prize find. LONG markdown description."""
    logger = get_run_logger()

    time.sleep(0.5)
    create_image_artifact(
        key="prize-find-photo",
        image_url=PRIZE_FIND_PHOTO_URL,
        description=(
            f"## Field photograph — find {find_id}\n\n"
            "Trilingual granodiorite stele from Trench T3, stratum II, as it "
            "sat on the registration table the evening it came out of the "
            "ground. The stele is *intrusive* in stratum II — Ptolemaic "
            "material in an Iron Age destruction layer — which is either a "
            "later pit we missed in section, or the single most interesting "
            "problem this project has ever produced.\n\n"
            "**Photography record:**\n\n"
            "- Camera: full-frame body, 50 mm macro, f/8, ISO 100\n"
            "- Lighting: raking light from the north-west to raise the "
            "inscription\n"
            "- Scale: 10 cm bar, north arrow at lower left\n"
            "- Filed as negatives `TQ26-N-0834` through `TQ26-N-0851`\n\n"
            "The registrar notes that all three scripts are legible along the "
            "left margin, and the middle register ends mid-sentence — the "
            "stele was cut down for reuse in antiquity. Epigraphic study is "
            "scheduled for next season; see the end-of-season report for the "
            "publication plan."
        ),
    )

    logger.info("Prize find %s photographed.", find_id)
    return PRIZE_FIND_PHOTO_URL


@task
def submit_radiocarbon_samples(sample_count: int = 6) -> str:
    """Send charcoal samples to the lab and file the chain-of-custody link."""
    logger = get_run_logger()

    time.sleep(0.5)
    create_link_artifact(
        key="radiocarbon-lab",
        link=RADIOCARBON_LAB_URL,
        link_text="Radiocarbon dating methodology (lab reference)",
        description=(
            f"{sample_count} charcoal samples from the stratum II burn layer, "
            "submitted for AMS dating.\n\n"
            "**Chain of custody:**\n\n"
            "1. Lifted in foil by the trench supervisors, never handled bare\n"
            "2. Logged against locus and basket numbers in the field register\n"
            "3. Couriered to the lab with the sample manifest\n"
            "4. Results expected before the study season\n\n"
            "If the dates cluster, the destruction event gets an absolute "
            "anchor and the whole stratum II assemblage dates with it."
        ),
    )

    logger.info("%d samples submitted for radiocarbon dating.", sample_count)
    return "samples in transit"


# ---------------------------------------------------------------------------
# Phase 5 — the field diary (task-run markdown artifact, deliberately LONG)
# ---------------------------------------------------------------------------

@task
def transcribe_field_diary(days: int = 30) -> int:
    """Type up the director's handwritten field diary — the long one."""
    logger = get_run_logger()

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
        "mudbrick collapse just right and even the skeptics took pictures.",
    )
    special = {
        1: "The lorry delivered the equipment to the wrong wadi. Day spent "
           "carrying theodolite boxes across a landscape that has not "
           "changed since the Chalcolithic, and feels it.",
        3: "The bronze fibula came out of T1 stratum II an hour before "
           "close. First real find of the season; the trench supervisor "
           "bought the evening's soft drinks as tradition demands.",
        7: "Sandstorm. Trenches tarped by nine, camp shuttered by ten, "
           "everyone re-reading last season's report by eleven. The tell "
           "has weathered worse.",
        11: "The inscribed clay tablet surfaced in T2 stratum III during "
            "the last pass of the morning. Work in the square stopped for "
            "an hour while it was lifted, and nobody minded.",
        19: "The ash line runs unbroken through all three trenches. "
            "Whatever happened here, it happened everywhere at once. The "
            "evening's argument about who did it lasted until the "
            "generator was switched off.",
        22: "The stele. Three scripts, one stone, found face-down as a "
            "threshold slab in T3. The whole camp filed past it before "
            "dinner like a receiving line. Nobody will remember this "
            "season for anything else.",
    }

    sections = [
        f"# Field diary — {SITE_NAME}, season {SEASON}",
        "",
        "Transcribed from the director's three notebooks at the end of the "
        "season. Spelling normalized, complaints preserved.",
        "",
    ]
    for day in range(1, days + 1):
        sections += [
            f"### Day {day} — {weather[day % len(weather)]}",
            "",
            special.get(day, routine[day % len(routine)]),
            "",
        ]
        if day % 10 == 0:
            sections += [
                f"**Decade summary, day {day}:**",
                "",
                "| Measure | Count |",
                "|---------|-------|",
                f"| Buckets sieved | {day * 31} |",
                f"| Sherds washed | {day * 118} |",
                f"| Find cards filed | {day * 4} |",
                f"| Arguments about stratum II | {day // 2} |",
                "",
            ]
    sections += [
        "---",
        "",
        "> Final page, unsigned: \"We came for potsherds and left with a "
        "trilingual stele. The tell keeps its own schedule.\"",
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
# The flow — one field season, flow-run artifacts filed along the way
# ---------------------------------------------------------------------------

@flow(
    name="the-archeologist",
    description=(
        "Runs one excavation season at a fictional tell and files every "
        "Prefect artifact type along the way — link, markdown, progress, "
        "table, and image — as a mix of task-run and flow-run artifacts, "
        "with description lengths from two words to multi-paragraph "
        "markdown, one wall-of-text diary, and a progress artifact that "
        "never reaches 100%."
    ),
)
def the_archeologist(grid_squares: int = 8, diary_days: int = 30) -> dict:
    """
    Parameters
    ----------
    grid_squares:
        How many grid squares the magnetometer survey covers. Defaults to 8.
    diary_days:
        How many days the field diary covers. More days, longer markdown.
        Defaults to 30.
    """
    logger = get_run_logger()
    logger.info("Season %s opens at %s.", SEASON, SITE_NAME)

    # Flow-run artifact: season progress, updated as each phase completes.
    season_progress_id = create_progress_artifact(
        key="season-progress",
        progress=0.0,
        description=(
            f"Season {SEASON} at {SITE_NAME}: survey, excavation, "
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
    trenches = survey_site(grid_squares)
    update_progress_artifact(artifact_id=season_progress_id, progress=20.0)

    # Phase 2 — excavation, one task per trench.
    all_finds: list[dict] = []
    for trench in trenches:
        finds = excavate_trench(trench)
        for find in finds:
            all_finds.append({"trench": trench.upper(), **find})
    update_progress_artifact(artifact_id=season_progress_id, progress=45.0)

    # Flow-run artifact: the master field catalog. LONG markdown description.
    create_table_artifact(
        key="field-catalog",
        table=all_finds,
        description=(
            f"## Master field catalog — {SITE_NAME}, season {SEASON}\n\n"
            f"All {len(all_finds)} registered finds across trenches "
            f"{', '.join(t.upper() for t in trenches)}, merged from the "
            "per-trench registers after the trenches closed.\n\n"
            "**Cataloging conventions:**\n\n"
            "- Find numbers are assigned in order of registration, not "
            "discovery — `TQ26-001` was simply first to the registrar's "
            "table\n"
            "- *Stratum* records where the object sat, not when it was "
            "made; the stele is the season's reminder that those differ\n"
            "- *Condition* is the conservator's field grade and gets "
            "revised on the bench, usually downward\n\n"
            "Objects flagged `Excellent` are stored in the finds hut under "
            "lock; everything else lives in labeled crates by trench and "
            "stratum, awaiting the study season."
        ),
    )

    # Phase 3 — conservation. This progress artifact stalls at 87.5%.
    reconstruct_pottery(all_finds)
    update_progress_artifact(artifact_id=season_progress_id, progress=60.0)

    # Phase 4 — analysis.
    verdict = analyze_stratigraphy(trenches)
    photograph_prize_find(PRIZE_FIND_ID)
    submit_radiocarbon_samples()
    update_progress_artifact(artifact_id=season_progress_id, progress=80.0)

    # Phase 5 — the long diary.
    transcribe_field_diary(diary_days)

    # Phase 6 — publication. Flow-run artifacts: report and accession record.
    periods = sorted({find["period"] for find in all_finds})
    report = f"""# End-of-season report — {SITE_NAME}, {SEASON}

## Summary

| Metric               | Value |
|----------------------|-------|
| Grid squares surveyed | {grid_squares} |
| Trenches opened       | {len(trenches)} |
| Registered finds      | {len(all_finds)} |
| Periods represented   | {len(periods)} |
| Prize find            | {PRIZE_FIND_ID} (trilingual stele) |

## Headline result

{verdict}. The stratum II burn layer is continuous across all trenches;
radiocarbon results are pending.

## Periods represented

{chr(10).join(f"- {period}" for period in periods)}

> Field journal, final entry: "We came for potsherds and left with a
> trilingual stele. The tell keeps its own schedule."

## Next season

- [ ] Extend Trench T3 north toward the presumed gate
- [ ] Full epigraphic study of {PRIZE_FIND_ID}
- [ ] Publish the stratum II destruction horizon
- [ ] Re-sort the spoil heap for the missing rim sherds
"""
    create_markdown_artifact(
        key="season-report",
        markdown=report,
        description="Ready for the editor.",
    )

    create_link_artifact(
        key="museum-accession",
        link=MUSEUM_RECORD_URL,
        link_text=f"Museum accession record for {PRIZE_FIND_ID}",
        description="Accession pending.",
    )

    update_progress_artifact(artifact_id=season_progress_id, progress=100.0)
    logger.info(
        "Season %s closes: %d finds, %d trenches.",
        SEASON, len(all_finds), len(trenches),
    )

    return {
        "site": SITE_NAME,
        "season": SEASON,
        "trenches": len(trenches),
        "finds": len(all_finds),
        "prize_find": PRIZE_FIND_ID,
    }


# ---------------------------------------------------------------------------
# Entrypoint — serves the flow as a deployment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_archeologist.serve(name="default")
