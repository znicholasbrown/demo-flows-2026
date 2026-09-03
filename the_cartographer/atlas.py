"""
atlas — a deterministic, prefix-stable asset graph for load testing.

Every asset is generated from its own seeded random stream keyed by
``(seed, index)``, and its upstream edges only ever point at lower indices.
Two consequences:

- Any scale is an exact prefix of a larger scale with the same seed.
  ``generate_atlas(seed, 1000)`` is the first thousand assets of
  ``generate_atlas(seed, 5000)``, keys and edges included. Scaling down never
  invents assets; it re-materializes the ones that already exist.
- The graph is acyclic by construction.

Shape: five layers (sources, landing, warehouse, models, serving) spread
across thirteen realms (``s3://``, ``snowflake://``, ``dbt://`` ...). Each
non-source asset draws a heavy-tailed number of upstreams, mostly from the
layer above with some skip-layer edges. On top: a few hub sources that fan
out to many downstreams, and three interleaved chains that link every
twenty-fifth asset to the one before it, so depth grows with scale.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

LAYERS = ("sources", "landing", "warehouse", "models", "serving")
LAYER_WEIGHTS = (0.15, 0.20, 0.35, 0.20, 0.10)

LAYER_REALMS = {
    0: ("s3", "sharepoint", "salesforce", "api"),
    1: ("motherduck", "azure", "gcs"),
    2: ("snowflake", "sql", "bigquery"),
    3: ("dbt",),
    4: ("powerbi", "tableau", "s3"),
}
REALMS = tuple(dict.fromkeys(realm for realms in LAYER_REALMS.values() for realm in realms))

TOOLS = {
    "s3": "spark",
    "sharepoint": "airbyte",
    "salesforce": "fivetran",
    "api": "airbyte",
    "motherduck": "duckdb",
    "azure": "spark",
    "gcs": "spark",
    "snowflake": "dbt",
    "sql": "sqlmesh",
    "bigquery": "dbt",
    "dbt": "dbt",
    "powerbi": "powerbi",
    "tableau": "tableau",
}

# Assets whose index satisfies these rules get a fixed structural role.
WARMUP_PER_LAYER = 8  # the first 40 assets fill each layer in turn
HUB_EVERY = 400  # index % HUB_EVERY == 1 is a hub source
CHAIN_STRIDE = 25  # chain members link to index - CHAIN_STRIDE
CHAIN_OFFSETS = (2, 9, 16)  # three interleaved chains
HUB_EDGE_PROBABILITY = 0.15
SKIP_LAYER_PROBABILITY = 0.15
MAX_UPSTREAMS = 40

DOMAINS = (
    "sales", "finance", "hr", "marketing", "supply_chain", "inventory", "orders",
    "customers", "products", "shipments", "returns", "payments", "invoices",
    "ledger", "budget", "forecast", "campaigns", "leads", "tickets", "sessions",
    "pricing", "contracts", "vendors", "assets", "fleet", "warranty", "quality",
    "energy", "payroll", "recruiting",
)
SUFFIXES = (
    "raw", "staged", "clean", "daily", "monthly", "fact", "dim", "agg",
    "snapshot", "history", "summary", "v2", "current", "archive",
)
OWNERS = (
    "data-platform@acme.io", "analytics-eng@acme.io", "finance-data@acme.io",
    "sales-ops@acme.io", "bi-team@acme.io", "ml-platform@acme.io",
)
SF_OBJECTS = (
    "account", "contact", "opportunity", "lead", "case", "contentversion",
    "campaign", "product2", "pricebook2", "order", "contract", "task", "event",
    "user", "asset", "quote", "invoice__c", "shipment__c", "warranty__c",
)
SITES = ("hr", "finance", "ops", "sales", "marketing")
LIBRARIES = ("budget", "policies", "forecasts", "reports", "uploads")
API_HOSTS = ("api.partner-one.com", "api.shipping-hub.io", "api.rates.example")


@dataclass(frozen=True)
class AtlasAsset:
    index: int
    key: str
    realm: str
    layer: int
    tool: str
    upstream: tuple[int, ...]
    name: str
    description: str
    owners: tuple[str, ...]
    url: str


@dataclass(frozen=True)
class Atlas:
    seed: int
    edge_factor: float
    assets: tuple[AtlasAsset, ...]

    def upstream_keys(self, asset: AtlasAsset) -> list[str]:
        return [self.assets[i].key for i in asset.upstream]


def _stream(seed: int, index: int, purpose: str = "gen") -> random.Random:
    return random.Random(f"{seed}:{purpose}:{index}")


def _layer_for(index: int, rng: random.Random) -> int:
    if index % HUB_EVERY == 1:
        return 0
    if index < WARMUP_PER_LAYER * len(LAYERS):
        return index // WARMUP_PER_LAYER
    return rng.choices(range(len(LAYERS)), weights=LAYER_WEIGHTS)[0]


def _table(rng: random.Random) -> str:
    return f"{rng.choice(DOMAINS)}_{rng.choice(SUFFIXES)}"


def _base_key(realm: str, rng: random.Random) -> str:
    table = _table(rng)
    folder = rng.choice(DOMAINS)
    if realm == "s3":
        bucket = rng.choice(("acme-lake-prd", "acme-lake-dev", "acme-exports"))
        return f"s3://{bucket}/{folder}/{rng.choice(SUFFIXES)}/{table}.parquet"
    if realm == "sharepoint":
        return f"sharepoint://graph.microsoft.com/{rng.choice(SITES)}/{rng.choice(LIBRARIES)}/{table}"
    if realm == "salesforce":
        return f"salesforce://acme.my.salesforce.com/{rng.choice(SF_OBJECTS)}"
    if realm == "api":
        return f"api://{rng.choice(API_HOSTS)}/v{rng.randint(1, 3)}/{folder}/{table}"
    if realm == "motherduck":
        return f"motherduck://acme_{rng.choice(('prd', 'dev'))}/{folder}/{table}"
    if realm == "azure":
        account = rng.choice(("acmedatalakeprd", "acmedatalakedev", "acmedatalakestg"))
        return f"azure://{account}/{rng.choice(SITES)}/{folder}/{rng.choice(SUFFIXES)}/{table}"
    if realm == "gcs":
        return f"gcs://acme-{rng.choice(('landing', 'staging'))}/{folder}/{table}"
    if realm == "snowflake":
        region = rng.choice(("central-us.azure", "us-east-1", "eu-west-1"))
        db = rng.choice(("RAW_PRD", "RAW_DEV", "ANALYTICS", "MART"))
        return f"snowflake://xy{rng.randint(10000, 99999)}.{region}/{db}/{folder.upper()}/{table.upper()}"
    if realm == "sql":
        host = rng.choice(("db-erp-prd.acme.local", "db-crm.acme.local", "pg-warehouse.acme.local"))
        return f"sql://{host}/{rng.choice(('erp', 'crm', 'warehouse'))}/{folder}/{table}"
    if realm == "bigquery":
        return f"bigquery://acme-{rng.choice(('prod', 'analytics'))}/{folder}/{table}"
    if realm == "dbt":
        project = rng.choice(("acme_core", "acme_finance", "acme_marts"))
        kind = rng.choice(("model", "model", "model", "tag"))
        leaf = table if kind == "model" else folder
        return f"dbt://ni{rng.randint(100, 999)}.us1.dbt.com/{project}/{kind}/{leaf}"
    if realm == "powerbi":
        return f"powerbi://api.powerbi.com/{rng.choice(SITES)}-workspace/{table}"
    if realm == "tableau":
        return f"tableau://acme.online.tableau.com/{rng.choice(SITES)}/{table}_dashboard"
    raise ValueError(f"unknown realm {realm}")


def _unique_key(base: str, taken: dict[str, int]) -> str:
    """Append a deterministic counter when a base key was already used."""
    count = taken.get(base, 0)
    taken[base] = count + 1
    if count == 0:
        return base
    stem, dot, extension = base.rpartition(".")
    if dot and "/" not in extension:
        return f"{stem}_{count}.{extension}"
    return f"{base}_{count}"


def _pick_upstreams(
    index: int,
    layer: int,
    rng: random.Random,
    by_layer: dict[int, list[int]],
    hubs: list[int],
    mean_upstreams: float,
) -> tuple[int, ...]:
    chosen: set[int] = set()

    if layer > 0:
        count = min(MAX_UPSTREAMS, 1 + int(rng.expovariate(1 / mean_upstreams)))
        previous = by_layer.get(layer - 1, [])
        deeper = [i for lower in range(layer - 1) for i in by_layer.get(lower, [])]
        everything = [i for lower in range(layer) for i in by_layer.get(lower, [])]
        for _ in range(count):
            pool = deeper if deeper and rng.random() < SKIP_LAYER_PROBABILITY else previous
            pool = pool or everything
            if pool:
                chosen.add(rng.choice(pool))

    if index >= WARMUP_PER_LAYER * len(LAYERS) and hubs and rng.random() < HUB_EDGE_PROBABILITY:
        chosen.add(rng.choice(hubs))

    if index >= CHAIN_STRIDE and index % CHAIN_STRIDE in CHAIN_OFFSETS:
        chosen.add(index - CHAIN_STRIDE)

    return tuple(sorted(chosen))


def _display(realm: str, key: str, layer: int, rng: random.Random) -> tuple[str, str, tuple[str, ...], str]:
    leaf = key.rstrip("/").rsplit("/", 1)[-1]
    stem = leaf.split(".")[0]
    name = " ".join(part.capitalize() for part in stem.replace("-", "_").split("_") if part)
    description = (
        f"{LAYERS[layer].capitalize()} asset in the {realm} realm, "
        f"materialized by {TOOLS[realm]}. Generated by the_cartographer for catalog load testing."
    )
    owners = tuple(sorted(rng.sample(OWNERS, k=rng.choice((1, 1, 2)))))
    url = f"https://catalog.acme.io/{realm}/{stem}"
    return name, description, owners, url


def generate_atlas(seed: int, asset_count: int, edge_factor: float = 4.0) -> Atlas:
    """Generate ``asset_count`` assets; see the module docstring for the shape."""
    source_share = LAYER_WEIGHTS[0]
    mean_upstreams = max(0.1, edge_factor / (1 - source_share) - 1)

    assets: list[AtlasAsset] = []
    by_layer: dict[int, list[int]] = {}
    hubs: list[int] = []
    taken: dict[str, int] = {}

    for index in range(asset_count):
        rng = _stream(seed, index)
        layer = _layer_for(index, rng)
        realm = rng.choice(LAYER_REALMS[layer])
        key = _unique_key(_base_key(realm, rng), taken)
        upstream = _pick_upstreams(index, layer, rng, by_layer, hubs, mean_upstreams)
        name, description, owners, url = _display(realm, key, layer, rng)

        assets.append(
            AtlasAsset(
                index=index,
                key=key,
                realm=realm,
                layer=layer,
                tool=TOOLS[realm],
                upstream=upstream,
                name=name,
                description=description,
                owners=owners,
                url=url,
            )
        )
        by_layer.setdefault(layer, []).append(index)
        if index % HUB_EVERY == 1:
            hubs.append(index)

    return Atlas(seed=seed, edge_factor=edge_factor, assets=tuple(assets))


def select_subset(atlas: Atlas, fraction: float) -> list[AtlasAsset]:
    """A deterministic fraction of the atlas, stable across scales."""
    return [
        asset
        for asset in atlas.assets
        if _stream(atlas.seed, asset.index, "subset").random() < fraction
    ]
