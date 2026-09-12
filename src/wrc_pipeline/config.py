"""Single source of truth for every tunable value in the pipeline.

The exercise requires that all connection strings, storage paths, partition
sizes and scraping parameters be configurable, with no hardcoded values. This
module is how that requirement is met, and it follows one rule:

    config/settings.yaml  ->  pipeline BEHAVIOUR
                              (partition size, bucket and collection names,
                              body IDs, selectors, Scrapy tuning)

    environment / .env    ->  CONNECTIONS, SECRETS and ENVIRONMENT
                              (URIs, credentials, log level, log destination)

Every value has exactly one source, and which file it lives in tells you which.
There is deliberately no partial "env can override some yaml keys" mechanism,
because that immediately raises the unanswerable question of why one key is
overridable and the next one is not. To run with different behaviour, point
WRC_CONFIG_FILE at a different YAML file - the whole behavioural profile swaps
at once, which is both simpler to explain and simpler to reason about.

Configuration errors are collected and raised together rather than one at a
time: finding out about four missing variables in four consecutive runs is a
bad way to spend an afternoon.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import yaml
from dotenv import load_dotenv

from wrc_pipeline.partitions import PARTITION_SIZES

# config.py lives at src/wrc_pipeline/config.py, so the repository root is two
# levels up. Derived from __file__ rather than the current working directory so
# that the pipeline behaves the same whether it is launched from the repo root,
# from scripts/, or by an orchestrator with a working directory of its own.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_FILE = PROJECT_ROOT / "config" / "settings.yaml"

# Partition sizes are validated at load time so that a typo fails at startup
# rather than after the crawl has already been running for ten minutes. The
# list is imported from partitions.py rather than restated here: that module
# implements the slicing, so it is the only thing that actually knows which
# sizes work. Restating it would let the two drift, and the failure mode of
# that drift is a config value that validates and then raises mid-crawl.
VALID_PARTITION_SIZES = PARTITION_SIZES

# The date template must produce d/M/yyyy with no leading zeros. Checked at load
# time because getting it wrong makes the site return zero results *silently*.
REQUIRED_DATE_PLACEHOLDERS = ("{day}", "{month}", "{year}")


class ConfigError(RuntimeError):
    """Configuration is missing or invalid.

    Deliberately fatal. A pipeline that starts with half its configuration and
    discovers the rest is missing mid-crawl produces a partial Landing Zone
    that is worse than no Landing Zone, because it looks like a successful run.
    """


# --------------------------------------------------------------------------
# Settings objects
#
# Frozen dataclasses rather than plain dicts: attribute access (settings.mongo
# .database) fails loudly on a typo, whereas settings["mongo"]["databse"] fails
# at the point of use with a KeyError that says nothing useful. Frozen because
# nothing should be rewriting configuration at runtime.
#
# Plain dataclasses rather than pydantic: the validation needed here is a
# handful of explicit checks, and adding a dependency for it would be harder to
# justify than the twenty lines it replaces.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceSettings:
    """Everything specific to the Workplace Relations site.

    Isolating the site-specific parts here is the first half of the answer to
    "what would you change to support 50+ sources": a second source becomes a
    second settings block, not a second pipeline.
    """

    name: str
    base_url: str
    search_path: str
    date_format: str
    results_per_page: int
    # Regexes matching per-request noise to strip before hashing or storing.
    # A tuple so Settings stays hashable and immutable.
    volatile_patterns: tuple[str, ...] = ()
    # CSS selectors locating the decision itself on a detail page, most
    # specific first. Used by the transformation step.
    content_selectors: tuple[str, ...] = ()

    @property
    def search_url(self) -> str:
        """Absolute URL of the decisions search endpoint."""
        return urljoin(self.base_url, self.search_path)

    def format_date(self, value: date) -> str:
        """Render a date the way the site's ``from``/``to`` filters expect.

        Irish convention, d/M/yyyy, with NO leading zeros: 1/10/2025, not
        01/10/2025. The site accepts the zero-padded form without complaint and
        returns an empty result set, so an error here looks like "that month
        had no decisions" rather than like a bug.

        Substituting integers is what strips the zeros - ``1`` formats as "1"
        and never "01" - which keeps this working identically on Windows, where
        the strftime equivalent ``%-d`` raises ValueError.
        """
        return self.date_format.format(
            day=value.day, month=value.month, year=value.year
        )


@dataclass(frozen=True)
class PartitioningSettings:
    """How a requested date range is sliced into units of work."""

    size: str


@dataclass(frozen=True)
class TransformSettings:
    """How the Landing Zone is turned into the Curated Zone."""

    strip_tags: tuple[str, ...]
    # Cleaned documents shorter than this are flagged rather than rejected -
    # a run of them signals a changed page template.
    min_content_chars: int


@dataclass(frozen=True)
class MongoSettings:
    """Metadata store. Landing and curated are separate collections.

    The exercise requires the Landing Zone to be immutable and the transform to
    write somewhere new, so the two collection names are configuration rather
    than something derived - it must be impossible to accidentally point both
    at the same place.
    """

    uri: str
    database: str
    landing_collection: str
    curated_collection: str
    state_collection: str | None = None

    @property
    def current_collection(self) -> str:
        """Mutable crawl observations live outside the immutable landing data."""
        return self.state_collection or f"{self.landing_collection}_state"

    @property
    def safe_uri(self) -> str:
        """The URI with any password removed, for logging.

        Connection strings carry credentials, and log lines outlive the run.
        """
        return redact_uri(self.uri)


@dataclass(frozen=True)
class ObjectStoreSettings:
    """S3-compatible object storage (MinIO locally, S3 or equivalent later).

    ``endpoint_url`` is the only value tying this to MinIO. The pipeline talks
    to it through boto3 using the standard S3 API, so moving to real S3 is a
    change to this one setting rather than a change to any code.
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    landing_bucket: str
    curated_bucket: str


@dataclass(frozen=True)
class ScrapingSettings:
    """Scrapy tuning, translated into Scrapy's own setting names in Step 4.

    The exercise asks for "the fastest way to scrape the URLs without getting
    blocked". AutoThrottle is the primary control because it adapts to the
    latency the site actually exhibits, rather than to a delay guessed in
    advance; download_delay is a floor beneath it, not the main lever.
    """

    autothrottle_enabled: bool
    autothrottle_start_delay: float
    autothrottle_max_delay: float
    autothrottle_target_concurrency: float
    download_delay: float
    concurrent_requests: int
    concurrent_requests_per_domain: int
    download_timeout: int
    retry_times: int
    retry_http_codes: tuple[int, ...]
    user_agent: str
    httpcache_enabled: bool
    httpcache_expiration_secs: int


@dataclass(frozen=True)
class LoggingSettings:
    """Log level and optional file destination.

    Environment rather than YAML: how loudly a run logs, and where those logs
    land, is a property of where it is running, not of the pipeline.
    """

    level: str
    file: Path | None


@dataclass(frozen=True)
class Settings:
    """The complete, validated configuration for one run."""

    source: SourceSettings
    bodies: dict[str, int]
    partitioning: PartitioningSettings
    transform: TransformSettings
    mongo: MongoSettings
    object_store: ObjectStoreSettings
    scraping: ScrapingSettings
    logging: LoggingSettings
    config_file: Path

    def body_id(self, name: str) -> int:
        """Look up a body's numeric site ID, failing with the valid options.

        A KeyError here would only say 'labour-court'; this says what was
        actually available, which is the difference between a five-second fix
        and a ten-minute one.
        """
        try:
            return self.bodies[name]
        except KeyError:
            raise ConfigError(
                f"Unknown body {name!r}. Configured bodies: "
                f"{', '.join(sorted(self.bodies))}"
            ) from None


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def redact_uri(uri: str) -> str:
    """Strip the password from a URI so it is safe to log.

    mongodb://user:secret@host:27017/  ->  mongodb://user:***@host:27017/
    """
    try:
        parts = urlsplit(uri)
    except ValueError:
        return "<unparseable uri>"
    if "@" not in parts.netloc:
        return uri
    credentials, _, host = parts.netloc.rpartition("@")
    user, _, password = credentials.partition(":")
    netloc = f"{user}:***@{host}" if password else f"{user}@{host}"
    return urlunsplit(parts._replace(netloc=netloc))


def _env(name: str, errors: list[str], *, default: str | None = None) -> str:
    """Read a required environment variable, recording rather than raising.

    Recording lets the caller report every missing variable at once.
    """
    value = os.environ.get(name, default)
    if value is None or value == "":
        errors.append(f"environment variable {name} is required but not set")
        return ""
    return value


def _section(data: dict[str, Any], name: str, errors: list[str]) -> dict[str, Any]:
    """Pull a top-level mapping out of the YAML, recording if it is missing."""
    section = data.get(name)
    if not isinstance(section, dict):
        errors.append(f"config file is missing a '{name}:' section")
        return {}
    return section


def _key(
    section: dict[str, Any],
    section_name: str,
    key: str,
    errors: list[str],
    *,
    cast: type | None = None,
) -> Any:
    """Pull a key out of a YAML section, recording if missing or uncastable."""
    if key not in section:
        errors.append(f"config file is missing '{section_name}.{key}'")
        return None
    value = section[key]
    if cast is not None:
        try:
            return cast(value)
        except (TypeError, ValueError):
            errors.append(
                f"config value '{section_name}.{key}' should be "
                f"{cast.__name__}, got {value!r}"
            )
            return None
    return value


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


def load_settings(
    config_file: str | Path | None = None,
    *,
    load_env: bool = True,
) -> Settings:
    """Build a validated Settings object from YAML plus the environment.

    Args:
        config_file: YAML file to read. Defaults to $WRC_CONFIG_FILE, then to
            config/settings.yaml in the repository root.
        load_env: Read a .env file into the environment first. Disabled by the
            tests so they can control the environment precisely.

    Raises:
        ConfigError: with *every* problem found, not just the first.
    """
    if load_env:
        # override=False: a variable already exported in the shell or injected
        # by the orchestrator beats the .env file on disk. That is the ordering
        # a container deployment needs.
        load_dotenv(PROJECT_ROOT / ".env", override=False)

    path = Path(
        config_file
        or os.environ.get("WRC_CONFIG_FILE")
        or DEFAULT_CONFIG_FILE
    ).resolve()

    if not path.is_file():
        raise ConfigError(
            f"Config file not found: {path}\n"
            f"Set WRC_CONFIG_FILE, or restore {DEFAULT_CONFIG_FILE}."
        )

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Config file {path} is not valid YAML:\n{exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError(f"Config file {path} must contain a mapping at the top level")

    errors: list[str] = []

    # --- source ---
    src = _section(raw, "source", errors)
    source = SourceSettings(
        name=_key(src, "source", "name", errors, cast=str),
        base_url=_key(src, "source", "base_url", errors, cast=str),
        search_path=_key(src, "source", "search_path", errors, cast=str),
        date_format=_key(src, "source", "date_format", errors, cast=str),
        results_per_page=_key(src, "source", "results_per_page", errors, cast=int),
        # Optional: a source with no volatile content simply declares none.
        volatile_patterns=tuple(src.get("volatile_patterns") or ()),
        content_selectors=tuple(src.get("content_selectors") or ()),
    )
    if source.date_format:
        missing = [p for p in REQUIRED_DATE_PLACEHOLDERS if p not in source.date_format]
        if missing:
            errors.append(
                f"config value 'source.date_format' must contain "
                f"{', '.join(REQUIRED_DATE_PLACEHOLDERS)}; missing {', '.join(missing)}. "
                f"Got {source.date_format!r} - note this is a str.format template, "
                f"not a strftime pattern."
            )

    # --- bodies ---
    bodies_raw = raw.get("bodies")
    bodies: dict[str, int] = {}
    if not isinstance(bodies_raw, dict) or not bodies_raw:
        errors.append("config file is missing a non-empty 'bodies:' section")
    else:
        for name, body_id in bodies_raw.items():
            try:
                bodies[str(name)] = int(body_id)
            except (TypeError, ValueError):
                errors.append(f"body ID for {name!r} must be an integer, got {body_id!r}")

    # --- partitioning ---
    part = _section(raw, "partitioning", errors)
    size = _key(part, "partitioning", "size", errors, cast=str)
    if size is not None and size not in VALID_PARTITION_SIZES:
        errors.append(
            f"config value 'partitioning.size' must be one of "
            f"{', '.join(VALID_PARTITION_SIZES)}; got {size!r}"
        )
    partitioning = PartitioningSettings(size=size)

    # --- transform ---
    tfm = _section(raw, "transform", errors)
    transform = TransformSettings(
        strip_tags=tuple(tfm.get("strip_tags") or ()),
        min_content_chars=_key(tfm, "transform", "min_content_chars", errors, cast=int),
    )

    # --- storage (names from YAML, connections from the environment) ---
    storage = _section(raw, "storage", errors)
    mongo_yaml = storage.get("mongo", {}) if isinstance(storage, dict) else {}
    store_yaml = storage.get("object_store", {}) if isinstance(storage, dict) else {}
    if not isinstance(mongo_yaml, dict):
        errors.append("config file section 'storage.mongo' must be a mapping")
        mongo_yaml = {}
    if not isinstance(store_yaml, dict):
        errors.append("config file section 'storage.object_store' must be a mapping")
        store_yaml = {}

    mongo = MongoSettings(
        uri=_env("MONGO_URI", errors),
        database=_key(mongo_yaml, "storage.mongo", "database", errors, cast=str),
        landing_collection=_key(
            mongo_yaml, "storage.mongo", "landing_collection", errors, cast=str
        ),
        curated_collection=_key(
            mongo_yaml, "storage.mongo", "curated_collection", errors, cast=str
        ),
        state_collection=mongo_yaml.get("state_collection"),
    )
    if mongo.state_collection is not None and (
        not isinstance(mongo.state_collection, str) or not mongo.state_collection.strip()
    ):
        errors.append("storage.mongo.state_collection must be a non-empty string or null")
    if mongo.current_collection in (mongo.landing_collection, mongo.curated_collection):
        errors.append("storage.mongo.state_collection must differ from landing and curated collections")
    if (
        mongo.landing_collection
        and mongo.landing_collection == mongo.curated_collection
    ):
        # The Landing Zone must stay immutable; writing the curated output back
        # into it would violate that silently and irreversibly.
        errors.append(
            "storage.mongo.landing_collection and curated_collection must differ - "
            "the transform must not write back into the immutable Landing Zone"
        )

    object_store = ObjectStoreSettings(
        endpoint_url=_env("MINIO_ENDPOINT_URL", errors),
        access_key=_env("MINIO_ROOT_USER", errors),
        secret_key=_env("MINIO_ROOT_PASSWORD", errors),
        region=os.environ.get("MINIO_REGION", "us-east-1"),
        landing_bucket=_key(
            store_yaml, "storage.object_store", "landing_bucket", errors, cast=str
        ),
        curated_bucket=_key(
            store_yaml, "storage.object_store", "curated_bucket", errors, cast=str
        ),
    )
    if (
        object_store.landing_bucket
        and object_store.landing_bucket == object_store.curated_bucket
    ):
        errors.append(
            "storage.object_store.landing_bucket and curated_bucket must differ - "
            "the transform must not write back into the immutable Landing Zone"
        )

    # --- scraping ---
    scr = _section(raw, "scraping", errors)
    retry_codes = _key(scr, "scraping", "retry_http_codes", errors)
    if retry_codes is not None and not isinstance(retry_codes, list):
        errors.append("config value 'scraping.retry_http_codes' must be a list")
        retry_codes = []

    scraping = ScrapingSettings(
        autothrottle_enabled=_key(scr, "scraping", "autothrottle_enabled", errors, cast=bool),
        autothrottle_start_delay=_key(scr, "scraping", "autothrottle_start_delay", errors, cast=float),
        autothrottle_max_delay=_key(scr, "scraping", "autothrottle_max_delay", errors, cast=float),
        autothrottle_target_concurrency=_key(
            scr, "scraping", "autothrottle_target_concurrency", errors, cast=float
        ),
        download_delay=_key(scr, "scraping", "download_delay", errors, cast=float),
        concurrent_requests=_key(scr, "scraping", "concurrent_requests", errors, cast=int),
        concurrent_requests_per_domain=_key(
            scr, "scraping", "concurrent_requests_per_domain", errors, cast=int
        ),
        download_timeout=_key(scr, "scraping", "download_timeout", errors, cast=int),
        retry_times=_key(scr, "scraping", "retry_times", errors, cast=int),
        retry_http_codes=tuple(int(c) for c in (retry_codes or [])),
        user_agent=_key(scr, "scraping", "user_agent", errors, cast=str),
        httpcache_enabled=_key(scr, "scraping", "httpcache_enabled", errors, cast=bool),
        httpcache_expiration_secs=_key(
            scr, "scraping", "httpcache_expiration_secs", errors, cast=int
        ),
    )

    # --- logging (environment only) ---
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    if level not in valid_levels:
        errors.append(
            f"LOG_LEVEL must be one of {', '.join(valid_levels)}; got {level!r}"
        )
    log_file_raw = os.environ.get("LOG_FILE", "").strip()
    log_settings = LoggingSettings(
        level=level,
        file=Path(log_file_raw).expanduser() if log_file_raw else None,
    )

    if errors:
        raise ConfigError(
            f"Configuration is invalid ({len(errors)} problem"
            f"{'s' if len(errors) > 1 else ''} found).\n"
            f"  config file: {path}\n\n  - "
            + "\n  - ".join(errors)
            + "\n\nIf this is a fresh clone, copy .env.example to .env first."
        )

    return Settings(
        source=source,
        bodies=bodies,
        partitioning=partitioning,
        transform=transform,
        mongo=mongo,
        object_store=object_store,
        scraping=scraping,
        logging=log_settings,
        config_file=path,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, loaded once.

    Cached because configuration cannot change during a run and re-reading the
    file from a Scrapy item pipeline that fires once per record would be
    needless I/O. Tests call ``load_settings`` directly, or
    ``get_settings.cache_clear()``, to bypass the cache.
    """
    return load_settings()
