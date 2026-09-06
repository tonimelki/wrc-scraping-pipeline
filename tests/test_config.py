"""Tests for configuration loading and validation.

The tests worth having here are the ones covering things that fail *silently*
in production. The date format is the clearest example: get it wrong and the
site returns an empty result set rather than an error, so the pipeline reports
a successful run that scraped nothing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from wrc_pipeline.config import (
    DEFAULT_CONFIG_FILE,
    ConfigError,
    SourceSettings,
    load_settings,
    redact_uri,
)

# A minimal but complete configuration, used as the starting point for each
# test. Tests mutate a copy to introduce exactly one problem, so a failure
# points at one cause.
VALID_CONFIG: dict = {
    "source": {
        "name": "workplace_relations",
        "base_url": "https://www.workplacerelations.ie",
        "search_path": "/en/search/",
        "date_format": "{day}/{month}/{year}",
        "results_per_page": 10,
    },
    "bodies": {
        "equality_tribunal": 1,
        "employment_appeals_tribunal": 2,
        "labour_court": 3,
        "workplace_relations_commission": 15376,
    },
    "partitioning": {"size": "monthly"},
    "transform": {
        "strip_tags": ["script", "style", "noscript"],
        "min_content_chars": 200,
    },
    "storage": {
        "mongo": {
            "database": "wrc",
            "landing_collection": "landing_decisions",
            "curated_collection": "curated_decisions",
        },
        "object_store": {
            "landing_bucket": "wrc-landing",
            "curated_bucket": "wrc-curated",
        },
    },
    "scraping": {
        "autothrottle_enabled": True,
        "autothrottle_start_delay": 1.0,
        "autothrottle_max_delay": 30.0,
        "autothrottle_target_concurrency": 4.0,
        "download_delay": 0.25,
        "concurrent_requests": 16,
        "concurrent_requests_per_domain": 8,
        "download_timeout": 60,
        "retry_times": 3,
        "retry_http_codes": [429, 500, 502, 503, 504],
        "user_agent": "wrc-pipeline/0.1 (test)",
        "httpcache_enabled": False,
        "httpcache_expiration_secs": 86400,
    },
}

REQUIRED_ENV = {
    "MONGO_URI": "mongodb://user:pw@localhost:27017/?authSource=admin",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ROOT_USER": "wrcadmin",
    "MINIO_ROOT_PASSWORD": "wrc_local_dev_pw",
}


@pytest.fixture
def env(monkeypatch):
    """A clean environment holding exactly the required variables.

    LOG_LEVEL and LOG_FILE are deleted rather than set, so the defaults are
    what gets exercised.
    """
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    for key in ("LOG_LEVEL", "LOG_FILE", "WRC_CONFIG_FILE", "MINIO_REGION"):
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def write_config(tmp_path: Path, overrides: dict | None = None) -> Path:
    """Write VALID_CONFIG to a temp file, deep-merging any overrides."""
    import copy

    data = copy.deepcopy(VALID_CONFIG)
    for section, values in (overrides or {}).items():
        if values is None:
            data.pop(section, None)
        elif isinstance(values, dict) and isinstance(data.get(section), dict):
            for key, value in values.items():
                if value is None:
                    data[section].pop(key, None)
                else:
                    data[section][key] = value
        else:
            data[section] = values

    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# The date format - the highest-value test in this file
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # Single-digit day AND month: the case that breaks the site.
        (date(2025, 1, 5), "5/1/2025"),
        # Single-digit day, two-digit month.
        (date(2025, 10, 1), "1/10/2025"),
        # Two-digit day, single-digit month.
        (date(2026, 9, 29), "29/9/2026"),
        # Both two digits - must be unchanged.
        (date(2024, 12, 31), "31/12/2024"),
        # Leap day, because February is where date bugs live.
        (date(2024, 2, 29), "29/2/2024"),
    ],
)
def test_format_date_never_pads(value: date, expected: str):
    """No leading zeros, ever. Padded dates make the site return nothing."""
    source = SourceSettings(**VALID_CONFIG["source"])
    assert source.format_date(value) == expected


def test_format_date_rejects_strftime_pattern(tmp_path, env):
    """A strftime pattern in date_format must fail loudly at load time.

    Left unchecked, "%-d/%-m/%Y" would pass through str.format untouched and
    be sent to the site literally, producing an empty result set that looks
    exactly like a month with no decisions.
    """
    path = write_config(tmp_path, {"source": {"date_format": "%-d/%-m/%Y"}})
    with pytest.raises(ConfigError, match="date_format"):
        load_settings(path, load_env=False)


def test_search_url_is_absolute(tmp_path, env):
    settings = load_settings(write_config(tmp_path), load_env=False)
    assert settings.source.search_url == "https://www.workplacerelations.ie/en/search/"


# --------------------------------------------------------------------------
# Loading a good configuration
# --------------------------------------------------------------------------


def test_loads_valid_configuration(tmp_path, env):
    settings = load_settings(write_config(tmp_path), load_env=False)

    assert settings.source.results_per_page == 10
    assert settings.partitioning.size == "monthly"
    assert settings.bodies["workplace_relations_commission"] == 15376
    assert len(settings.bodies) == 4

    # Connections come from the environment, names from the YAML.
    assert settings.mongo.uri == REQUIRED_ENV["MONGO_URI"]
    assert settings.mongo.database == "wrc"
    assert settings.object_store.endpoint_url == "http://localhost:9000"
    assert settings.object_store.landing_bucket == "wrc-landing"

    # Types are coerced, not left as whatever YAML guessed.
    assert isinstance(settings.scraping.retry_http_codes, tuple)
    assert settings.scraping.retry_http_codes == (429, 500, 502, 503, 504)

    # Defaults applied where the environment says nothing.
    assert settings.logging.level == "INFO"
    assert settings.logging.file is None


def test_body_id_lookup_lists_options_on_typo(tmp_path, env):
    settings = load_settings(write_config(tmp_path), load_env=False)
    assert settings.body_id("labour_court") == 3
    with pytest.raises(ConfigError, match="labour_court"):
        settings.body_id("labour-court")  # hyphen, not underscore


# --------------------------------------------------------------------------
# Failing loudly
# --------------------------------------------------------------------------


def test_missing_env_var_is_reported_by_name(tmp_path, env):
    env.delenv("MONGO_URI")
    with pytest.raises(ConfigError, match="MONGO_URI"):
        load_settings(write_config(tmp_path), load_env=False)


def test_all_problems_reported_at_once(tmp_path, env):
    """Four missing variables should take one run to discover, not four."""
    for key in ("MONGO_URI", "MINIO_ENDPOINT_URL", "MINIO_ROOT_USER"):
        env.delenv(key)
    path = write_config(tmp_path, {"partitioning": {"size": "fortnightly"}})

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, load_env=False)

    message = str(exc_info.value)
    for expected in ("MONGO_URI", "MINIO_ENDPOINT_URL", "MINIO_ROOT_USER", "fortnightly"):
        assert expected in message
    assert "4 problems found" in message


def test_invalid_partition_size_rejected(tmp_path, env):
    path = write_config(tmp_path, {"partitioning": {"size": "fortnightly"}})
    with pytest.raises(ConfigError, match="partitioning.size"):
        load_settings(path, load_env=False)


def test_missing_section_reported(tmp_path, env):
    path = write_config(tmp_path, {"scraping": None})
    with pytest.raises(ConfigError, match="'scraping:' section"):
        load_settings(path, load_env=False)


def test_landing_and_curated_collections_must_differ(tmp_path, env):
    """The Landing Zone is immutable; the transform must not write back into it."""
    path = write_config(
        tmp_path,
        {"storage": {"mongo": {**VALID_CONFIG["storage"]["mongo"], "curated_collection": "landing_decisions"}}},
    )
    with pytest.raises(ConfigError, match="must differ"):
        load_settings(path, load_env=False)


def test_landing_and_curated_buckets_must_differ(tmp_path, env):
    path = write_config(
        tmp_path,
        {"storage": {"object_store": {"landing_bucket": "same", "curated_bucket": "same"}}},
    )
    with pytest.raises(ConfigError, match="must differ"):
        load_settings(path, load_env=False)


def test_missing_config_file_names_the_path(tmp_path, env):
    missing = tmp_path / "nope.yaml"
    with pytest.raises(ConfigError, match="nope.yaml"):
        load_settings(missing, load_env=False)


def test_invalid_yaml_is_reported_as_such(tmp_path, env):
    path = tmp_path / "settings.yaml"
    path.write_text("source:\n  base_url: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_settings(path, load_env=False)


def test_invalid_log_level_rejected(tmp_path, env):
    env.setenv("LOG_LEVEL", "CHATTY")
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        load_settings(write_config(tmp_path), load_env=False)


# --------------------------------------------------------------------------
# Credential handling
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("mongodb://user:secret@host:27017/", "mongodb://user:***@host:27017/"),
        ("mongodb://host:27017/", "mongodb://host:27017/"),
        ("mongodb://user@host:27017/", "mongodb://user@host:27017/"),
    ],
)
def test_redact_uri_removes_password(uri: str, expected: str):
    assert redact_uri(uri) == expected


def test_safe_uri_does_not_leak_the_password(tmp_path, env):
    settings = load_settings(write_config(tmp_path), load_env=False)
    assert "pw" not in settings.mongo.safe_uri
    assert "***" in settings.mongo.safe_uri


# --------------------------------------------------------------------------
# The real repository configuration must itself be valid
# --------------------------------------------------------------------------


def test_shipped_settings_yaml_is_valid(env):
    """Guards against a typo in the committed config/settings.yaml.

    Without this, a broken config file is only discovered by running the whole
    pipeline.
    """
    settings = load_settings(DEFAULT_CONFIG_FILE, load_env=False)
    assert len(settings.bodies) == 4, "the site's Body filter has exactly four bodies"
    assert settings.source.format_date(date(2025, 10, 1)) == "1/10/2025"


def test_transform_settings_are_loaded(tmp_path, env):
    settings = load_settings(write_config(tmp_path), load_env=False)
    assert settings.transform.strip_tags == ("script", "style", "noscript")
    assert settings.transform.min_content_chars == 200


def test_missing_transform_section_is_reported(tmp_path, env):
    path = write_config(tmp_path, {"transform": None})
    with pytest.raises(ConfigError, match="'transform:' section"):
        load_settings(path, load_env=False)


def test_shipped_config_declares_content_selectors(env):
    """The transform needs to know where the decision lives on a page.

    Without a selector it would have to guess, and guessing wrong stores the
    site's navigation furniture as the curated corpus.
    """
    settings = load_settings(DEFAULT_CONFIG_FILE, load_env=False)
    assert settings.source.content_selectors
    assert "div.col-sm-9" in settings.source.content_selectors


# --------------------------------------------------------------------------
# Scrapy settings must come from configuration, not from literals
# --------------------------------------------------------------------------


def test_scrapy_settings_are_derived_from_the_config_file(env):
    """The exercise requires no hardcoded values.

    Scrapy wants a module of UPPERCASE names, which is exactly the shape that
    invites `DOWNLOAD_DELAY = 0.25` written inline. This asserts the module is a
    translation layer over config/settings.yaml rather than a second place where
    the numbers live.
    """
    import importlib

    from wrc_pipeline import config as config_module
    from wrc_pipeline.scraper import settings as scrapy_settings

    config_module.get_settings.cache_clear()
    importlib.reload(scrapy_settings)
    shipped = load_settings(DEFAULT_CONFIG_FILE, load_env=False)

    assert shipped.scraping.download_delay == scrapy_settings.DOWNLOAD_DELAY
    assert shipped.scraping.concurrent_requests == scrapy_settings.CONCURRENT_REQUESTS
    assert (
        shipped.scraping.autothrottle_target_concurrency
        == scrapy_settings.AUTOTHROTTLE_TARGET_CONCURRENCY
    )
    assert shipped.scraping.user_agent == scrapy_settings.USER_AGENT
    assert shipped.scraping.retry_times == scrapy_settings.RETRY_TIMES


def test_robots_is_obeyed_and_scrapys_logging_is_disabled(env):
    """Two settings that are load-bearing rather than incidental.

    ROBOTSTXT_OBEY stays on as a deliberate judgement call (see ARCHITECTURE.md),
    and LOG_ENABLED must stay off or Scrapy installs a plain-text handler and the
    exercise's JSON log requirement quietly stops being met.
    """
    from wrc_pipeline.scraper import settings as scrapy_settings

    assert scrapy_settings.ROBOTSTXT_OBEY is True
    assert scrapy_settings.LOG_ENABLED is False


def test_all_four_pipeline_stages_are_registered(env):
    """A stage missing from ITEM_PIPELINES fails silently - it just never runs."""
    from wrc_pipeline.scraper import settings as scrapy_settings

    stages = [name.rsplit(".", 1)[-1] for name in scrapy_settings.ITEM_PIPELINES]
    assert stages == [
        "ValidationPipeline",
        "DeduplicationPipeline",
        "DocumentStoragePipeline",
        "MetadataWriterPipeline",
    ], "order matters: validate before dedup before store before write"
