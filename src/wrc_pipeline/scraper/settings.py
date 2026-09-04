"""Scrapy settings, derived from the project's own configuration.

Scrapy requires a module of UPPERCASE names, which is a hardcoded-values trap:
the obvious implementation writes ``DOWNLOAD_DELAY = 0.25`` here and quietly
breaks the exercise's "no hardcoded values" requirement. So every tunable below
is read from ``config/settings.yaml`` via ``config.py`` instead, and this module
is a translation layer between our names and Scrapy's.

Loading configuration at import time is deliberate: a bad config becomes a
startup failure with the full list of problems, rather than an exception thrown
from inside the reactor once the crawl is already running.
"""

from __future__ import annotations

from wrc_pipeline.config import get_settings

_settings = get_settings()
_scraping = _settings.scraping

# --------------------------------------------------------------------------
# Project identity
# --------------------------------------------------------------------------

BOT_NAME = "wrc_pipeline"

SPIDER_MODULES = ["wrc_pipeline.scraper.spiders"]
NEWSPIDER_MODULE = "wrc_pipeline.scraper.spiders"

# --------------------------------------------------------------------------
# Politeness
# --------------------------------------------------------------------------

# Left ON, deliberately. robots.txt disallows /Cases/ and /en/Cases/ in
# CAPITALISED form while the live URLs are lowercase (/en/cases/...), and
# RFC 9309 paths are case-sensitive, so the crawl passes. That ambiguity is a
# judgement call rather than a technicality to hide, so it is written up in
# ARCHITECTURE.md rather than silently worked around by disabling this.
ROBOTSTXT_OBEY = True

# Honest identification with a contact route, rather than impersonating a
# browser. Appropriate for a public government publication database, and the
# thing a site owner would want to see in their logs.
USER_AGENT = _scraping.user_agent

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IE,en;q=0.9",
}

# The search endpoint is a plain GET with everything in the query string, so
# there is no session to maintain. Disabling cookies removes per-request
# overhead and, more usefully, removes any chance of one partition's response
# influencing another's.
COOKIES_ENABLED = False

# --------------------------------------------------------------------------
# Throughput
#
# The exercise asks for "the fastest way to scrape the URLs without getting
# blocked" - both halves. A fixed sleep only answers the second, so AutoThrottle
# is the primary control: it measures actual response latency and converges on
# the concurrency the site is comfortable with, which is a number the site tells
# us rather than one guessed in advance. DOWNLOAD_DELAY is a floor beneath it.
# --------------------------------------------------------------------------

CONCURRENT_REQUESTS = _scraping.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _scraping.concurrent_requests_per_domain
DOWNLOAD_DELAY = _scraping.download_delay
DOWNLOAD_TIMEOUT = _scraping.download_timeout

AUTOTHROTTLE_ENABLED = _scraping.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = _scraping.autothrottle_start_delay
AUTOTHROTTLE_MAX_DELAY = _scraping.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = _scraping.autothrottle_target_concurrency
AUTOTHROTTLE_DEBUG = False

# --------------------------------------------------------------------------
# Failure handling
#
# Scrapy's built-in RetryMiddleware, with the codes and count from config. A
# request that exhausts its retries is *not* silently dropped: the spider logs
# it against the partition and body so it lands in the failure count, which is
# what makes "found = scraped + failed" reconcile.
# --------------------------------------------------------------------------

RETRY_ENABLED = True
RETRY_TIMES = _scraping.retry_times
RETRY_HTTP_CODES = list(_scraping.retry_http_codes)

# --------------------------------------------------------------------------
# Development cache
#
# Off by default so a real run never serves stale data. Turned on while
# iterating on selectors, re-running a partition costs the site nothing.
# --------------------------------------------------------------------------

HTTPCACHE_ENABLED = _scraping.httpcache_enabled
HTTPCACHE_EXPIRATION_SECS = _scraping.httpcache_expiration_secs
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# --------------------------------------------------------------------------
# Logging
#
# Scrapy installs its own plain-text handler on the root logger. The exercise
# requires structured JSON, so Scrapy's handler is disabled here and the spider
# installs ours instead (see WrcDecisionsSpider.from_crawler).
#
# This disables the *handler*, not the logging: Scrapy's own loggers still emit
# records, which propagate to the root logger and come out as JSON alongside the
# pipeline's own events. Retries, 404s and the final stats dump are all captured.
# --------------------------------------------------------------------------

LOG_ENABLED = False

# --------------------------------------------------------------------------
# Item pipelines
#
# Four stages, each doing one thing, in the order a record has to pass through
# them. Splitting them this way gives a one-word answer to "where does
# deduplication happen?" and lets each stage be tested on its own.
#
#   validate  - reject records that cannot be stored correctly
#   dedup     - hash the payload, compare with what is stored
#   download  - write the bytes to object storage (skipping unchanged ones)
#   mongo     - upsert the metadata record and count the outcome
#
# The numbers are Scrapy's ordering weights, spaced so a stage can be inserted
# between two others without renumbering everything.
# --------------------------------------------------------------------------

ITEM_PIPELINES = {
    "wrc_pipeline.scraper.pipelines.validate.ValidationPipeline": 100,
    "wrc_pipeline.scraper.pipelines.dedup.DeduplicationPipeline": 200,
    "wrc_pipeline.scraper.pipelines.download.DocumentStoragePipeline": 300,
    "wrc_pipeline.scraper.pipelines.mongo_writer.MetadataWriterPipeline": 400,
}

FEED_EXPORT_ENCODING = "utf-8"

# Nothing here needs a debugging telnet endpoint, and not binding a port keeps
# the crawl safe to run in a container without extra thought.
TELNETCONSOLE_ENABLED = False
