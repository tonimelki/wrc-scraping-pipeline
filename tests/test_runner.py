"""Tests for the crawl subprocess launcher.

No network and no Scrapy: what matters here is the contract around the
subprocess - that dates are passed in the format the CLI expects, that a
crashed child becomes a clear exception rather than a silent empty result, and
that a hung crawl is killed rather than stalling an orchestrator task forever.

The subprocess itself is stubbed. Whether Scrapy can crawl is tested elsewhere;
whether *this* code notices when it did not is the question here.
"""

from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path

import pytest

from wrc_pipeline.scraper import runner
from wrc_pipeline.scraper.runner import CrawlFailedError, run_crawl


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def spy(monkeypatch):
    """Capture the command and environment, and write a stats file."""
    captured: dict = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs.get("env", {})
        captured["timeout"] = kwargs.get("timeout")
        # The CLI writes its stats to the path given after --stats-json.
        stats_path = Path(command[command.index("--stats-json") + 1])
        stats_path.write_text(
            json.dumps({"found": 10, "stored": 10, "reconciles": True}),
            encoding="utf-8",
        )
        return FakeCompleted()

    monkeypatch.setattr(subprocess, "run", fake_run)
    return captured


def test_dates_are_passed_in_iso_form(spy):
    """The CLI parses ISO strictly; anything else is rejected there."""
    run_crawl(date(2024, 1, 1), date(2024, 3, 31))

    assert "2024-01-01" in spy["command"]
    assert "2024-03-31" in spy["command"]


def test_strings_are_accepted_unchanged(spy):
    run_crawl("2024-01-01", "2024-03-31")
    assert "2024-01-01" in spy["command"]


def test_optional_arguments_are_only_passed_when_given(spy):
    run_crawl("2024-01-01", "2024-01-31")
    assert "--bodies" not in spy["command"]
    assert "--size" not in spy["command"]

    run_crawl("2024-01-01", "2024-01-31", bodies="labour_court", size="weekly")
    assert spy["command"][spy["command"].index("--bodies") + 1] == "labour_court"
    assert spy["command"][spy["command"].index("--size") + 1] == "weekly"


def test_a_config_file_reaches_the_child_process(spy):
    """The child loads its own settings, so this is how a profile is chosen."""
    run_crawl("2024-01-01", "2024-01-31", config_file="/tmp/other.yaml")
    assert spy["env"]["WRC_CONFIG_FILE"] == "/tmp/other.yaml"


def test_the_stats_are_returned(spy):
    stats = run_crawl("2024-01-01", "2024-01-31")
    assert stats["found"] == 10
    assert stats["reconciles"] is True


def test_a_crawl_with_failures_still_returns_normally(monkeypatch):
    """Whether some failed records should fail the task is the caller's policy.

    The orchestrator decides that; this function only reports.
    """

    def fake_run(command, **kwargs):
        path = Path(command[command.index("--stats-json") + 1])
        path.write_text(
            json.dumps({"found": 10, "stored": 8, "failed": 2, "reconciles": True}),
            encoding="utf-8",
        )
        return FakeCompleted(returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    stats = run_crawl("2024-01-01", "2024-01-31")
    assert stats["failed"] == 2


def test_a_child_that_produces_no_stats_raises(monkeypatch):
    """A crashed crawl must not look like a crawl that found nothing."""

    def fake_run(command, **kwargs):
        return FakeCompleted(returncode=1, stderr="Traceback: it exploded")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(CrawlFailedError, match="produced no summary"):
        run_crawl("2024-01-01", "2024-01-31")


def test_the_childs_output_is_included_in_the_error(monkeypatch):
    """The child's own logs are the useful diagnostic, not the exit code."""

    def fake_run(command, **kwargs):
        return FakeCompleted(returncode=1, stderr="ConfigError: MONGO_URI is required")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(CrawlFailedError, match="MONGO_URI"):
        run_crawl("2024-01-01", "2024-01-31")


def test_a_hung_crawl_is_killed(monkeypatch):
    """A stalled orchestrator task is worse than a failed one."""

    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs.get("timeout", 0))

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(CrawlFailedError, match="exceeded"):
        run_crawl("2024-01-01", "2024-01-31", timeout=5)


def test_the_timeout_is_passed_to_the_subprocess(spy):
    run_crawl("2024-01-01", "2024-01-31", timeout=42)
    assert spy["timeout"] == 42


def test_it_invokes_the_real_cli_script(spy):
    """Guards against the path breaking if the package is restructured."""
    run_crawl("2024-01-01", "2024-01-31")

    script = Path(spy["command"][1])
    assert script.name == "run_spider.py"
    assert script.exists(), f"{script} does not exist - REPO_ROOT is wrong"


def test_repo_root_resolves_to_the_project_directory():
    """runner.py is four levels deep; an off-by-one here breaks every crawl."""
    assert (runner.REPO_ROOT / "pyproject.toml").exists()
    assert (runner.REPO_ROOT / "scripts" / "run_spider.py").exists()
