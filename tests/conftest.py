"""Shared test fixtures.

Most of the suite is pure and runs in milliseconds with nothing else running.
The storage modules cannot be: their whole job is talking to MongoDB and MinIO,
and a test that mocks both would only prove the mocks agree with themselves.

So those tests are marked ``integration`` and **skip** - rather than fail - when
the containers are not up. `pytest` therefore stays green on a fresh clone with
no Docker, while `pytest -m integration` exercises the real thing. The skip
message says how to turn them on, so a reviewer is never left wondering whether
something silently did not run.
"""

from __future__ import annotations

import os

import pytest

from wrc_pipeline.config import ConfigError, Settings, load_settings

_CACHED: dict[str, object] = {}


def _services_available() -> tuple[bool, str]:
    """Are Mongo and MinIO both reachable? Checked once per session."""
    if "result" in _CACHED:
        return _CACHED["result"]  # type: ignore[return-value]

    try:
        settings = load_settings()
    except ConfigError as exc:
        result = (False, f"configuration is not loadable: {exc}")
        _CACHED["result"] = result
        return result

    from wrc_pipeline.storage.mongo import MetadataStore
    from wrc_pipeline.storage.object_store import ObjectStore

    try:
        with MetadataStore.from_settings(settings) as store:
            if not store.ping():
                raise RuntimeError("MongoDB did not respond to ping")
        if not ObjectStore.from_settings(settings).ping():
            raise RuntimeError("MinIO did not respond")
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        result = (False, f"{type(exc).__name__}: {exc}")
        _CACHED["result"] = result
        return result

    _CACHED["result"] = (True, "")
    _CACHED["settings"] = settings
    return True, ""


@pytest.fixture(scope="session")
def live_settings() -> Settings:
    """Real settings, with the containers confirmed up. Skips otherwise."""
    available, reason = _services_available()
    if not available:
        if os.environ.get("WRC_REQUIRE_INTEGRATION") == "1":
            pytest.fail(f"integration storage is required but unavailable: {reason}")
        pytest.skip(
            f"storage containers are not available ({reason}). "
            f"Start them with: docker compose up -d"
        )
    return _CACHED["settings"]  # type: ignore[return-value]
