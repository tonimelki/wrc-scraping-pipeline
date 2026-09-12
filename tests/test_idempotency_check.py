"""A fresh proof run must not clear the user's Landing Zone."""

from scripts.check_idempotency import isolated_profile

from tests.test_detail_branch import REQUIRED_ENV
from wrc_pipeline.config import DEFAULT_CONFIG_FILE, load_settings


def test_fresh_profiles_are_isolated_and_leave_original_configuration_unchanged(tmp_path, monkeypatch):
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    original = load_settings(DEFAULT_CONFIG_FILE, load_env=False)
    original_bytes = DEFAULT_CONFIG_FILE.read_bytes()
    first = isolated_profile(original, tmp_path)
    second = isolated_profile(original, tmp_path)
    assert first.mongo.landing_collection != second.mongo.landing_collection
    assert first.mongo.current_collection != original.mongo.current_collection
    assert first.mongo.curated_collection != original.mongo.curated_collection
    assert first.object_store.landing_bucket != original.object_store.landing_bucket
    assert first.object_store.curated_bucket != original.object_store.curated_bucket
    assert first.mongo.uri == original.mongo.uri
    assert first.source == original.source
    assert DEFAULT_CONFIG_FILE.read_bytes() == original_bytes
