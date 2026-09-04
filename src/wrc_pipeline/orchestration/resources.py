"""MongoDB and object storage as Dagster resources.

Dagster resources exist so that an asset says *what* it needs rather than *how*
to build it: the asset asks for a metadata store, and Dagster supplies one -
the real thing in production, something else in a test - without the asset
knowing the difference.

The pipeline already loads its own configuration from ``settings.yaml`` plus the
environment, and that must not be duplicated here. Re-declaring connection
strings as Dagster config would give the project two sources of truth for the
same values, and they would drift. So these resources are thin: they carry an
optional ``config_file``, and everything else comes from the same loader the
CLI uses. Pointing Dagster at a different profile is then the same one-line
change as pointing the CLI at one.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from dagster import ConfigurableResource

from wrc_pipeline.config import Settings, load_settings
from wrc_pipeline.storage.mongo import MetadataStore
from wrc_pipeline.storage.object_store import ObjectStore


class PipelineSettingsResource(ConfigurableResource):
    """The project's own settings object, as a Dagster resource.

    Attributes:
        config_file: Optional path to a settings YAML. Left unset, the loader's
            usual precedence applies (``WRC_CONFIG_FILE``, then
            ``config/settings.yaml``), so Dagster and the CLI see exactly the
            same configuration by default.
    """

    config_file: str | None = None

    def load(self) -> Settings:
        """Load and validate the settings.

        Not cached: a Dagster run is a fresh process, and re-reading a small
        YAML file once per asset is cheaper than reasoning about when a cache
        would need invalidating.
        """
        return load_settings(self.config_file)


class MongoResource(ConfigurableResource):
    """Access to the metadata store.

    Exposed as a context manager rather than a bare client so the connection is
    closed when the asset finishes. A long-lived Dagster daemon materialising
    hundreds of partitions would otherwise leak a client per run.
    """

    config_file: str | None = None

    @contextmanager
    def store(self) -> Iterator[MetadataStore]:
        settings = load_settings(self.config_file)
        with MetadataStore.from_settings(settings) as store:
            yield store


class ObjectStoreResource(ConfigurableResource):
    """Access to S3-compatible object storage.

    No context manager: boto3 clients hold no connection that needs closing,
    and pretending otherwise would be ceremony.
    """

    config_file: str | None = None

    def client(self) -> ObjectStore:
        return ObjectStore.from_settings(load_settings(self.config_file))
