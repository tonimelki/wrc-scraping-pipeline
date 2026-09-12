"""MongoDB capture history and current-record lookup.

Landing writes insert immutable metadata snapshots, deduplicated by a digest
of content and metadata. A separate configurable state collection tracks the
latest snapshot and first/last observation per detail URL. Curated metadata
is mutable and rebuildable. Existing URL-keyed landing records remain readable
without migration or modification.

find_by_id/find_by_range/count on the landing collection expose the current
logical corpus; querying the landing collection directly exposes its history.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timezone
from enum import Enum
from typing import Any, Iterator, Mapping

from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

from wrc_pipeline.config import MongoSettings, Settings, get_settings
from wrc_pipeline.logging_setup import get_logger

logger = get_logger(__name__)


class MetadataStoreError(RuntimeError):
    """A metadata store operation failed.

    Wraps pymongo's exceptions so callers depend on this module's contract
    rather than on the driver's.
    """


class UpsertResult(str, Enum):
    """What an upsert actually did.

    Three outcomes, not two, because "we wrote a record" and "the record was
    already correct" are the difference between a working pipeline and a
    working *idempotent* pipeline. The run summary reports them separately.
    """

    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class MetadataStore:
    """Read and write decision metadata."""

    def __init__(self, settings: MongoSettings) -> None:
        self.settings = settings
        # serverSelectionTimeoutMS keeps a wrong host or a stopped container
        # from hanging for the 30-second default before saying anything useful.
        self._client: MongoClient = MongoClient(
            settings.uri, serverSelectionTimeoutMS=5000, tz_aware=True
        )
        self._db = self._client[settings.database]

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> MetadataStore:
        return cls((settings or get_settings()).mongo)

    # Context-manager support so scripts and the transform job cannot leak
    # connections when something raises mid-run.
    def __enter__(self) -> MetadataStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def ensure_indexes(self, collection: str) -> list[str]:
        """Create the indexes the pipeline queries by. Idempotent.

        Without these, the transform's date-range query is a full collection
        scan. At the exercise's evaluation size that is unnoticeable; at the
        "1000x that" it asks us to design for, it is the difference between a
        job that finishes and one that does not.
        """
        if collection == self.settings.landing_collection:
            self.ensure_indexes(self.settings.current_collection)
        try:
            return self._db[collection].create_indexes(
                [
                    # The transform fetches "everything in this date range".
                    _index([("partition_date", ASCENDING)], "idx_partition_date"),
                    _index([("published_date", ASCENDING)], "idx_published_date"),
                    # Not unique - the site reuses reference numbers. Indexed
                    # because looking a decision up by its reference is the
                    # first thing any human wants to do.
                    _index([("identifier", ASCENDING)], "idx_identifier"),
                    # Re-running one partition for one body is the natural unit
                    # of retry, so it gets a compound index.
                    _index(
                        [("partition_date", ASCENDING), ("body", ASCENDING)],
                        "idx_partition_body",
                    ),
                    _index([("file_hash", ASCENDING)], "idx_file_hash"),
                    # The transform's collision backstop looks a curated key up
                    # by file_key before every write (see
                    # transform.job._guard_key_collision). Without this index
                    # that check is a collection scan, so the cost of writing
                    # one document grows with the size of the corpus - the
                    # worst scaling behaviour in the pipeline, and precisely
                    # what the "1000x" note above exists to prevent.
                    #
                    # Deliberately NOT unique. A unique index would make the
                    # no-overwrite guarantee structural rather than advisory,
                    # which is tempting - but this method serves both zones,
                    # and only the *curated* key is unique by construction.
                    # Nothing stops two landing records from sharing one
                    # attachment: two decisions published as a single combined
                    # PDF would collide, and a unique index would turn that
                    # into a hard ingestion failure instead of a stored record.
                    _index([("file_key", ASCENDING)], "idx_file_key"),
                ]
            )
        except PyMongoError as exc:
            raise MetadataStoreError(
                f"could not create indexes on {collection!r}: {exc}"
            ) from exc

    def ping(self) -> bool:
        """True if the database is reachable."""
        try:
            self._client.admin.command("ping")
            return True
        except PyMongoError:
            return False

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def upsert_metadata(
        self, collection: str, document: Mapping[str, Any], *, _first_seen: datetime | None = None
    ) -> UpsertResult:
        """Insert or update one record, keyed on ``detail_url``.

        Returns which of the three things happened. ``UNCHANGED`` means the
        stored document already matched - the case a second run over the same
        range should produce for every record, and therefore the thing worth
        asserting when demonstrating idempotency.

        Raises:
            MetadataStoreError: on failure, or if the document has no
                ``detail_url`` to key on.
        """
        if collection == self.settings.landing_collection:
            return self._capture_landing(document)
        document = dict(document)
        key = document.get("detail_url")
        if not key:
            raise MetadataStoreError(
                "document has no 'detail_url' to key on. detail_url is the "
                "record identity - see this module's docstring on why it is not "
                "'identifier'."
            )

        document["_id"] = key
        now = datetime.now(timezone.utc)

        # Dates are stored as datetimes: BSON has no date-only type, and letting
        # pymongo guess produces documents that compare inconsistently against
        # range queries. Normalising here means one representation everywhere.
        for field in ("partition_date", "published_date"):
            if isinstance(document.get(field), date):
                document[field] = _to_datetime(document[field])

        # Compare against what is stored *before* writing, so "unchanged" is a
        # real observation rather than an inference from a driver return value.
        existing = self.find_by_id(collection, key)
        if existing is not None and _same_content(existing, document):
            try:
                # Touch last_seen_at only. Records that this run confirmed still
                # exist are worth distinguishing from ones it never looked at,
                # and this write does not alter any scraped content.
                self._db[collection].update_one(
                    {"_id": key}, {"$set": {"last_seen_at": now}}
                )
            except PyMongoError as exc:
                raise MetadataStoreError(f"could not touch {key!r}: {exc}") from exc
            return UpsertResult.UNCHANGED

        update = {
            "$set": {
                **{
                    field: value
                    for field, value in document.items()
                    if field not in _STORE_MANAGED_FIELDS
                },
                "last_seen_at": now,
            },
            # Written on insert and never again: the Landing Zone records when
            # it first saw a document, and that fact is not revised.
            "$setOnInsert": {"first_seen_at": _first_seen or now},
        }
        removed = set(existing or {}) - set(document) - _VOLATILE_FIELDS
        if removed:
            update["$unset"] = dict.fromkeys(removed, "")
        try:
            result = self._db[collection].update_one(
                {"_id": key}, update, upsert=True
            )
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not upsert {key!r}: {exc}") from exc

        return UpsertResult.INSERTED if result.upserted_id else UpsertResult.UPDATED

    def _capture_landing(self, document: Mapping[str, Any]) -> UpsertResult:
        """Insert an immutable snapshot before updating the separate current index.

        Snapshot IDs describe content and metadata, excluding run bookkeeping.
        A failed index write can be retried safely; A -> B -> A reuses capture A
        while moving the current index back to it. Legacy URL-keyed captures
        are left in place and remain readable until revisited.
        """
        incoming = {k: v for k, v in document.items()
                    if k not in _STORE_MANAGED_FIELDS and k != "landing_snapshot_id"}
        key = incoming.get("detail_url")
        if not key:
            raise MetadataStoreError("document has no 'detail_url' to key on")
        for field in ("partition_date", "published_date"):
            if isinstance(incoming.get(field), date):
                incoming[field] = _to_datetime(incoming[field])
        existing = self.find_by_id(self.settings.landing_collection, key)
        canonical = {k: v for k, v in incoming.items() if k not in _VOLATILE_FIELDS}
        digest = hashlib.sha256(json.dumps(
            canonical, sort_keys=True, ensure_ascii=True, default=str,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        snapshot_id = f"{key}#{digest}"
        now = datetime.now(timezone.utc)
        snapshot = {**incoming, "_id": snapshot_id, "first_seen_at": now}
        try:
            self._db[self.settings.landing_collection].update_one(
                {"_id": snapshot_id}, {"$setOnInsert": snapshot}, upsert=True
            )
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not capture {key!r}: {exc}") from exc
        self.upsert_metadata(
            self.settings.current_collection,
            {**incoming, "landing_snapshot_id": snapshot_id},
            _first_seen=(existing or {}).get("first_seen_at"),
        )
        if existing is None:
            return UpsertResult.INSERTED
        return UpsertResult.UNCHANGED if _same_content(existing, incoming) else UpsertResult.UPDATED

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def find_by_id(self, collection: str, detail_url: str) -> dict[str, Any] | None:
        """One record by its identity, or None."""
        try:
            if collection == self.settings.landing_collection:
                current = self._db[self.settings.current_collection].find_one({"_id": detail_url})
                if current is not None:
                    return current
            return self._db[collection].find_one({"_id": detail_url})
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not read {detail_url!r}: {exc}") from exc

    def find_one_by(
        self, collection: str, query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """First record matching an arbitrary query, or None.

        Used by the transform to ask "does another document already own this
        curated filename?" - a question that is not answerable by _id, since
        the whole point is that a *different* record might hold the key.
        """
        try:
            return self._db[collection].find_one(dict(query))
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not query {collection!r}: {exc}") from exc

    def get_stored_hash(self, collection: str, detail_url: str) -> str | None:
        """The stored ``file_hash`` for a record, or None if absent.

        The deduplication step's single question, asked without pulling the
        whole document back: has this document's content changed since we last
        stored it?
        """
        if collection == self.settings.landing_collection:
            return (self.find_by_id(collection, detail_url) or {}).get("file_hash")
        try:
            found = self._db[collection].find_one(
                {"_id": detail_url}, {"file_hash": 1}
            )
        except PyMongoError as exc:
            raise MetadataStoreError(
                f"could not read hash for {detail_url!r}: {exc}"
            ) from exc
        return (found or {}).get("file_hash")

    def find_by_range(
        self,
        collection: str,
        start_date: date | datetime,
        end_date: date | datetime,
        *,
        body: str | None = None,
        field: str = "partition_date",
    ) -> Iterator[dict[str, Any]]:
        """Yield records whose ``field`` falls in the inclusive date range.

        The transform job's entry point: "given a start date and an end date,
        fetch metadata from mongo". Inclusive at both ends, matching how
        partitions and the site's own filters behave, so the same dates mean the
        same thing everywhere in the pipeline.

        A cursor rather than a list - at 1000x scale the result set should not
        have to fit in memory before the first document can be processed.
        """
        query: dict[str, Any] = {
            field: {
                "$gte": _to_datetime(start_date),
                # End of the last day, not its midnight, or every record
                # published on end_date would be excluded.
                "$lte": _to_datetime(end_date, end_of_day=True),
            }
        }
        if body:
            query["body"] = body

        try:
            if collection == self.settings.landing_collection:
                yield from self._current_records(query, field)
                return
            # Sorted so a run's output order is reproducible, which makes two
            # runs' logs diffable.
            yield from self._db[collection].find(query).sort(
                [(field, ASCENDING), ("_id", ASCENDING)]
            )
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not query {collection!r}: {exc}") from exc

    def count(self, collection: str, query: Mapping[str, Any] | None = None) -> int:
        """How many records match."""
        try:
            if collection == self.settings.landing_collection:
                return sum(1 for _ in self._current_records(dict(query or {})))
            return self._db[collection].count_documents(dict(query or {}))
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not count {collection!r}: {exc}") from exc

    def _current_records(self, query: dict[str, Any], field: str = "_id") -> Iterator[dict[str, Any]]:
        """Stream current observations, plus untouched pre-versioning records.

        New snapshots are never mixed into this logical one-record-per-URL view.
        The legacy fallback is read-only; an existing current record wins even
        if its amended publication date has moved outside the requested range.
        """
        state = self._db[self.settings.current_collection]
        yield from state.find(query).sort([(field, ASCENDING)])
        legacy_query = {"$and": [query, {"$expr": {"$eq": ["$_id", "$detail_url"]}}]}
        for record in self._db[self.settings.landing_collection].find(legacy_query).sort([(field, ASCENDING)]):
            if state.find_one({"_id": record["_id"]}) is None:
                yield record

    def delete_by_id(self, collection: str, detail_url: str) -> bool:
        """Delete one record. Returns True if something was removed.

        Only test helpers use this method. The pipeline never deletes captures;
        the idempotency check's --fresh option uses isolated storage instead.
        """
        try:
            return self._db[collection].delete_one({"_id": detail_url}).deleted_count > 0
        except PyMongoError as exc:
            raise MetadataStoreError(f"could not delete {detail_url!r}: {exc}") from exc

    def drop_collection(self, collection: str) -> None:
        """Drop a collection. Tests and scripts only - never the Landing Zone."""
        self._db[collection].drop()
        if collection == self.settings.landing_collection:
            self._db[self.settings.current_collection].drop()


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

# Fields that change on every run without the document's content changing.
# Excluded from the comparison, or nothing would ever be "unchanged" and the
# idempotency check would be meaningless.
_VOLATILE_FIELDS = frozenset(
    {
        "_id",
        # Ingestion bookkeeping.
        "run_id",
        "scraped_at",
        # Transformation bookkeeping. Without these, a curated record compares
        # different on every run - the transform reports rewriting all 895
        # documents while actually rewriting none, which is a worse failure
        # than it sounds: the counters stop being evidence of anything.
        "transform_run_id",
        "transformed_at",
        # This store's own.
        "first_seen_at",
        "last_seen_at",
        "landing_snapshot_id",
    }
)

# Fields this store maintains itself. A caller supplying one of these is
# silently ignored rather than obeyed.
#
# Not a stylistic preference: `first_seen_at` is written with `$setOnInsert`,
# and MongoDB refuses an update that touches the same path in two operators
# ("Updating the path 'first_seen_at' would create a conflict"). The transform
# hit exactly that, because a curated record is built by copying the landing
# record - bookkeeping fields included. Stripping them here fixes the whole
# class of problem at the one place that can, rather than requiring every
# future caller to remember.
_STORE_MANAGED_FIELDS = frozenset({"_id", "first_seen_at", "last_seen_at"})


def _same_content(stored: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    """True if two documents agree on everything that is not run bookkeeping."""
    keys = (set(stored) | set(incoming)) - _VOLATILE_FIELDS
    return all(stored.get(k) == incoming.get(k) for k in keys)


def _to_datetime(value: date | datetime, *, end_of_day: bool = False) -> datetime:
    """Normalise a date or datetime to a timezone-aware UTC datetime.

    BSON stores datetimes, not dates. Doing the conversion in one place stops
    "date at midnight" and "datetime with a time" from coexisting in the same
    field and comparing unequal in range queries.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    moment = time.max if end_of_day else time.min
    return datetime.combine(value, moment, tzinfo=timezone.utc)


def _index(keys: list[tuple[str, int]], name: str):
    """Build a named IndexModel. Named explicitly so re-runs are no-ops."""
    from pymongo import IndexModel

    return IndexModel(keys, name=name)
