"""Prove the Docker infrastructure actually works before building on top of it.

Step 1 of the build order. This does the smallest possible end-to-end exercise
of both stores:

    Mongo  - insert a document, read it back, upsert the same _id again and
             confirm the collection still holds exactly one record
    MinIO  - create a bucket, put an object, get it back, compare bytes

The Mongo upsert is not padding. Idempotency is the requirement most likely to
be tested first by a reviewer, and it rests entirely on "upsert by a stable
identifier gives one record, not two". Proving that primitive here, against the
real containers, means that when Step 7 fails the cause is the pipeline logic
and not the database.

Deliberately dependency-light and standalone: it imports nothing from
`wrc_pipeline`, so it stays runnable as a plain infrastructure health check
even if the application code is mid-refactor.

    python scripts/check_infra.py
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Read .env if present. The defaults below match docker-compose.yml, so this
# script works from a clean clone with no .env file at all.
load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://wrc:wrc_local_dev_pw@localhost:27017/?authSource=admin",
)
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "wrcadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "wrc_local_dev_pw")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

# Everything this script creates is namespaced and removed on the way out, so
# running it never pollutes the real landing/curated namespaces.
CHECK_DB = "wrc_infra_check"
CHECK_COLLECTION = "roundtrip"
CHECK_BUCKET = "wrc-infra-check"


def _ok(message: str) -> None:
    print(f"  [ok]   {message}")


def _fail(message: str) -> None:
    print(f"  [FAIL] {message}")


def check_mongo() -> bool:
    """Insert, read back, and re-upsert a document. Returns True on success."""
    print("MongoDB")
    print(f"  uri: {MONGO_URI.split('@')[-1]}")  # never print the credentials

    client: MongoClient | None = None
    try:
        # serverSelectionTimeoutMS keeps a wrong host/port from hanging for the
        # 30-second default before reporting anything useful.
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        server_version = client.server_info()["version"]
        _ok(f"connected (server {server_version})")

        collection = client[CHECK_DB][CHECK_COLLECTION]
        collection.delete_many({})  # start from a known-empty state

        # A stable, meaningful _id is the same idempotency key the real
        # pipeline will use: the record's identifier from the source site.
        doc_id = f"CHECK-{uuid.uuid4().hex[:8]}"
        document = {
            "_id": doc_id,
            "checked_at": datetime.now(timezone.utc),
            "note": "infrastructure round-trip check",
        }

        collection.insert_one(document)
        _ok(f"inserted _id={doc_id}")

        fetched = collection.find_one({"_id": doc_id})
        if fetched is None or fetched["note"] != document["note"]:
            _fail("document read back did not match what was written")
            return False
        _ok("read back and matched")

        # The idempotency primitive: same key, upsert, still one record.
        collection.update_one(
            {"_id": doc_id},
            {"$set": {"note": "second write, same identifier"}},
            upsert=True,
        )
        count = collection.count_documents({})
        if count != 1:
            _fail(f"upsert on an existing _id produced {count} documents, expected 1")
            return False
        _ok("re-upserted same _id -> still 1 document (idempotency primitive)")

        client.drop_database(CHECK_DB)
        _ok(f"cleaned up database '{CHECK_DB}'")
        return True

    except PyMongoError as exc:
        _fail(f"{type(exc).__name__}: {exc}")
        print("\n  Is the container up?  docker compose ps")
        return False
    finally:
        if client is not None:
            client.close()


def check_minio() -> bool:
    """Create a bucket, put an object, get it back byte-for-byte."""
    print("\nMinIO (S3 API)")
    print(f"  endpoint: {MINIO_ENDPOINT_URL}")

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
            # MinIO requires SigV4, and path-style addressing: virtual-host
            # style ("bucket.localhost:9000") does not resolve against a local
            # container. Omitting this is the classic first-run failure.
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

        s3.list_buckets()
        _ok("connected")

        try:
            s3.create_bucket(Bucket=CHECK_BUCKET)
            _ok(f"created bucket '{CHECK_BUCKET}'")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                _ok(f"bucket '{CHECK_BUCKET}' already existed")
            else:
                raise

        key = f"roundtrip/{uuid.uuid4().hex[:8]}.txt"
        payload = b"WRC pipeline infrastructure check\n"

        s3.put_object(Bucket=CHECK_BUCKET, Key=key, Body=payload)
        _ok(f"put object '{key}' ({len(payload)} bytes)")

        retrieved = s3.get_object(Bucket=CHECK_BUCKET, Key=key)["Body"].read()
        if retrieved != payload:
            _fail("object read back did not match the bytes written")
            return False
        _ok("read back and matched byte-for-byte")

        s3.delete_object(Bucket=CHECK_BUCKET, Key=key)
        s3.delete_bucket(Bucket=CHECK_BUCKET)
        _ok(f"cleaned up bucket '{CHECK_BUCKET}'")
        return True

    except Exception as exc:  # noqa: BLE001 - a health check reports, never raises
        _fail(f"{type(exc).__name__}: {exc}")
        print("\n  Is the container up?  docker compose ps")
        return False


def main() -> int:
    print("Infrastructure check - Mongo + MinIO round-trip\n")
    mongo_ok = check_mongo()
    minio_ok = check_minio()

    print("\n" + "-" * 52)
    if mongo_ok and minio_ok:
        print("PASS - both stores round-tripped successfully.")
        return 0
    print("FAIL - mongo:", "ok" if mongo_ok else "failed", "| minio:", "ok" if minio_ok else "failed")
    return 1


if __name__ == "__main__":
    # Exit code, not just a printed message, so this can gate a later CI step.
    sys.exit(main())
