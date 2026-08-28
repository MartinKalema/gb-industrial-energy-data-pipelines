from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

import pytest
from botocore.exceptions import ClientError

from ingestion.batch.pipeline.models import ImmutableObjectConflict
from ingestion.batch.pipeline.storage import R2ObjectStore, content_sha256


class MemoryS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], dict] = {}

    def put_object(self, **request):
        identity = (request["Bucket"], request["Key"])
        if identity in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[identity] = {
            "content": bytes(request["Body"]),
            "content_type": request["ContentType"],
            "metadata": dict(request["Metadata"]),
            "last_modified": datetime(2026, 8, 28, 1, 0, tzinfo=timezone.utc),
        }

    def head_object(self, *, Bucket, Key):
        value = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(value["content"]),
            "ContentType": value["content_type"],
            "Metadata": value["metadata"],
            "LastModified": value["last_modified"],
        }

    def get_object(self, *, Bucket, Key):
        return {"Body": BytesIO(self.objects[(Bucket, Key)]["content"])}


def _put(store: R2ObjectStore, content: bytes, content_type: str = "text/plain"):
    return store.put_immutable(
        bucket="raw-test",
        key="raw/evidence.txt",
        content=content,
        content_type=content_type,
    )


def test_r2_replay_downloads_and_verifies_identical_bytes() -> None:
    client = MemoryS3Client()
    store = R2ObjectStore(client)

    created = _put(store, b"evidence")
    replayed = _put(store, b"evidence")

    assert created.disposition == "created"
    assert replayed.disposition == "reused"
    assert replayed.last_modified_utc == created.last_modified_utc
    assert replayed.sha256 == content_sha256(b"evidence")


@pytest.mark.parametrize(
    ("existing_content", "existing_type"),
    [
        (b"tampered", "text/plain"),
        (b"evidence", "application/octet-stream"),
    ],
)
def test_r2_replay_rejects_spoofed_metadata_or_content_type(
    existing_content: bytes, existing_type: str
) -> None:
    client = MemoryS3Client()
    client.objects[("raw-test", "raw/evidence.txt")] = {
        "content": existing_content,
        "content_type": existing_type,
        # Deliberately claim the incoming hash even when the bytes differ.
        "metadata": {"sha256": content_sha256(b"evidence")},
        "last_modified": datetime(2026, 8, 28, 1, 0, tzinfo=timezone.utc),
    }

    with pytest.raises(ImmutableObjectConflict, match="different content"):
        _put(R2ObjectStore(client), b"evidence")
