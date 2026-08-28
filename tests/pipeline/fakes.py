"""Test doubles for pipeline boundaries; never imported by runtime code."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from ingestion.batch.pipeline.models import (
    ImmutableObjectConflict,
    PipelineError,
    utc_text,
)
from ingestion.batch.pipeline.storage import ObjectInfo, content_sha256


class FakeObjectStore:
    """Filesystem-backed R2 test double with immutable-write semantics."""

    def __init__(self, root: Path):
        self.root = root.resolve()

    def _path(self, bucket: str, key: str) -> Path:
        relative = Path(key)
        if not bucket or "/" in bucket or relative.is_absolute() or ".." in relative.parts:
            raise PipelineError("invalid fake object-store location")
        return self.root / bucket / relative

    def put_immutable(
        self,
        *,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        del metadata
        path = self._path(bucket, key)
        digest = content_sha256(content)
        if path.exists():
            if path.read_bytes() != content:
                raise ImmutableObjectConflict(
                    f"immutable test object fake://{bucket}/{key} already has different content"
                )
            disposition = "reused"
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            disposition = "created"
        return ObjectInfo(
            bucket=bucket,
            key=key,
            uri=f"r2://{bucket}/{key}",
            sha256=digest,
            content_length=len(content),
            content_type=content_type,
            last_modified_utc=utc_text(
                datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            ),
            disposition=disposition,
        )

    def get_bytes(self, *, bucket: str, key: str) -> bytes:
        path = self._path(bucket, key)
        if not path.is_file():
            raise PipelineError(f"fake object r2://{bucket}/{key} does not exist")
        return path.read_bytes()
