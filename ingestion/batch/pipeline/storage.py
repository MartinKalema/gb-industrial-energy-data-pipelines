"""Immutable object-store adapters for Cloudflare R2 and local tests."""

from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

from .models import ImmutableObjectConflict, PipelineError, utc_text


@dataclass(frozen=True)
class ObjectInfo:
    bucket: str
    key: str
    uri: str
    sha256: str
    content_length: int
    content_type: str
    last_modified_utc: str
    disposition: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ObjectStore(Protocol):
    def put_immutable(
        self,
        *,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo: ...

    def get_bytes(self, *, bucket: str, key: str) -> bytes: ...


def content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class R2ObjectStore:
    """Cloudflare R2 S3 adapter using conditional immutable writes."""

    def __init__(self, client: Any):
        self.client = client

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "R2ObjectStore":
        environment = environment or os.environ
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - exercised in runtime image
            raise PipelineError("boto3 is required for R2 object storage") from exc
        required = {
            name: environment.get(name, "").strip()
            for name in (
                "R2_ENDPOINT",
                "R2_ACCESS_KEY_ID",
                "R2_SECRET_ACCESS_KEY",
            )
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise PipelineError(
                "missing R2 client environment: " + ", ".join(missing)
            )
        client = boto3.client(
            "s3",
            endpoint_url=required["R2_ENDPOINT"],
            aws_access_key_id=required["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=required["R2_SECRET_ACCESS_KEY"],
            region_name=environment.get("R2_REGION", "auto"),
        )
        return cls(client)

    def put_immutable(
        self,
        *,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        try:
            from botocore.exceptions import ClientError
        except ImportError as exc:  # pragma: no cover - exercised in runtime image
            raise PipelineError("botocore is required for R2 object storage") from exc

        digest = content_sha256(content)
        object_metadata = {
            "sha256": digest,
            **{str(key): str(value) for key, value in (metadata or {}).items()},
        }
        disposition = "created"
        head: Mapping[str, Any] | None = None
        try:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                Metadata=object_metadata,
                IfNoneMatch="*",
            )
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code")
            if status != 412 and code not in {"PreconditionFailed", "412"}:
                raise PipelineError(f"R2 immutable write failed for r2://{bucket}/{key}") from exc
            try:
                head = self.client.head_object(Bucket=bucket, Key=key)
                existing_length = int(head.get("ContentLength", -1))
                existing_type = str(head.get("ContentType", ""))
            except Exception as read_exc:
                raise PipelineError(
                    f"R2 replay verification failed for r2://{bucket}/{key}"
                ) from read_exc
            if existing_length != len(content) or existing_type != content_type:
                raise ImmutableObjectConflict(
                    f"immutable object r2://{bucket}/{key} already has different content"
                ) from exc
            try:
                existing_content = self.client.get_object(Bucket=bucket, Key=key)[
                    "Body"
                ].read()
            except Exception as read_exc:
                raise PipelineError(
                    f"R2 replay verification failed for r2://{bucket}/{key}"
                ) from read_exc
            if existing_content != content:
                raise ImmutableObjectConflict(
                    f"immutable object r2://{bucket}/{key} already has different content"
                ) from exc
            disposition = "reused"
        try:
            head = head or self.client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise PipelineError(f"R2 metadata read failed for r2://{bucket}/{key}") from exc
        last_modified = head.get("LastModified")
        if not isinstance(last_modified, datetime) or last_modified.tzinfo is None:
            raise PipelineError(f"R2 returned no last-modified time for r2://{bucket}/{key}")
        if (
            int(head.get("ContentLength", -1)) != len(content)
            or str(head.get("ContentType", "")) != content_type
        ):
            raise PipelineError(f"R2 metadata mismatch for r2://{bucket}/{key}")
        return ObjectInfo(
            bucket=bucket,
            key=key,
            uri=f"r2://{bucket}/{key}",
            sha256=digest,
            content_length=len(content),
            content_type=content_type,
            last_modified_utc=utc_text(last_modified),
            disposition=disposition,
        )

    def get_bytes(self, *, bucket: str, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except Exception as exc:  # boto3 exposes several transport/client errors
            raise PipelineError(f"R2 read failed for r2://{bucket}/{key}") from exc
