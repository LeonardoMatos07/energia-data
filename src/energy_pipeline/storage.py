from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import boto3
import polars as pl
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class Lake:
    def __init__(self, bucket: str, endpoint_url: str | None = None, client: Any = None) -> None:
        self.bucket = bucket
        if client is None:
            config = Config(s3={"addressing_style": "path"}) if endpoint_url else None
            client = boto3.client("s3", endpoint_url=endpoint_url, config=config)
        self._s3 = client

    def ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                raise
            log.info("Creating bucket %s", self.bucket)
            self._s3.create_bucket(Bucket=self.bucket)

    def upload_file(self, path: Path, key: str, metadata: dict[str, str] | None = None) -> None:
        extra_args = {"Metadata": metadata} if metadata else None
        self._s3.upload_file(str(path), self.bucket, key, ExtraArgs=extra_args)
        log.info("Uploaded s3://%s/%s", self.bucket, key)

    def metadata(self, key: str) -> dict[str, str] | None:
        try:
            return self._s3.head_object(Bucket=self.bucket, Key=key)["Metadata"]
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def download_file(self, key: str, path: Path) -> None:
        self._s3.download_file(self.bucket, key, str(path))

    def write_parquet(self, df: pl.DataFrame, key: str) -> None:
        buffer = io.BytesIO()
        df.write_parquet(buffer, compression="zstd")
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        log.info("Wrote %d rows to s3://%s/%s", df.height, self.bucket, key)

    def read_parquet(self, key: str) -> pl.DataFrame:
        body = self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return pl.read_parquet(io.BytesIO(body))
