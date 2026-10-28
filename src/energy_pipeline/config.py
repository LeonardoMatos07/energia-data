from __future__ import annotations

import os
from dataclasses import dataclass

from energy_pipeline.sources import DEFAULT_ONS_BASE_URL


@dataclass(frozen=True)
class Settings:
    lake_bucket: str
    s3_endpoint_url: str | None
    warehouse_dsn: str
    ons_base_url: str
    http_timeout_seconds: int
    max_rejected_ratio: float
    reconciliation_tolerance: float
    reprocess_months: int
    api_url: str | None
    cognito_token_url: str | None
    data_client_id: str | None
    data_client_secret: str | None
    api_scope: str
    dispatch_report_email: bool = True

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            lake_bucket=os.getenv("LAKE_BUCKET", "energy-lake"),
            s3_endpoint_url=os.getenv("LAKE_S3_ENDPOINT_URL") or None,
            warehouse_dsn=os.getenv(
                "WAREHOUSE_DSN",
                "postgresql://warehouse:warehouse@warehouse-db:5432/warehouse",
            ),
            ons_base_url=os.getenv("ONS_BASE_URL", DEFAULT_ONS_BASE_URL),
            http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "120")),
            max_rejected_ratio=float(os.getenv("MAX_REJECTED_RATIO", "0.01")),
            reconciliation_tolerance=float(os.getenv("RECONCILIATION_TOLERANCE", "0.01")),
            reprocess_months=int(os.getenv("REPROCESS_MONTHS", "3")),
            api_url=os.getenv("API_URL") or None,
            cognito_token_url=os.getenv("COGNITO_TOKEN_URL") or None,
            data_client_id=os.getenv("DATA_CLIENT_ID") or None,
            data_client_secret=os.getenv("DATA_CLIENT_SECRET") or None,
            api_scope=os.getenv("API_SCOPE", "energia-api/admin"),
            dispatch_report_email=os.getenv("DISPATCH_REPORT_EMAIL", "true").lower()
            not in ("0", "false", "no"),
        )
