import base64
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Tuple

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.ukg.ukg_schemas import (
    TABLE_METADATA,
    TABLE_REGISTRY,
    TABLE_SCHEMAS,
)

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503}


class UkgLakeflowConnect(LakeflowConnect):
    """UKG Pro (UltiPro) connector implementing the LakeflowConnect interface.

    Supports two authentication mechanisms:
    - Basic Auth for core HCM REST endpoints (personnel, configuration, payroll)
    - OAuth 2.0 client credentials for recruiting and onboarding endpoints

    All table routing is driven by TABLE_REGISTRY — no per-table branching.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        # Required: core HCM auth
        self._service_host = options["service_host"].rstrip("/")
        username = options["username"]
        password = options["password"]
        self._customer_api_key = options["customer_api_key"]

        # Pre-compute Basic Auth header
        auth_str = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._basic_headers = {
            "Authorization": f"Basic {auth_str}",
            "US-Customer-Api-Key": self._customer_api_key,
            "Content-Type": "application/json",
        }

        # Optional: OAuth for recruiting/onboarding
        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        self._tenant_alias = options.get("tenant_alias")
        self._oauth_token = None
        self._oauth_expiry = 0.0

        # Cap cursor at init time to prevent chasing new data
        self._init_ts = datetime.now(timezone.utc).isoformat()

        # Default batch size
        self._default_max_records = 10000

    # ------------------------------------------------------------------
    # Interface methods
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Return all supported table names.

        Tables requiring OAuth (recruiting/onboarding) are only listed if
        client_id and client_secret are provided.
        """
        tables = []
        for name, config in TABLE_REGISTRY.items():
            if config["auth"] == "oauth" and not self._client_id:
                continue
            tables.append(name)
        return tables

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        config = TABLE_REGISTRY[table_name]

        if config["ingestion_type"] == "snapshot":
            return self._read_snapshot(table_name, config, table_options)
        else:
            return self._read_cdc(
                table_name, config, start_offset, table_options
            )

    # ------------------------------------------------------------------
    # Read strategies
    # ------------------------------------------------------------------

    def _read_snapshot(
        self,
        table_name: str,
        config: Dict[str, Any],
        table_options: dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        """Full refresh — paginate through all records, return empty offset."""
        records = self._paginate_all(table_name, config, params={}, table_options=table_options)
        return iter(records), {}

    def _read_cdc(
        self,
        table_name: str,
        config: Dict[str, Any],
        start_offset: dict,
        table_options: dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        """Incremental read using cursor field.

        On first run (start_offset is None), reads all data up to _init_ts.
        On subsequent runs, reads from the last cursor value up to _init_ts.
        """
        cursor_field = config["cursor_field"]
        cursor_filter = config["cursor_filter_param"]

        # Determine starting cursor
        since = None
        if start_offset and start_offset.get("cursor"):
            since = start_offset["cursor"]

        # If we've already caught up to init time, return no data
        if since and since >= self._init_ts:
            return iter([]), start_offset

        # Build filter params
        params = {}
        if since and cursor_filter:
            params[cursor_filter] = since

        records = self._paginate_all(table_name, config, params=params, table_options=table_options)

        if not records:
            # No new data — return same offset to signal completion
            return iter([]), start_offset or {}

        # Sort by cursor field and cap at init time
        records.sort(key=lambda r: r.get(cursor_field, ""))
        last_cursor = records[-1].get(cursor_field, "")
        capped_cursor = min(last_cursor, self._init_ts) if last_cursor else self._init_ts

        return iter(records), {"cursor": capped_cursor}

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _paginate_all(
        self,
        table_name: str,
        config: Dict[str, Any],
        params: Dict[str, str],
        table_options: dict[str, str],
    ) -> List[dict]:
        """Paginate through an endpoint, collecting up to max_records_per_batch."""
        max_records = int(
            table_options.get("max_records_per_batch", str(self._default_max_records))
        )
        pag = config["pagination"]
        page_param = pag["page_param"]
        per_page_param = pag["per_page_param"]
        per_page = min(
            int(table_options.get("per_page", str(pag["max_per_page"]))),
            pag["max_per_page"],
        )

        records: List[dict] = []
        page = 1

        while len(records) < max_records:
            request_params = {
                page_param: str(page),
                per_page_param: str(per_page),
                **params,
            }

            url = self._build_url(table_name, config)
            resp = self._request_with_retry("GET", url, config["auth"], params=request_params)

            if resp.status_code != 200:
                raise RuntimeError(
                    f"UKG API error for '{table_name}': "
                    f"{resp.status_code} {resp.text}"
                )

            batch = resp.json()
            # UKG returns a bare JSON array for most endpoints
            if isinstance(batch, dict):
                # Some endpoints may wrap in a key — try common patterns
                batch = batch.get("results", batch.get("data", batch.get("items", [])))
            if not isinstance(batch, list):
                batch = [batch]

            if not batch:
                break

            records.extend(batch)

            # If we got fewer records than requested, we've reached the end
            if len(batch) < per_page:
                break

            page += 1

        return records[:max_records]

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _build_url(self, table_name: str, config: Dict[str, Any]) -> str:
        """Build the full URL for an endpoint."""
        endpoint = config["endpoint"]

        # Onboarding endpoints have a tenant identifier placeholder
        if "{tenant_identifier}" in endpoint:
            if not self._tenant_alias:
                raise ValueError(
                    f"Table '{table_name}' requires 'tenant_alias' option "
                    "for onboarding endpoints."
                )
            endpoint = endpoint.replace("{tenant_identifier}", self._tenant_alias)

        base = f"https://{self._service_host}"

        # Recruiting endpoints use tenant alias as path prefix
        if config["auth"] == "oauth" and "/api/" in endpoint:
            if self._tenant_alias:
                return f"{base}/{self._tenant_alias}{endpoint}"
            return f"{base}{endpoint}"

        return f"{base}{endpoint}"

    def _request_with_retry(
        self,
        method: str,
        url: str,
        auth_type: str,
        **kwargs: Any,
    ) -> requests.Response:
        """Issue an API request with exponential backoff on retriable errors."""
        headers = self._get_headers(auth_type)

        backoff = INITIAL_BACKOFF
        resp = None
        for attempt in range(MAX_RETRIES):
            resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)

            # On 401 with OAuth, refresh token and retry once
            if resp.status_code == 401 and auth_type == "oauth" and attempt == 0:
                self._oauth_token = None
                headers = self._get_headers(auth_type)
                continue

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2

        return resp

    def _get_headers(self, auth_type: str) -> Dict[str, str]:
        """Return appropriate headers based on auth type."""
        if auth_type == "basic":
            return dict(self._basic_headers)

        # OAuth 2.0
        token = self._get_oauth_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _get_oauth_token(self) -> str:
        """Fetch or return cached OAuth 2.0 Bearer token."""
        now = time.time()
        if self._oauth_token and now < self._oauth_expiry:
            return self._oauth_token

        if not self._client_id or not self._client_secret:
            raise ValueError(
                "OAuth credentials (client_id, client_secret) are required "
                "for recruiting and onboarding tables."
            )

        token_url = f"https://{self._service_host}/authentication/v1/token"
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"OAuth token request failed: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        self._oauth_token = data["access_token"]
        # Cache with 5 min buffer before expiry
        expires_in = data.get("expires_in", 3600)
        self._oauth_expiry = now + expires_in - 300
        return self._oauth_token

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        """Raise ValueError if table_name is not in the registry."""
        available = self.list_tables()
        if table_name not in available:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {available}"
            )
