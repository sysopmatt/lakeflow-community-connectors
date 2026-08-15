import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hubspot_extended.hubspot_extended_schemas import (
    CRM_OBJECTS,
    SEARCH_CURSOR_PROPERTIES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    crm_request_properties,
)


class HubspotExtendedLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict) -> None:
        self.access_token = options["access_token"]
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        if table_name in CRM_OBJECTS:
            return self._read_crm_object(table_name, start_offset)
        if table_name == "owners":
            return self._read_snapshot("/crm/v3/owners")
        if table_name == "pipelines":
            return self._read_snapshot("/crm/v3/pipelines/deals")

        raise ValueError(f"Unsupported table: {table_name}")

    def _read_crm_object(self, table_name: str, start_offset: dict) -> tuple[Iterator[dict], dict]:
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        if checkpoint and checkpoint >= self._init_ts:
            return iter(()), start_offset

        if checkpoint:
            records = self._fetch_search_records(table_name, checkpoint)
        else:
            records = self._fetch_list_records(f"/crm/v3/objects/{table_name}")

        latest_updated = checkpoint
        for record in records:
            updated_at = record.get("updatedAt")
            if updated_at and (latest_updated is None or updated_at > latest_updated):
                latest_updated = updated_at

        if latest_updated and latest_updated > self._init_ts:
            latest_updated = self._init_ts
        offset = {"updatedAt": latest_updated} if latest_updated else {}
        return iter(records), offset

    def _read_snapshot(self, path: str) -> tuple[Iterator[dict], dict]:
        return iter(self._fetch_list_records(path)), {}

    def _fetch_list_records(self, path: str) -> list[dict]:
        records: list[dict] = []
        after = None
        while True:
            params = {"limit": "100"}
            if after:
                params["after"] = after
            response = requests.get(
                f"{self.base_url}{path}",
                headers=self.headers,
                params=params,
                timeout=60,
            )
            self._raise_for_status(response)
            payload = response.json()
            records.extend(payload.get("results", []))
            after = payload.get("paging", {}).get("next", {}).get("after")
            if not after:
                return records
            time.sleep(0.1)

    def _fetch_search_records(self, table_name: str, checkpoint: str) -> list[dict]:
        records: list[dict] = []
        after = None
        modified_property = SEARCH_CURSOR_PROPERTIES[table_name]
        value = str(self._iso_to_epoch_millis(checkpoint))
        while True:
            body = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": modified_property,
                                "operator": "GTE",
                                "value": value,
                            }
                        ]
                    }
                ],
                "sorts": [{"propertyName": modified_property, "direction": "ASCENDING"}],
                "limit": 100,
                "properties": crm_request_properties(table_name),
            }
            if after:
                body["after"] = after
            response = requests.post(
                f"{self.base_url}/crm/v3/objects/{table_name}/search",
                headers=self.headers,
                json=body,
                timeout=60,
            )
            self._raise_for_status(response)
            payload = response.json()
            records.extend(payload.get("results", []))
            after = payload.get("paging", {}).get("next", {}).get("after")
            if not after:
                return records
            time.sleep(0.1)

    @staticmethod
    def _iso_to_epoch_millis(value: str) -> int:
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
        except ValueError:
            return 0

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        if response.status_code >= 400:
            raise RuntimeError(f"HubSpot API error: {response.status_code} {response.text}")

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {SUPPORTED_TABLES}"
            )
