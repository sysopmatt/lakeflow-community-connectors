import json
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hubspot_extended.hubspot_extended_schemas import (
    CRM_OBJECTS,
    LEGACY_OFFSET_TABLE_PATHS,
    PROPERTY_OBJECT_TYPES,
    SEARCH_CURSOR_PROPERTIES,
    SNAPSHOT_TABLE_PATHS,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    V3_CURSOR_TABLE_PATHS,
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
        if table_name in V3_CURSOR_TABLE_PATHS:
            return self._read_v3_cursor_table(table_name, start_offset)
        if table_name in LEGACY_OFFSET_TABLE_PATHS:
            return self._read_legacy_offset_table(table_name, start_offset)
        if table_name == "form_submissions":
            return self._read_form_submissions(start_offset)
        if table_name == "conversation_messages":
            return self._read_conversation_messages(start_offset)
        if table_name == "properties":
            return self._read_properties()
        if table_name in SNAPSHOT_TABLE_PATHS:
            return self._read_snapshot(table_name, SNAPSHOT_TABLE_PATHS[table_name])
        if table_name == "owners":
            return self._read_snapshot(table_name, "/crm/v3/owners")
        if table_name == "pipelines":
            return self._read_snapshot(table_name, "/crm/v3/pipelines/deals")

        raise ValueError(f"Unsupported table: {table_name}")

    def _read_crm_object(self, table_name: str, start_offset: dict) -> tuple[Iterator[dict], dict]:
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        if checkpoint and checkpoint >= self._init_ts:
            return iter(()), start_offset

        if checkpoint:
            records = self._fetch_search_records(table_name, checkpoint)
        else:
            records = self._fetch_list_records(
                f"/crm/v3/objects/{table_name}",
                extra_params={"properties": ",".join(crm_request_properties(table_name))},
            )

        latest_updated = checkpoint
        for record in records:
            updated_at = record.get("updatedAt")
            if updated_at and (latest_updated is None or updated_at > latest_updated):
                latest_updated = updated_at

        if latest_updated and latest_updated > self._init_ts:
            latest_updated = self._init_ts
        offset = {"updatedAt": latest_updated} if latest_updated else {}
        return iter(records), offset

    def _read_v3_cursor_table(
        self, table_name: str, start_offset: dict
    ) -> tuple[Iterator[dict], dict]:
        cursor_field = TABLE_METADATA[table_name]["cursor_field"]
        checkpoint = start_offset.get(cursor_field) if start_offset else None
        since_params = self._v3_since_params(table_name, checkpoint)
        records = self._fetch_list_records(
            V3_CURSOR_TABLE_PATHS[table_name], extra_params=since_params
        )
        if checkpoint:
            # Server-side filters are an efficiency hint; client-side filtering is the
            # correctness guard if HubSpot ignores an unsupported or mistyped parameter.
            records = [
                record
                for record in records
                if record.get(cursor_field) and record.get(cursor_field) >= checkpoint
            ]
        records = self._prepare_records(table_name, records)

        latest = checkpoint
        for record in records:
            cursor = record.get(cursor_field)
            if cursor and (latest is None or cursor > latest):
                latest = cursor

        latest = self._cap_latest(latest)
        offset = {cursor_field: latest} if latest else {}
        return iter(records), offset

    def _read_legacy_offset_table(
        self, table_name: str, start_offset: dict
    ) -> tuple[Iterator[dict], dict]:
        cursor_field = TABLE_METADATA[table_name]["cursor_field"]
        checkpoint = start_offset.get(cursor_field) if start_offset else None
        extra_params: dict[str, str] = {}
        if table_name == "email_events" and checkpoint is not None:
            extra_params["startTimestamp"] = str(checkpoint)
        records = self._fetch_legacy_offset_records(
            LEGACY_OFFSET_TABLE_PATHS[table_name], extra_params=extra_params
        )
        if checkpoint is not None:
            records = [
                record
                for record in records
                if record.get(cursor_field) is not None and record.get(cursor_field) > checkpoint
            ]

        latest = checkpoint
        for record in records:
            cursor = record.get(cursor_field)
            if cursor is not None and (latest is None or cursor > latest):
                latest = cursor

        latest = self._cap_latest(latest)
        records = self._prepare_records(table_name, records)
        offset = {cursor_field: latest} if latest is not None else {}
        return iter(records), offset

    def _read_form_submissions(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """Fan out over forms, then read each form's submissions endpoint.

        HubSpot exposes submissions by form, not as one global stream. This
        method first enumerates ``/marketing/v3/forms/`` with v3 cursor
        pagination, then calls
        ``/form-integrations/v1/submissions/forms/{formGuid}`` for each form
        using that endpoint's cursor pagination. HubSpot does not include the
        parent form GUID on each submission payload, so this method injects
        ``form_id`` to make the composite primary key materializable.
        """
        cursor_field = TABLE_METADATA["form_submissions"]["cursor_field"]
        checkpoint = start_offset.get(cursor_field) if start_offset else None
        forms = self._fetch_list_records(V3_CURSOR_TABLE_PATHS["forms"])
        records: list[dict] = []
        for form in forms:
            form_id = form.get("guid") or form.get("id")
            if not form_id:
                continue
            submissions = self._fetch_list_records(
                f"/form-integrations/v1/submissions/forms/{form_id}", limit="50"
            )
            records.extend(self._inject_parent_key(submissions, "form_id", form_id))

        if checkpoint:
            records = [
                record
                for record in records
                if record.get(cursor_field) and record.get(cursor_field) > checkpoint
            ]

        latest = checkpoint
        for record in records:
            cursor = record.get(cursor_field)
            if cursor and (latest is None or cursor > latest):
                latest = cursor

        latest = self._cap_latest(latest)
        offset = {cursor_field: latest} if latest else {}
        return iter(records), offset

    def _read_conversation_messages(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """Fan out over conversation threads, then read each thread's messages.

        HubSpot exposes messages below a thread resource. This method first
        enumerates ``/conversations/v3/conversations/threads`` with v3 cursor
        pagination, then calls
        ``/conversations/v3/conversations/threads/{threadId}/messages`` for
        each thread. Message records are yielded exactly as returned by the
        messages endpoint. HubSpot does not include the parent thread id on
        every message payload, so this method injects ``thread_id`` for the
        composite primary key.
        """
        cursor_field = TABLE_METADATA["conversation_messages"]["cursor_field"]
        checkpoint = start_offset.get(cursor_field) if start_offset else None
        threads = self._fetch_list_records(V3_CURSOR_TABLE_PATHS["conversation_threads"])
        records: list[dict] = []
        for thread in threads:
            thread_id = thread.get("id")
            if not thread_id:
                continue
            messages = self._fetch_list_records(
                f"/conversations/v3/conversations/threads/{thread_id}/messages"
            )
            records.extend(self._inject_parent_key(messages, "thread_id", thread_id))

        if checkpoint:
            records = [
                record
                for record in records
                if record.get(cursor_field) and record.get(cursor_field) > checkpoint
            ]

        latest = checkpoint
        for record in records:
            cursor = record.get(cursor_field)
            if cursor and (latest is None or cursor > latest):
                latest = cursor

        latest = self._cap_latest(latest)
        offset = {cursor_field: latest} if latest else {}
        return iter(records), offset

    def _read_properties(self) -> tuple[Iterator[dict], dict]:
        """Fan out over CRM object types and read their property definitions."""
        records: list[dict] = []
        for object_type in PROPERTY_OBJECT_TYPES:
            definitions = self._fetch_list_records(f"/crm/v3/properties/{object_type}")
            records.extend(self._inject_parent_key(definitions, "objectType", object_type))
        return iter(records), {}

    def _read_snapshot(self, table_name: str, path: str) -> tuple[Iterator[dict], dict]:
        return iter(self._prepare_records(table_name, self._fetch_list_records(path))), {}

    def _fetch_list_records(
        self,
        path: str,
        *,
        extra_params: dict[str, str] | None = None,
        limit: str = "100",
    ) -> list[dict]:
        records: list[dict] = []
        after = None
        while True:
            params = {"limit": limit}
            if extra_params:
                params.update(extra_params)
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

    def _fetch_legacy_offset_records(
        self, path: str, *, extra_params: dict[str, str] | None = None
    ) -> list[dict]:
        records: list[dict] = []
        offset = None
        while True:
            params = {"limit": "100"}
            if extra_params:
                params.update(extra_params)
            if offset is not None:
                params["offset"] = str(offset)
            response = requests.get(
                f"{self.base_url}{path}",
                headers=self.headers,
                params=params,
                timeout=60,
            )
            self._raise_for_status(response)
            payload = response.json()
            page = (
                payload.get("events") or payload.get("results") or payload.get("submissions") or []
            )
            records.extend(page)
            if not payload.get("hasMore"):
                return records
            offset = payload.get("offset")
            if offset is None:
                return records
            time.sleep(0.1)

    def _prepare_records(self, table_name: str, records: list[dict]) -> list[dict]:
        if table_name == "pipelines":
            return [self._encode_pipeline_metadata(record) for record in records]
        if table_name == "behavioral_events":
            return [self._encode_json_field(record, "properties") for record in records]
        if table_name == "email_events":
            return [self._encode_json_field(record, "response") for record in records]
        return records

    def _v3_since_params(self, table_name: str, checkpoint: str | None) -> dict[str, str]:
        if not checkpoint:
            return {}
        if table_name == "behavioral_events":
            # HubSpot Events API documents occurredAfter for event occurrence time filters.
            return {"occurredAfter": checkpoint}
        if table_name in {"blog_posts", "blog_tags", "blog_authors"}:
            # HubSpot CMS blog collections document updatedAfter.
            return {"updatedAfter": checkpoint}
        if table_name in {"landing_pages", "site_pages"}:
            # HubSpot CMS pages document property filters; updatedAt supports gte.
            return {"updatedAt__gte": checkpoint}
        return {}

    @classmethod
    def _inject_parent_key(cls, records: list[dict], field_name: str, value: Any) -> list[dict]:
        injected = []
        for record in records:
            next_record = dict(record)
            next_record[field_name] = next_record.get(field_name) or value
            injected.append(next_record)
        return injected

    @classmethod
    def _encode_pipeline_metadata(cls, record: dict) -> dict:
        stages = record.get("stages")
        if not isinstance(stages, list):
            return record

        next_record = dict(record)
        next_stages = []
        for stage in stages:
            if isinstance(stage, dict):
                next_stages.append(cls._encode_json_field(stage, "metadata"))
            else:
                next_stages.append(stage)
        next_record["stages"] = next_stages
        return next_record

    @staticmethod
    def _encode_json_field(record: dict, field_name: str) -> dict:
        value = record.get(field_name)
        if not isinstance(value, (dict, list)):
            return record
        next_record = dict(record)
        next_record[field_name] = json.dumps(value, sort_keys=True)
        return next_record

    def _cap_latest(self, latest: Any) -> Any:
        if latest is None:
            return latest
        init_cap = (
            self._iso_to_epoch_millis(self._init_ts) if isinstance(latest, int) else self._init_ts
        )
        return init_cap if latest > init_cap else latest

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
