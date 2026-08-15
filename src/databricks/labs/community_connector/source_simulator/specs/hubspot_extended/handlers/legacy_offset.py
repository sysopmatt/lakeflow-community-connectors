from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


def serve_email_events(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    return _legacy_response(prep, records, records_key="events", default_limit=2)


def serve_form_submissions(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    form_guid = urlparse(prep.url or "").path.rstrip("/").split("/")[-1]
    filtered = [record for record in records if record.get("form_id") == form_guid]
    return _legacy_response(prep, filtered, records_key="results", default_limit=100)


def serve_thread_messages(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    parts = urlparse(prep.url or "").path.rstrip("/").split("/")
    thread_id = parts[-2] if len(parts) >= 2 else ""
    filtered = [record for record in records if record.get("thread_id") == thread_id]
    return _json_response(prep, {"results": filtered, "paging": {}})


def serve_properties(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    object_type = urlparse(prep.url or "").path.rstrip("/").split("/")[-1]
    filtered = [record for record in records if record.get("objectType") == object_type]
    return _json_response(prep, {"results": filtered, "paging": {}})


def _legacy_response(
    prep: PreparedRequest,
    records: list[dict[str, Any]],
    *,
    records_key: str,
    default_limit: int,
) -> Response:
    query = parse_qs(urlparse(prep.url or "").query)
    offset = _int_param(query, "offset", 0)
    limit = min(_int_param(query, "limit", default_limit), default_limit)
    page = records[offset : offset + limit]
    next_offset = offset + len(page)
    has_more = next_offset < len(records)
    body = {
        records_key: page,
        "hasMore": has_more,
        "offset": next_offset if has_more else None,
    }
    record = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)


def _json_response(prep: PreparedRequest, body: dict[str, Any]) -> Response:
    record = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)


def _int_param(query: dict[str, list[str]], name: str, default: int) -> int:
    try:
        return max(0, int((query.get(name) or [str(default)])[-1]))
    except (TypeError, ValueError):
        return default
