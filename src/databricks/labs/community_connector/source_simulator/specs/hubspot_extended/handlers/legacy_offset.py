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
    query = parse_qs(urlparse(prep.url or "").query)
    start_timestamp = _int_param(query, "startTimestamp", 0)
    if start_timestamp:
        records = [record for record in records if record.get("created", 0) >= start_timestamp]
    return _legacy_response(prep, records, records_key="events", default_limit=2)


def serve_form_submissions(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    return _cursor_response(prep, records, default_limit=1, max_limit=50)


def serve_thread_messages(prep: PreparedRequest, spec, corpus) -> Response:
    records = corpus.get(spec.corpus) or []
    parts = urlparse(prep.url or "").path.rstrip("/").split("/")
    thread_id = parts[-2] if len(parts) >= 2 else ""
    filtered = [record for record in records if record.get("thread_id") == thread_id]
    return _json_response(prep, {"results": filtered, "paging": {}})


def serve_properties(prep: PreparedRequest, spec, corpus) -> Response:
    records_by_object = corpus.get(spec.corpus) or {}
    object_type = urlparse(prep.url or "").path.rstrip("/").split("/")[-1]
    records = records_by_object.get(object_type, []) if isinstance(records_by_object, dict) else []
    return _json_response(prep, {"results": records, "paging": {}})


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


def _cursor_response(
    prep: PreparedRequest,
    records: list[dict[str, Any]],
    *,
    default_limit: int,
    max_limit: int,
) -> Response:
    query = parse_qs(urlparse(prep.url or "").query)
    after = _int_param(query, "after", 0)
    limit = min(_int_param(query, "limit", default_limit), max_limit, default_limit)
    page = records[after : after + limit]
    next_after = after + len(page)
    body: dict[str, Any] = {"results": page}
    if next_after < len(records):
        body["paging"] = {"next": {"after": str(next_after)}}
    else:
        body["paging"] = {}
    return _json_response(prep, body)


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
