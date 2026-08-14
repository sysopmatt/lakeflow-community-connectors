import requests

from databricks.labs.community_connector.sources.dualentry.dualentry import (
    DualEntryLakeflowConnect,
    _retry_after_seconds,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


def _response_with_headers(headers: dict) -> requests.Response:
    resp = requests.Response()
    resp.headers.update(headers)
    return resp


class TestDualEntryConnector(LakeflowConnectTests):
    connector_class = DualEntryLakeflowConnect
    simulator_source = "dualentry"
    replay_config = {
        # The live API root is https://api.dualentry.com; the simulator matches
        # on the URL path only (/public/v2/...), so any host works here. Keep an
        # obviously-fake one.
        "base_url": "https://simulator.dualentry.example",
        "api_key": "simulator-fake-key",
    }

    # ------------------------------------------------------------------
    # DualEntry-specific behaviour
    # ------------------------------------------------------------------

    def test_auth_header_is_raw_key_in_x_api_key(self):
        """The OpenAPI security scheme is an apiKey header literally named
        ``X-API-KEY`` carrying the raw key — not ``Bearer <key>`` and not an
        ``authorization`` header. A well-meaning "fix" adding a Bearer prefix
        would break live auth."""
        headers = self.connector._headers
        assert headers["X-API-KEY"] == "simulator-fake-key"
        assert "Authorization" not in headers and "authorization" not in headers

    def test_incremental_caps_at_init_time(self):
        """The simulator seeds future-dated records (see
        ``synthesize_future_records`` in the spec); the first read must exclude
        them via ``updated_before = _init_ts`` and park the cursor at init time
        so Trigger.AvailableNow terminates."""
        records, offset = self.connector.read_table("journal_entries", {}, {})
        rows = list(records)
        init_iso = self.connector._init_ts_iso
        assert offset == {"cursor": init_iso}
        assert rows, "expected the seeded corpus records on the first read"
        for row in rows:
            assert row["updated_at"] <= init_iso, (
                f"record with updated_at={row['updated_at']} leaked past the "
                f"init-time cap {init_iso}"
            )

    def test_incremental_uses_updated_at_cursor_and_internal_id_pk(self):
        """journal_entries is keyed on ``internal_id`` (not ``id``) with an
        ``updated_at`` cursor — a per-stream fact taken from the OpenAPI list
        schema."""
        meta = self.connector.read_table_metadata("journal_entries", {})
        assert meta["primary_keys"] == ["internal_id"]
        assert meta["cursor_field"] == "updated_at"
        assert meta["ingestion_type"] == "cdc"
        assert self.connector.read_table_metadata("bills", {})["primary_keys"] == ["number"]

    def test_max_records_per_batch_splits_range_without_loss(self):
        """``max_records_per_batch`` splits a bounded updated_at range across
        microbatches with the range pinned in the offset; the union of the
        microbatches must cover every record exactly once (state resumption via
        the ``offset`` continuation)."""
        seen_ids: list[int] = []
        offset: dict = {}
        for _ in range(20):
            records, next_offset = self.connector.read_table(
                "journal_entries", offset, {"limit": "2", "max_records_per_batch": "2"}
            )
            seen_ids.extend(r["internal_id"] for r in records)
            if next_offset == offset:
                break
            offset = next_offset
        else:
            raise AssertionError("paged incremental read did not converge")

        full_records, _ = self.connector.read_table("journal_entries", {}, {})
        expected_ids = {r["internal_id"] for r in full_records}
        assert len(seen_ids) == len(set(seen_ids)), (
            f"duplicate records across microbatches: {seen_ids}"
        )
        assert set(seen_ids) == expected_ids, (
            f"paged read missed records: got {sorted(seen_ids)}, "
            f"expected {sorted(expected_ids)}"
        )

    def test_lookback_not_baked_into_stored_cursor(self):
        """Lookback is a read-time widening only. The stored cursor after a
        drained range must be the range's upper bound — never the widened lower
        bound — or the cursor would walk backwards forever."""
        _, first_offset = self.connector.read_table("invoices", {}, {})
        records, second_offset = self.connector.read_table(
            "invoices", first_offset, {"lookback_seconds": "86400"}
        )
        list(records)
        assert second_offset == first_offset, (
            "caught-up read must return start_offset unchanged "
            "(termination contract), even with a large lookback"
        )

    def test_snapshot_completes_after_one_pass(self):
        """Snapshot tables emit records on the first call and a ``done``
        sentinel offset; the second call short-circuits to no records (the
        end_offset == start_offset termination contract)."""
        records, offset = self.connector.read_table("accounts", {}, {})
        rows = list(records)
        assert rows, "expected the seeded corpus records on the first read"
        assert offset == {"done": True}
        more, offset2 = self.connector.read_table("accounts", offset, {})
        assert list(more) == []
        assert offset2 == {"done": True}

    def test_retry_after_parses_seconds(self):
        """DualEntry 429s advertise a numeric ``Retry-After`` (seconds); the
        connector honours it (taking the max of the advertised wait and its
        own backoff). Absent / unparseable headers fall back to backoff."""
        assert _retry_after_seconds(_response_with_headers({"Retry-After": "7"})) == 7.0
        assert _retry_after_seconds(_response_with_headers({})) is None
        assert _retry_after_seconds(_response_with_headers({"Retry-After": "soon"})) is None

    def test_custom_fields_json_encoded(self):
        """``custom_fields[].field`` / ``.value`` are user-defined and untyped
        on the API. Non-string values must be JSON-serialized so the StringType
        sub-columns stay stable."""
        raw = {
            "internal_id": 42,
            "custom_fields": [
                {"field": {"id": 1, "name": "cf"}, "value": {"k": 1}},
                {"field": "plain_field", "value": "plain_value"},
            ],
            "next_approvers": [{"id": 9, "name": "a"}],
        }
        mapped = self.connector._map_record("journal_entries", raw)
        cfs = mapped["custom_fields"]
        assert cfs[0]["field"] == '{"id": 1, "name": "cf"}'
        assert cfs[0]["value"] == '{"k": 1}'
        assert cfs[1]["field"] == "plain_field"
        assert cfs[1]["value"] == "plain_value"
        assert mapped["next_approvers"] == '[{"id": 9, "name": "a"}]'

    def test_bill_tax_union_json_encoded(self):
        """``bills.tax.data`` is a discriminated union keyed by
        ``tax.regime``; ``data`` is JSON-serialized to a string while ``regime``
        stays a plain string, and ``tax_registration_numbers`` is serialized
        whole."""
        raw = {
            "number": 7,
            "tax": {"regime": "vat", "data": {"rate": "0.2", "amount": "5.00"}},
            "tax_registration_numbers": {"company": [{"value": "GB1"}], "counterparty": []},
        }
        mapped = self.connector._map_record("bills", raw)
        assert mapped["tax"]["regime"] == "vat"
        assert mapped["tax"]["data"] == '{"amount": "5.00", "rate": "0.2"}'
        assert mapped["tax_registration_numbers"] == (
            '{"company": [{"value": "GB1"}], "counterparty": []}'
        )
