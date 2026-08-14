import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from pyspark.sql.types import ArrayType, StructType, VariantType

import databricks.labs.community_connector.source_simulator as _simulator_pkg
from databricks.labs.community_connector.sources.dualentry import dualentry as dualentry_module
from databricks.labs.community_connector.sources.dualentry.dualentry import (
    DualEntryLakeflowConnect,
    _retry_after_seconds,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

_CORPUS_DIR = Path(_simulator_pkg.__file__).parent / "specs" / "dualentry" / "corpus"


def _load_corpus(table: str) -> list:
    with open(_CORPUS_DIR / f"{table}.json", "r") as f:
        return json.load(f)


def _response_with_headers(headers: dict) -> requests.Response:
    resp = requests.Response()
    resp.headers.update(headers)
    return resp


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` for the retry test."""

    def __init__(self, status_code: int, headers: dict | None = None, body=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body if body is not None else {"items": []}
        self.text = ""

    def json(self):
        return self._body


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
    # Auth
    # ------------------------------------------------------------------

    def test_auth_header_is_raw_key_in_x_api_key(self):
        """The OpenAPI security scheme is an apiKey header literally named
        ``X-API-KEY`` carrying the raw key — not ``Bearer <key>`` and not an
        ``authorization`` header. A well-meaning "fix" adding a Bearer prefix
        would break live auth."""
        headers = self.connector._headers
        assert headers["X-API-KEY"] == "simulator-fake-key"
        assert "Authorization" not in headers and "authorization" not in headers

    # ------------------------------------------------------------------
    # Contract: read_table yields RAW records unchanged
    # ------------------------------------------------------------------

    def test_read_table_preserves_raw_records_unchanged(self):
        """Per the LakeflowConnect contract, ``read_table`` must yield the raw
        JSON records unchanged — no projection, no field mutation, no schema
        coercion (the framework's ``parse_value`` does that). Deep-equal the
        emitted records against the corpus fixtures, including nested
        ``custom_fields`` / ``next_approvers`` objects and arrays."""
        # Snapshot table: full corpus returned in file order, untouched.
        records, _ = self.connector.read_table("accounts", {}, {})
        assert list(records) == _load_corpus("accounts")

        # CDC table: the seeded corpus records (all past-dated) are returned
        # unchanged; the injected future records are excluded by the init cap,
        # so the emitted list equals the on-disk corpus exactly.
        records, _ = self.connector.read_table("journal_entries", {}, {})
        emitted = list(records)
        corpus = _load_corpus("journal_entries")
        assert emitted == corpus
        # The nested structured payloads survive as dicts/lists, not strings.
        assert isinstance(emitted[0]["custom_fields"], list)
        assert isinstance(emitted[0]["custom_fields"][0]["field"], dict)
        assert isinstance(emitted[0]["next_approvers"], list)
        assert isinstance(emitted[0]["next_approvers"][0], dict)

    def test_schema_declares_nested_structured_types(self):
        """``get_table_schema`` must declare the documented structured fields as
        Struct/Array/Variant so ``parse_value`` can consume the raw nested JSON
        — they must not be flattened to StringType."""
        je = self.connector.get_table_schema("journal_entries", {})
        fields = {f.name: f.dataType for f in je.fields}

        # custom_fields: ARRAY<STRUCT<field: STRUCT, value: STRUCT>>
        cf = fields["custom_fields"]
        assert isinstance(cf, ArrayType)
        assert isinstance(cf.elementType, StructType)
        cf_pair = {f.name: f.dataType for f in cf.elementType.fields}
        assert isinstance(cf_pair["field"], StructType)
        assert isinstance(cf_pair["value"], StructType)
        # The user-defined custom-field value payload is VariantType.
        value_fields = {f.name: f.dataType for f in cf_pair["value"].fields}
        assert isinstance(value_fields["value"], VariantType)

        # next_approvers: ARRAY<STRUCT>
        na = fields["next_approvers"]
        assert isinstance(na, ArrayType)
        assert isinstance(na.elementType, StructType)

        # bills.tax: STRUCT<regime: STRING, data: VARIANT>; tax_registration
        # _numbers: STRUCT<company: ARRAY, counterparty: ARRAY>.
        bills = self.connector.get_table_schema("bills", {})
        bfields = {f.name: f.dataType for f in bills.fields}
        tax = bfields["tax"]
        assert isinstance(tax, StructType)
        tax_sub = {f.name: f.dataType for f in tax.fields}
        assert isinstance(tax_sub["data"], VariantType)
        trn = bfields["tax_registration_numbers"]
        assert isinstance(trn, StructType)
        trn_sub = {f.name: f.dataType for f in trn.fields}
        assert isinstance(trn_sub["company"], ArrayType)
        assert isinstance(trn_sub["counterparty"], ArrayType)

    # ------------------------------------------------------------------
    # Incremental behaviour
    # ------------------------------------------------------------------

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
        assert len(seen_ids) == len(
            set(seen_ids)
        ), f"duplicate records across microbatches: {seen_ids}"
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

    # ------------------------------------------------------------------
    # Snapshot behaviour
    # ------------------------------------------------------------------

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

    def test_snapshot_pagination_advances_offsets(self):
        """A snapshot read walks the limit/offset pages: with ``limit=2`` over
        the 4-record corpus it requests offsets 0, 2, then a terminating empty
        page, and returns all 4 unique records exactly once."""
        seen_offsets: list = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy  # instance attr shadows the bound method
        try:
            records, offset = self.connector.read_table("accounts", {}, {"limit": "2"})
            rows = list(records)
        finally:
            del self.connector._get_json

        ids = [r["id"] for r in rows]
        assert len(rows) == 4 and len(set(ids)) == 4, f"expected 4 unique rows, got {ids}"
        assert seen_offsets == ["0", "2", "4"], seen_offsets
        assert offset == {"done": True}

    # ------------------------------------------------------------------
    # Retry / backoff
    # ------------------------------------------------------------------

    def test_retry_backoff_and_retry_after(self):
        """The request path retries on 429/500/503, honours ``Retry-After`` for
        the delay, stops as soon as a success arrives, and raises through
        ``_get_json`` when retries are exhausted."""
        conn = DualEntryLakeflowConnect({"api_key": "k"})

        # 429 (Retry-After=2) -> 500 -> 503 -> 200: all three retriable codes
        # are retried, then success stops further requests.
        responses = [
            _FakeResponse(429, {"Retry-After": "2"}),
            _FakeResponse(500),
            _FakeResponse(503),
            _FakeResponse(200, body={"items": [{"id": 1}]}),
        ]
        slept: list[float] = []
        with (
            patch.object(dualentry_module.requests, "get", side_effect=responses) as mock_get,
            patch.object(dualentry_module, "_sleep", side_effect=lambda s: slept.append(s)),
        ):
            body = conn._get_json("public/v2/accounts/")

        assert body == {"items": [{"id": 1}]}
        assert mock_get.call_count == 4, "should stop requesting after the first success"
        # Retry-After=2 overrides the initial 1.0s backoff on attempt 1; the
        # backoff then doubles (2s, 4s) on the retries that carry no header.
        assert slept == [2.0, 2.0, 4.0]

        # Exhausted retries: every attempt is a 429 -> the last non-2xx surfaces
        # as a RuntimeError from _get_json, after MAX_RETRIES-1 sleeps.
        conn2 = DualEntryLakeflowConnect({"api_key": "k"})
        slept2: list[float] = []
        with (
            patch.object(
                dualentry_module.requests,
                "get",
                side_effect=[_FakeResponse(429, {"Retry-After": "1"}) for _ in range(5)],
            ) as mock_get2,
            patch.object(dualentry_module, "_sleep", side_effect=lambda s: slept2.append(s)),
        ):
            with pytest.raises(RuntimeError):
                conn2._get_json("public/v2/accounts/")

        assert mock_get2.call_count == 5
        assert len(slept2) == 4  # MAX_RETRIES - 1

    def test_retry_after_helper_parses_seconds(self):
        """Unit cover for the header parser used by the retry path."""
        assert _retry_after_seconds(_response_with_headers({"Retry-After": "7"})) == 7.0
        assert _retry_after_seconds(_response_with_headers({})) is None
        assert _retry_after_seconds(_response_with_headers({"Retry-After": "soon"})) is None

    # ------------------------------------------------------------------
    # AR transaction streams (batch 1)
    # ------------------------------------------------------------------

    _AR_STREAMS = (
        "sales_orders",
        "customer_payments",
        "customer_credits",
        "customer_refunds",
        "customer_deposits",
        "customer_prepayments",
        "customer_prepayment_applications",
        "cash_sales",
    )

    def test_ar_streams_registered_as_cdc_on_internal_id(self):
        """All 8 AR transaction streams are exposed, incremental, and keyed on
        ``internal_id`` with an ``updated_at`` cursor (per the OpenAPI list
        schemas — not assumed)."""
        tables = self.connector.list_tables()
        for stream in self._AR_STREAMS:
            assert stream in tables, f"{stream} missing from list_tables()"
            meta = self.connector.read_table_metadata(stream, {})
            assert meta["ingestion_type"] == "cdc", stream
            assert meta["primary_keys"] == ["internal_id"], stream
            assert meta["cursor_field"] == "updated_at", stream

    def test_ar_stream_reads_raw_records_unchanged(self):
        """A representative AR stream with rich nesting
        (``customer_credits.integration_remote_records``) must be yielded raw —
        deep-equal against the corpus, with nested objects preserved as
        dicts/lists rather than stringified."""
        records, offset = self.connector.read_table("customer_credits", {}, {})
        emitted = list(records)
        assert emitted == _load_corpus("customer_credits")
        assert offset == {"cursor": self.connector._init_ts_iso}

        irr = emitted[0]["integration_remote_records"]
        assert isinstance(irr, list) and isinstance(irr[0], dict)
        assert isinstance(irr[0]["integration_provider"], dict)

    def test_ar_stream_offset_pagination_walks_pages(self):
        """An AR stream fetches multiple limit/offset pages: with ``limit=2``
        over the 5-record corpus it requests offsets 0, 2, 4, then a
        terminating short page, returning every record exactly once."""
        seen_offsets: list = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy
        try:
            records, offset = self.connector.read_table("sales_orders", {}, {"limit": "2"})
            rows = list(records)
        finally:
            del self.connector._get_json

        ids = [r["internal_id"] for r in rows]
        assert len(ids) == 5 and len(set(ids)) == 5, f"expected 5 unique rows, got {ids}"
        assert seen_offsets == ["0", "2", "4"], seen_offsets
        assert offset == {"cursor": self.connector._init_ts_iso}

    # ------------------------------------------------------------------
    # AP & purchasing streams
    # ------------------------------------------------------------------

    _AP_CDC_STREAMS = (
        "purchase_orders",
        "vendor_payments",
        "vendor_credits",
        "vendor_refunds",
        "vendor_prepayments",
        "vendor_prepayment_applications",
        "direct_expenses",
    )

    def test_ap_cdc_streams_registered_as_cdc_on_internal_id(self):
        """The 7 incremental AP streams are exposed, incremental, and keyed on
        ``internal_id`` with an ``updated_at`` cursor (per the OpenAPI list
        schemas — e.g. PublicPurchaseOrderV2ListSchemaOut — not assumed)."""
        tables = self.connector.list_tables()
        for stream in self._AP_CDC_STREAMS:
            assert stream in tables, f"{stream} missing from list_tables()"
            meta = self.connector.read_table_metadata(stream, {})
            assert meta["ingestion_type"] == "cdc", stream
            assert meta["primary_keys"] == ["internal_id"], stream
            assert meta["cursor_field"] == "updated_at", stream

    def test_paper_checks_registered_as_snapshot_on_id(self):
        """paper_checks is a snapshot stream keyed on ``id`` (its OpenAPI
        list-item schema PublicPaperCheckSchemaOut carries no ``updated_at``),
        so it is re-listed in full each trigger — no cursor field."""
        assert "paper_checks" in self.connector.list_tables()
        meta = self.connector.read_table_metadata("paper_checks", {})
        assert meta["ingestion_type"] == "snapshot"
        assert meta["primary_keys"] == ["id"]
        assert "cursor_field" not in meta

    def test_ap_stream_reads_raw_records_unchanged(self):
        """A representative AP stream with rich nesting (``vendor_credits`` — a
        ``tax`` union, ``tax_registration_numbers``, ``custom_fields``,
        ``classifications``) must be yielded raw — deep-equal against the corpus,
        nested objects preserved as dicts/lists rather than stringified."""
        records, offset = self.connector.read_table("vendor_credits", {}, {})
        emitted = list(records)
        assert emitted == _load_corpus("vendor_credits")
        assert offset == {"cursor": self.connector._init_ts_iso}
        # tax is a STRUCT<regime, data:VARIANT>; the untyped data leaf survives
        # as a dict, custom_fields survive as a list of {field, value} dicts.
        assert isinstance(emitted[0]["tax"], dict)
        assert isinstance(emitted[0]["tax"]["data"], dict)
        cf = emitted[0]["custom_fields"]
        assert isinstance(cf, list) and isinstance(cf[0]["field"], dict)

    def test_ap_cdc_caps_at_init_time(self):
        """The simulator seeds future-dated AP records; the first incremental
        read must exclude them via ``updated_before = _init_ts`` and park the
        cursor at init time so Trigger.AvailableNow terminates."""
        records, offset = self.connector.read_table("purchase_orders", {}, {})
        rows = list(records)
        init_iso = self.connector._init_ts_iso
        assert offset == {"cursor": init_iso}
        assert rows, "expected the seeded corpus records on the first read"
        for row in rows:
            assert row["updated_at"] <= init_iso, (
                f"record with updated_at={row['updated_at']} leaked past the "
                f"init-time cap {init_iso}"
            )

    def test_paper_checks_snapshot_pagination_spans_multiple_pages(self):
        """paper_checks is a snapshot: with ``limit=2`` over the 5-record corpus
        the read walks offsets 0, 2, 4 (spanning >1 page), returns all 5 unique
        records exactly once, and parks a ``done`` sentinel offset."""
        seen_offsets: list = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy
        try:
            records, offset = self.connector.read_table("paper_checks", {}, {"limit": "2"})
            rows = list(records)
        finally:
            del self.connector._get_json

        ids = [r["id"] for r in rows]
        assert len(ids) == 5 and len(set(ids)) == 5, f"expected 5 unique rows, got {ids}"
        assert seen_offsets == ["0", "2", "4"], seen_offsets
        assert offset == {"done": True}
        # Second call short-circuits (end_offset == start_offset contract).
        more, offset2 = self.connector.read_table("paper_checks", offset, {})
        assert list(more) == []
        assert offset2 == {"done": True}

    # --- GL & core streams ---

    _GL_SNAPSHOT_STREAMS = (
        "companies",
        "classifications",
        "classification_lines",
        "custom_fields",
        "journal_entry_lines",
        "intercompany_journal_entries",
        "budgets",
    )

    def test_gl_stream_metadata(self):
        for stream in self._GL_SNAPSHOT_STREAMS:
            expected_key = "record_number" if stream == "intercompany_journal_entries" else "id"
            assert self.connector.read_table_metadata(stream, {}) == {
                "primary_keys": [expected_key],
                "ingestion_type": "snapshot",
            }

        assert self.connector.read_table_metadata("statistical_journals", {}) == {
            "primary_keys": ["id"],
            "cursor_field": "updated_at",
            "ingestion_type": "cdc",
        }

    def test_gl_streams_yield_raw_nested_records(self):
        for stream in (*self._GL_SNAPSHOT_STREAMS, "statistical_journals"):
            records, _ = self.connector.read_table(stream, {}, {})
            assert list(records) == _load_corpus(stream)

        companies, _ = self.connector.read_table("companies", {}, {})
        company = next(companies)
        assert isinstance(company["address"], dict)
        assert isinstance(company["vat_registration_numbers"], list)
        ijes, _ = self.connector.read_table("intercompany_journal_entries", {}, {})
        assert isinstance(next(ijes)["items"][0]["classifications"], list)

    def test_gl_snapshot_spans_multiple_default_pages(self):
        seen_offsets: list[str | None] = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy
        try:
            records, offset = self.connector.read_table("classification_lines", {}, {})
            rows = list(records)
        finally:
            del self.connector._get_json

        assert len(rows) == 101
        assert seen_offsets == ["0", "100"]
        assert offset == {"done": True}

    def test_gl_schemas_use_structs_and_arrays_for_nested_fields(self):
        companies = {f.name: f.dataType for f in self.connector.get_table_schema("companies", {})}
        assert isinstance(companies["address"], StructType)
        assert isinstance(companies["vat_registration_numbers"], ArrayType)
        ije = {
            f.name: f.dataType
            for f in self.connector.get_table_schema("intercompany_journal_entries", {})
        }
        assert isinstance(ije["items"], ArrayType)
        assert isinstance(ije["items"].elementType, StructType)

    # --- end GL & core ---

    # ------------------------------------------------------------------
    # Banking / Tax / Fixed-assets streams
    # --- Banking/Tax/FixedAssets streams ---
    # ------------------------------------------------------------------

    _BANK_SNAPSHOT_STREAMS = (
        "bank_transactions",
        "bank_match_suggestions",
        "vat_rates",
        "gst_tax_rates",
        "product_tax_codes",
        "fixed_assets",
        "depreciation_books",
    )

    def test_bank_tax_fa_streams_registered_with_expected_metadata(self):
        """All 8 banking/tax/fixed-asset streams are exposed. bank_transfers is
        CDC keyed on ``internal_id`` with an ``updated_at`` cursor; the rest are
        snapshot. fixed_assets is keyed on ``internal_id`` (not ``id``) despite
        the naming; the other snapshots are keyed on ``id`` — per the OpenAPI
        list-item schemas, not assumed."""
        tables = self.connector.list_tables()

        assert "bank_transfers" in tables
        bt = self.connector.read_table_metadata("bank_transfers", {})
        assert bt["ingestion_type"] == "cdc"
        assert bt["primary_keys"] == ["internal_id"]
        assert bt["cursor_field"] == "updated_at"

        expected_pk = {
            "bank_transactions": ["id"],
            "bank_match_suggestions": ["id"],
            "vat_rates": ["id"],
            "gst_tax_rates": ["id"],
            "product_tax_codes": ["id"],
            "fixed_assets": ["internal_id"],
            "depreciation_books": ["id"],
        }
        for stream in self._BANK_SNAPSHOT_STREAMS:
            assert stream in tables, f"{stream} missing from list_tables()"
            meta = self.connector.read_table_metadata(stream, {})
            assert meta["ingestion_type"] == "snapshot", stream
            assert meta["primary_keys"] == expected_pk[stream], stream
            assert "cursor_field" not in meta, stream

    def test_bank_transfers_incremental_reads_raw_and_caps_at_init(self):
        """bank_transfers is the only CDC stream in this batch: the seeded corpus
        (ascending, past-dated ``updated_at``) is returned raw and deep-equal,
        the injected future records are excluded by the init cap, and the cursor
        parks at init time so Trigger.AvailableNow terminates. The array-valued
        ``currency_iso_4217_code`` survives as a list (not stringified)."""
        records, offset = self.connector.read_table("bank_transfers", {}, {})
        emitted = list(records)
        assert emitted == _load_corpus("bank_transfers")
        assert offset == {"cursor": self.connector._init_ts_iso}
        init_iso = self.connector._init_ts_iso
        for row in emitted:
            assert row["updated_at"] <= init_iso
        assert isinstance(emitted[0]["currency_iso_4217_code"], list)
        assert isinstance(emitted[0]["created_by"], dict)

    def test_fixed_assets_snapshot_reads_raw_nested_unchanged(self):
        """fixed_assets is snapshot with deep nesting — depreciation schedules
        (with a custom-schedule sub-array), classifications, and source lines
        must be yielded raw as dicts/lists, deep-equal against the corpus."""
        records, offset = self.connector.read_table("fixed_assets", {}, {})
        emitted = list(records)
        assert emitted == _load_corpus("fixed_assets")
        assert offset == {"done": True}
        sched = emitted[0]["depreciation_schedules"]
        assert isinstance(sched, list) and isinstance(sched[0], dict)
        assert isinstance(sched[0]["custom_schedule"], list)
        assert isinstance(emitted[0]["classifications"][0], dict)
        assert isinstance(emitted[0]["source_lines"][0], dict)

    def test_gst_tax_rates_nested_components_preserved(self):
        """gst_tax_rates carries a nested ``components`` array (CGST/SGST/IGST/
        CESS breakdown) that must survive as a list of dicts."""
        records, _ = self.connector.read_table("gst_tax_rates", {}, {})
        emitted = list(records)
        assert emitted == _load_corpus("gst_tax_rates")
        comps = emitted[0]["components"]
        assert isinstance(comps, list) and isinstance(comps[0], dict)
        assert "component_type" in comps[0]

    def test_bank_transactions_snapshot_spans_multiple_pages(self):
        """The bank_transactions corpus is larger than one page (105 records >
        the default page size of 100), so a default read walks two limit/offset
        pages — offsets 0 then 100 — and returns every record exactly once. The
        short second page (5 records) terminates the walk without a third
        request."""
        seen_offsets: list = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy
        try:
            records, offset = self.connector.read_table("bank_transactions", {}, {})
            rows = list(records)
        finally:
            del self.connector._get_json

        corpus = _load_corpus("bank_transactions")
        assert len(corpus) > 100, "fixture must exceed one page to prove multi-page walk"
        ids = [r["id"] for r in rows]
        assert len(ids) == len(corpus) and len(set(ids)) == len(corpus)
        assert seen_offsets == ["0", "100"], seen_offsets
        assert offset == {"done": True}

    # --- end Banking/Tax/FixedAssets ---

    # --- Recurring/RevRec/Workflow streams ---

    _RECURRING_STREAMS = (
        "recurring_invoices",
        "recurring_bills",
        "recurring_journal_entries",
    )
    _WORKFLOW_SNAPSHOT_KEYS = {
        "contracts": "id",
        "workflows": "id",
        "workflow_execution_states": "id",
        "workflow_actions": "id",
        "inbox_transactions": "number",
        "inbox_records": "record_id",
        "webhooks": "uuid",
    }

    def test_recurring_streams_are_cdc_on_internal_id(self):
        tables = self.connector.list_tables()
        for stream in self._RECURRING_STREAMS:
            assert stream in tables
            meta = self.connector.read_table_metadata(stream, {})
            assert meta == {
                "primary_keys": ["internal_id"],
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
            }
            records, offset = self.connector.read_table(stream, {}, {})
            assert list(records) == _load_corpus(stream)
            assert offset == {"cursor": self.connector._init_ts_iso}

    def test_workflow_and_integration_streams_are_snapshots(self):
        tables = self.connector.list_tables()
        for stream, key in self._WORKFLOW_SNAPSHOT_KEYS.items():
            assert stream in tables
            assert self.connector.read_table_metadata(stream, {}) == {
                "primary_keys": [key],
                "ingestion_type": "snapshot",
            }
            records, offset = self.connector.read_table(stream, {}, {})
            assert list(records) == _load_corpus(stream)
            assert offset == {"done": True}

    def test_recurring_and_workflow_nested_fields_are_structured(self):
        recurring_fields = {
            field.name: field.dataType
            for field in self.connector.get_table_schema("recurring_invoices", {}).fields
        }
        assert isinstance(recurring_fields["record_payload"], VariantType)
        assert isinstance(recurring_fields["records"], ArrayType)
        assert isinstance(recurring_fields["records"].elementType, StructType)

        contract_fields = {
            field.name: field.dataType
            for field in self.connector.get_table_schema("contracts", {}).fields
        }
        assert isinstance(contract_fields["metrics"], StructType)
        assert isinstance(contract_fields["change_orders"], ArrayType)
        assert isinstance(contract_fields["change_orders"].elementType, StructType)

        inbox_fields = {
            field.name: field.dataType
            for field in self.connector.get_table_schema("inbox_transactions", {}).fields
        }
        assert isinstance(inbox_fields["approval_info"], StructType)

    def test_contract_snapshot_spans_multiple_pages(self):
        seen_offsets: list = []
        original_get_json = self.connector._get_json

        def spy(path, params=None):
            seen_offsets.append((params or {}).get("offset"))
            return original_get_json(path, params=params)

        self.connector._get_json = spy
        try:
            records, offset = self.connector.read_table("contracts", {}, {"limit": "2"})
            rows = list(records)
        finally:
            del self.connector._get_json

        assert [row["id"] for row in rows] == [9001, 9002, 9003, 9004, 9005, 9006]
        assert seen_offsets == ["0", "2", "4", "6"]
        assert offset == {"done": True}

    # --- end Recurring/RevRec/Workflow ---
