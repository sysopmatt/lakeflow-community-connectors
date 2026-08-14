"""DualEntry source connector for Lakeflow Community Connectors.

Implements the :class:`LakeflowConnect` interface against the DualEntry public
REST API (https://docs.dualentry.com). DualEntry is a cloud accounting / ERP
platform; seven tables are exposed:

    CDC (incremental): journal_entries, invoices, bills
    Snapshot:          accounts, customers, vendors, items

Authentication is a static API key sent in an HTTP header literally named
``X-API-KEY`` carrying the **raw** key (no ``Bearer`` prefix — this is what the
public OpenAPI security scheme declares). The optional ``base_url`` option
points the connector at the dev host (https://api-dev.dualentry.com); it
defaults to the production host (https://api.dualentry.com). All V2 resource
collections live under ``/public/v2/``.

Every list endpoint paginates with ``limit`` + ``offset`` query parameters
(``limit`` default and server maximum are both 100); records are wrapped in a
top-level ``items`` array and the connector detects the last page by receiving
fewer than ``limit`` records.

Incremental strategy (journal_entries, invoices, bills): the endpoints accept
inclusive ``updated_after`` / ``updated_before`` filters (``updated_at__gte`` /
``updated_at__lte``) and every record carries a required top-level
``updated_at`` (plus ``created_at``). Each trigger reads the bounded range
``[cursor - lookback_seconds, _init_ts]`` offset-page by offset-page; when a
page comes back short the range is drained and the cursor advances to the
range's upper bound. The upper bound is pinned at ``_init_ts`` (an ISO
timestamp captured once in ``__init__``) so Trigger.AvailableNow always
terminates — records updated after the trigger started are excluded until the
next trigger. ``lookback_seconds`` (default 300) is applied at read time only,
never stored, to re-capture records whose ``updated_at`` moved past the range's
upper bound while it was being paged; upserts on the primary key make the
overlap harmless. The very first sync has no lower bound (full backfill) unless
``start_timestamp`` is supplied. ``ordering=updated_at`` is sent so offset
pages are stable on the live API.

Snapshot tables (accounts, customers, vendors, items) have no
``updated_after`` / ``updated_before`` filter and their records carry no
``updated_at``, so they are re-listed in full each trigger and upserted on the
primary key. ``ordering=id`` is sent to keep their offset pages stable.

Primary keys differ per stream and are taken from the OpenAPI list-item
schemas (they are **not** uniformly ``id``): journal_entries and invoices are
keyed on ``internal_id``, bills on ``number``, and accounts/customers/vendors/
items on ``id``.

``read_table`` yields the **raw** JSON records exactly as the API returns them
(per the LakeflowConnect contract — the framework's ``parse_value`` coerces
each record to the declared schema; the connector does not mutate records or
project them). The static schemas therefore model the real nested shapes with
``StructType`` / ``ArrayType``. A handful of genuinely polymorphic / untyped
fields are declared ``VariantType`` so any JSON shape is preserved without
loss (see ``dualentry_api_doc.md``): ``bills.tax.data`` (a union keyed by
``tax.regime``), the custom-field ``value`` payload, and the untyped
``company_ids`` / ``options`` / ``default_value`` on a custom-field definition.

DualEntry rate-limits with a token bucket: a ``429`` carries a ``Retry-After``
header (seconds). The connector retries on ``429`` and on ``500`` / ``502`` /
``503`` / ``504`` with exponential backoff, honouring ``Retry-After`` when
present (taking the max of the advertised wait and the backoff). Every request
carries an explicit timeout so a stuck socket cannot hang a trigger.

DualEntry exposes no deleted-records feed, so deletes do not propagate
(``cdc``, not ``cdc_with_deletes``).
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Iterator
from urllib.parse import urljoin

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://api.dualentry.com"

# HTTP retry knobs — used by the lightweight retry helper.
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 30

# ``limit`` query param: DualEntry default and server max are both 100.
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100

# Read-time lookback for CDC ranges. Records updated while a bounded range is
# being paginated fall out of the range's filter (their new ``updated_at``
# exceeds the pinned upper bound) and can shift pagination; re-reading a small
# overlap on the next trigger re-captures them. Upserts make the overlap
# harmless.
DEFAULT_LOOKBACK_SECONDS = 300


# Table → endpoint path (all list endpoints wrap records in a top-level
# ``items`` array).
TABLE_ENDPOINTS: dict[str, str] = {
    "accounts": "public/v2/accounts/",
    "journal_entries": "public/v2/journal-entries/",
    "invoices": "public/v2/invoices/",
    "bills": "public/v2/bills/",
    "customers": "public/v2/customers/",
    "vendors": "public/v2/vendors/",
    "items": "public/v2/items/",
    # AR transaction streams (batch 1).
    "sales_orders": "public/v2/sales-orders/",
    "customer_payments": "public/v2/customer-payments/",
    "customer_credits": "public/v2/customer-credits/",
    "customer_refunds": "public/v2/customer-refunds/",
    "customer_deposits": "public/v2/customer-deposits/",
    "customer_prepayments": "public/v2/customer-prepayments/",
    "customer_prepayment_applications": "public/v2/customer-prepayment-applications/",
    "cash_sales": "public/v2/cash-sales/",
    # --- AP & purchasing streams ---
    "purchase_orders": "public/v2/purchase-orders/",
    "vendor_payments": "public/v2/vendor-payments/",
    "vendor_credits": "public/v2/vendor-credits/",
    "vendor_refunds": "public/v2/vendor-refunds/",
    "vendor_prepayments": "public/v2/vendor-prepayments/",
    "vendor_prepayment_applications": "public/v2/vendor-prepayment-applications/",
    "direct_expenses": "public/v2/direct-expenses/",
    "paper_checks": "public/v2/paper-checks/",
    # --- end AP & purchasing ---
}


# Ingestion-type metadata — primary keys and cursor fields per table. Kept as a
# module-level constant so it stays in sync with TABLE_SCHEMAS. Primary keys
# are per the OpenAPI list-item schemas and differ across streams.
TABLE_METADATA: dict[str, dict] = {
    # Snapshot tables (no updated_at cursor / filter on the wire).
    "accounts": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "customers": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "vendors": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "items": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    # CDC tables: server-side updated_after/updated_before filter plus a
    # required ``updated_at`` on every record. Primary keys differ per stream.
    "journal_entries": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "invoices": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "bills": {
        "primary_keys": ["number"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    # AR transaction streams (batch 1) — all CDC, keyed on internal_id.
    "sales_orders": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_payments": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_credits": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_refunds": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_deposits": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_prepayments": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "customer_prepayment_applications": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "cash_sales": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    # --- AP & purchasing streams ---
    # CDC streams — all keyed on internal_id per their OpenAPI list-item
    # schemas (PublicPurchaseOrderV2ListSchemaOut, ...), with an updated_at
    # cursor.
    "purchase_orders": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vendor_payments": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vendor_credits": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vendor_refunds": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vendor_prepayments": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vendor_prepayment_applications": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "direct_expenses": {
        "primary_keys": ["internal_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    # Snapshot stream — PublicPaperCheckSchemaOut is keyed on ``id`` and
    # carries no ``updated_at``, so it is re-listed in full each trigger.
    "paper_checks": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    # --- end AP & purchasing ---
}


def _build_schemas() -> dict[str, StructType]:
    """Return the static schema dictionary.

    Field names are kept exactly as the API returns them (snake_case) so raw
    records map 1:1 onto the DualEntry OpenAPI list-item schemas. Monetary
    quantities (``amount``, ``amount_due``, ``paid_total``, ``exchange_rate``)
    are strings on the wire and are typed ``StringType`` to preserve precision.
    ``*_at`` fields are ``TimestampType``; bare date fields are ``DateType``.
    Genuinely polymorphic / untyped payloads are ``VariantType``. See
    ``dualentry_api_doc.md`` for the extraction notes.
    """

    audit_actor = StructType(
        [
            StructField("actor_type", StringType()),
            StructField("email", StringType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    # AddressSchemaOut (invoices billing/shipping addresses).
    address_out = StructType(
        [
            StructField("name", StringType()),
            StructField("street", StringType()),
            StructField("second_line", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("postal_code", StringType()),
            StructField("country", StringType()),
        ]
    )
    # AddressSchemaIn (customer full/shipping addresses).
    address_in = StructType(
        [
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("postal_code", StringType()),
            StructField("country", StringType()),
            StructField("second_line", StringType()),
            StructField("name", StringType()),
        ]
    )
    # PublicVendorListAddressSchemaOut.
    vendor_address = StructType(
        [
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("postal_code", StringType()),
            StructField("country", StringType()),
            StructField("second_line", StringType()),
        ]
    )
    attachment = StructType(
        [
            StructField("id", LongType()),
            StructField("file_name", StringType()),
            StructField("file_size", LongType()),
            StructField("download_url", StringType()),
        ]
    )
    payment = StructType(
        [
            StructField("type", StringType()),
            StructField("id", LongType()),
            StructField("body", StringType()),
            StructField("link", StringType()),
        ]
    )
    rejected_by = StructType(
        [
            StructField("id", LongType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("full_name", StringType()),
            StructField("rejection_reason", StringType()),
        ]
    )
    # next_approvers[] — ApproverDictSchema.
    approver = StructType(
        [
            StructField("id", LongType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("full_name", StringType()),
            StructField("avatar_url", StringType()),
        ]
    )
    # custom_fields[] — CustomFieldValuePairOutputSchema = {field, value}.
    # ``field`` is the definition (CustomFieldSchemaOut); ``value`` is the
    # instance value (CustomFieldValueSchemaOut). Their untyped leaves
    # (``value.value``, ``company_ids``, ``options``, ``default_value``) are
    # ``VariantType`` — the DualEntry API leaves them user-defined.
    custom_field_definition = StructType(
        [
            StructField("id", LongType()),
            StructField("company_id", LongType()),
            StructField("company_ids", VariantType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("helper_text", StringType()),
            StructField("field_type", StringType()),
            StructField(
                "applies_to",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType()),
                            StructField("is_active", BooleanType()),
                            StructField("is_required", BooleanType()),
                        ]
                    )
                ),
            ),
            StructField("default_value", VariantType()),
            StructField("options", VariantType()),
            StructField("is_active", BooleanType()),
        ]
    )
    custom_field_value = StructType(
        [
            StructField("id", LongType()),
            StructField("custom_field_id", LongType()),
            StructField("custom_field_name", StringType()),
            StructField("custom_field_type", StringType()),
            StructField("custom_field_value_id", LongType()),
            StructField("value", VariantType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ]
    )
    custom_fields = ArrayType(
        StructType(
            [
                StructField("field", custom_field_definition),
                StructField("value", custom_field_value),
            ]
        )
    )
    # TaxRegistrationNumbersSnapshot = {company: [entry], counterparty: [entry]}.
    tax_reg_entry = StructType(
        [
            StructField("type", StringType()),
            StructField("number", StringType()),
            StructField("region", StringType()),
        ]
    )
    tax_registration_numbers = StructType(
        [
            StructField("company", ArrayType(tax_reg_entry)),
            StructField("counterparty", ArrayType(tax_reg_entry)),
        ]
    )
    # RecordTaxContextOut = {regime, data}. ``data`` is a documented union of
    # per-regime tax-data objects (sales_tax / vat / gst / none) — modelled as
    # VariantType so any member is preserved raw.
    bill_tax = StructType(
        [
            StructField("regime", StringType()),
            StructField("data", VariantType()),
        ]
    )
    # PublicRecordClassificationsSchemaOut — segment/dimension tags on AR
    # transactions (customer_payments, customer_refunds, cash_sales, ...).
    classification = StructType(
        [
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("line_id", LongType()),
            StructField("line_name", StringType()),
            StructField("parent_classification_id", LongType()),
            StructField("parent_classification_line_id", LongType()),
        ]
    )
    # CustomerDepositDataPublicSchema — deposits linked to a payment /
    # prepayment. ``date`` is a bare string (no date format) and ``amount`` is a
    # JSON number here (unlike the string monetary fields elsewhere).
    customer_deposit_data = StructType(
        [
            StructField("number", LongType()),
            StructField("date", StringType()),
            StructField("amount", DoubleType()),
            StructField("memo", StringType()),
            StructField("record_status", StringType()),
        ]
    )
    # IntegrationRemoteRecordSchemaOut — external-system provenance on
    # customer_credits.
    integration_remote_record = StructType(
        [
            StructField("id", LongType()),
            StructField("source_id", StringType()),
            StructField("transactional_date", StringType()),
            StructField("connection_id", LongType()),
            StructField("integration_id", LongType()),
            StructField("url", StringType()),
            StructField(
                "external_source",
                StructType(
                    [
                        StructField("integration_type", StringType()),
                        StructField("external_integration_url", StringType()),
                    ]
                ),
            ),
            StructField(
                "integration_provider",
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                        StructField("integration_type", StringType()),
                        StructField("logo_url", StringType()),
                        StructField("website", StringType()),
                        StructField("description", StringType()),
                        StructField("category", StringType()),
                    ]
                ),
            ),
        ]
    )

    return {
        "accounts": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("number", LongType()),
                StructField("description", StringType()),
                StructField("is_active", BooleanType()),
                StructField("is_system", BooleanType()),
                StructField("company_id", LongType()),
                StructField("parent_account_id", LongType()),
                StructField("account_type", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("category_1099", StringType()),
                StructField("system_ref", StringType()),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
            ]
        ),
        "journal_entries": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("transaction_id", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("memo", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("reversal_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("amount", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("record_status", StringType()),
                StructField("approval_status", StringType()),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "invoices": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("code", StringType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("amount", StringType()),
                StructField("amount_due", StringType()),
                StructField("paid_total", StringType()),
                StructField("amount_due_updated_at", TimestampType()),
                StructField("memo", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("due_date", DateType()),
                StructField("record_status", StringType()),
                StructField("approval_status", StringType()),
                StructField("recurring_record_number", LongType()),
                StructField("reference_number", StringType()),
                StructField("term_id", LongType()),
                StructField("term_name", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("bill_to_address", StringType()),
                StructField("ship_to_address", StringType()),
                StructField("billing_address", address_out),
                StructField("shipping_address", address_out),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("payment", payment),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "bills": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("memo", StringType()),
                StructField("amount", StringType()),
                StructField("amount_due", StringType()),
                StructField("paid_total", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("due_date", DateType()),
                StructField("supply_date", DateType()),
                StructField("place_of_supply", StringType()),
                StructField("record_status", StringType()),
                StructField("approval_status", StringType()),
                StructField("recurring_record_number", LongType()),
                StructField("purchase_order_number", LongType()),
                StructField("reference_number", StringType()),
                StructField("term_id", LongType()),
                StructField("term_name", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("tax", bill_tax),
                StructField("tax_registration_numbers", tax_registration_numbers),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customers": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("unique_id", StringType()),
                StructField("email", StringType()),
                StructField("website", StringType()),
                StructField("customer_type", StringType()),
                StructField("is_active", BooleanType()),
                StructField("phone", StringType()),
                StructField("full_address", address_in),
                StructField("shipping_address", address_in),
                StructField("approval_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
            ]
        ),
        "vendors": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("vendor_type", StringType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("customer_account_number", StringType()),
                StructField("single_address", StringType()),
                StructField("country", StringType()),
                StructField("unique_id", StringType()),
                StructField("website", StringType()),
                StructField("email", StringType()),
                StructField("is_active", BooleanType()),
                StructField("phone", StringType()),
                StructField("is_1099_eligible", BooleanType()),
                StructField("tin", StringType()),
                StructField("tin_type", StringType()),
                StructField("record_status", StringType()),
                StructField("address", vendor_address),
                StructField("approval_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
            ]
        ),
        "items": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("sku", StringType()),
                StructField("description", StringType()),
                StructField("item_type", StringType()),
                StructField("is_active", BooleanType()),
                StructField("record_status", StringType()),
                StructField("product_tax_code_id", LongType()),
                StructField("expense_account_id", LongType()),
                StructField("income_account_id", LongType()),
                StructField("deferral_account_id", LongType()),
                StructField("asset_account_id", LongType()),
                StructField("discount_account_id", LongType()),
                StructField("default_classification_line_ids", ArrayType(LongType())),
                StructField("default_rev_rec_method", StringType()),
                StructField("default_rev_rec_invoice_frequency", StringType()),
                StructField("default_rev_rec_invoice_interval", LongType()),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
            ]
        ),
        # ---------- AR transaction streams (batch 1) ----------
        "sales_orders": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("amount", StringType()),
                StructField("invoiced_total", StringType()),
                StructField("due_total", StringType()),
                StructField("invoiced_status", StringType()),
                StructField("memo", StringType()),
                StructField("date", DateType()),
                StructField("record_status", StringType()),
                StructField("recurring_record_number", LongType()),
                StructField("reference_number", StringType()),
                StructField("term_id", LongType()),
                StructField("term_name", StringType()),
                StructField("bill_to_address", StringType()),
                StructField("ship_to_address", StringType()),
                StructField("billing_address", address_out),
                StructField("shipping_address", address_out),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("approval_status", StringType()),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_payments": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("account_number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("undeposited_funds_currency", StringType()),
                StructField("bank_or_undeposited_account_amount", StringType()),
                StructField("amount", StringType()),
                StructField("unassigned_amount", StringType()),
                StructField("record_status", StringType()),
                StructField("paper_check_status", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("customer_deposits", ArrayType(customer_deposit_data)),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_credits": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("invoice_number", LongType()),
                StructField("cash_sale_number", LongType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("amount", StringType()),
                StructField("used_amount", StringType()),
                StructField("remaining_amount", StringType()),
                StructField("memo", StringType()),
                StructField("record_status", StringType()),
                StructField("contract_id", LongType()),
                StructField("contracted", BooleanType()),
                StructField("bill_to_address", StringType()),
                StructField("ship_to_address", StringType()),
                StructField("billing_address", address_out),
                StructField("shipping_address", address_out),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("attachments", ArrayType(attachment)),
                StructField("custom_fields", custom_fields),
                StructField(
                    "integration_remote_records",
                    ArrayType(integration_remote_record),
                ),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_refunds": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("account_number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("bank_or_credit_card_amount", StringType()),
                StructField("amount", StringType()),
                StructField("record_status", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("paper_check_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_deposits": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("transaction_id", LongType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("account_id", LongType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("amount", StringType()),
                StructField("memo", StringType()),
                StructField("record_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_prepayments": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("account_number", LongType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("exchange_rate", StringType()),
                StructField("undeposited_funds_currency", StringType()),
                StructField("bank_or_undeposited_funds_amount", StringType()),
                StructField("amount", StringType()),
                StructField("used_amount", StringType()),
                StructField("remaining_amount", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("record_status", StringType()),
                StructField("recurring_record_number", LongType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("attachments", ArrayType(attachment)),
                StructField("custom_fields", custom_fields),
                StructField("customer_deposits", ArrayType(customer_deposit_data)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "customer_prepayment_applications": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("customer_prepayment_number", LongType()),
                StructField("customer_credit_number", LongType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("memo", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("amount", StringType()),
                StructField("record_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "cash_sales": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("account_number", LongType()),
                StructField("amount", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("undeposited_funds_currency", StringType()),
                StructField("bank_or_undeposited_account_amount", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("record_status", StringType()),
                StructField("bill_to_address", StringType()),
                StructField("ship_to_address", StringType()),
                StructField("billing_address", address_out),
                StructField("shipping_address", address_out),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        # --- AP & purchasing streams ---
        # ``tax`` (RecordTaxContextOut) and ``tax_registration_numbers``
        # (TaxRegistrationNumbersSnapshot) reuse the ``bill_tax`` /
        # ``tax_registration_numbers`` helpers defined above for bills. All
        # nested actor / attachment / approver / classification shapes reuse the
        # existing shared helpers.
        "purchase_orders": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("date", DateType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("billed_total", StringType()),
                StructField("due_total", StringType()),
                StructField("amount", StringType()),
                StructField("memo", StringType()),
                StructField("record_status", StringType()),
                StructField("billed_status", StringType()),
                StructField("reference_number", StringType()),
                StructField("term_id", LongType()),
                StructField("term_name", StringType()),
                StructField("approval_status", StringType()),
                StructField("attachments", ArrayType(attachment)),
                StructField("next_approvers", ArrayType(approver)),
                StructField("rejected_by", rejected_by),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "vendor_payments": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("account_number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("bank_or_credit_card_amount", StringType()),
                StructField("amount", StringType()),
                StructField("record_status", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("paper_check_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "vendor_credits": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("bill_number", LongType()),
                StructField("direct_expense_number", LongType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("memo", StringType()),
                StructField("amount", StringType()),
                StructField("used_amount", StringType()),
                StructField("remaining_amount", StringType()),
                StructField("record_status", StringType()),
                StructField("supply_date", DateType()),
                StructField("place_of_supply", StringType()),
                StructField("tax", bill_tax),
                StructField("tax_registration_numbers", tax_registration_numbers),
                StructField("classifications", ArrayType(classification)),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "vendor_refunds": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("account_number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("bank_or_credit_card_amount", StringType()),
                StructField("amount", StringType()),
                StructField("record_status", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("paper_check_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("classifications", ArrayType(classification)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "vendor_prepayments": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("account_number", LongType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("check_number", StringType()),
                StructField("amount", StringType()),
                StructField("used_amount", StringType()),
                StructField("remaining_amount", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("bank_or_undeposited_funds_amount", StringType()),
                StructField("memo", StringType()),
                StructField("record_status", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("paper_check_status", StringType()),
                StructField("recurring_record_id", LongType()),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "vendor_prepayment_applications": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("vendor_prepayment_number", LongType()),
                StructField("vendor_credit_number", LongType()),
                StructField("exchange_rate", StringType()),
                StructField("memo", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("amount", StringType()),
                StructField("record_status", StringType()),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("attachments", ArrayType(attachment)),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "direct_expenses": StructType(
            [
                StructField("internal_id", LongType()),
                StructField("number", LongType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("company_currency", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("account_number", LongType()),
                StructField("amount", StringType()),
                StructField("memo", StringType()),
                StructField("check_number", StringType()),
                StructField("date", DateType()),
                StructField("transaction_date", DateType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("exchange_rate", StringType()),
                StructField("bank_or_credit_card_amount", StringType()),
                StructField("payment_method_id", LongType()),
                StructField("record_status", StringType()),
                StructField("supply_date", DateType()),
                StructField("place_of_supply", StringType()),
                StructField("tax", bill_tax),
                StructField("tax_registration_numbers", tax_registration_numbers),
                StructField("classifications", ArrayType(classification)),
                StructField("transaction_ids", ArrayType(LongType())),
                StructField("bank_match_status", StringType()),
                StructField("reconciliation_status", StringType()),
                StructField("custom_fields", custom_fields),
                StructField("attachments", ArrayType(attachment)),
                StructField("created_by", audit_actor),
                StructField("updated_by", audit_actor),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        ),
        "paper_checks": StructType(
            [
                StructField("id", LongType()),
                StructField("created_at", TimestampType()),
                StructField("transaction_date", DateType()),
                StructField("source_record_type", StringType()),
                StructField("source_record_id", LongType()),
                StructField("source_record_number", LongType()),
                StructField("check_number", StringType()),
                StructField("company_id", LongType()),
                StructField("company_name", StringType()),
                StructField("memo", StringType()),
                StructField("amount", StringType()),
                StructField("currency_iso_4217_code", StringType()),
                StructField("account_number", LongType()),
                StructField("account_name", StringType()),
                StructField("payee_name", StringType()),
                StructField("customer_id", LongType()),
                StructField("customer_name", StringType()),
                StructField("vendor_id", LongType()),
                StructField("vendor_name", StringType()),
                StructField("record_status", StringType()),
            ]
        ),
        # --- end AP & purchasing ---
    }


TABLE_SCHEMAS = _build_schemas()


class DualEntryLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the DualEntry public REST API."""

    # ------------------------------------------------------------------
    # Construction & helpers
    # ------------------------------------------------------------------

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError(
                "DualEntry connector requires 'api_key' option (a DualEntry "
                "public API key sent in the X-API-KEY header)."
            )

        base_url = options.get("base_url") or DEFAULT_BASE_URL
        self._root = base_url.rstrip("/") + "/"

        # The OpenAPI security scheme is an apiKey header literally named
        # ``X-API-KEY`` carrying the raw key — no ``Bearer`` prefix.
        self._headers = {
            "X-API-KEY": api_key,
            "Accept": "application/json",
        }

        # Cap cursors at init time so Trigger.AvailableNow always terminates.
        # Parse the formatted string back so ``_init_dt`` carries the same
        # millisecond precision as stored cursors (which come from
        # ``_format_ts``); otherwise sub-ms microseconds would make the
        # caught-up fast-path in ``_read_incremental`` never compare equal.
        self._init_ts_iso = _format_ts(datetime.now(timezone.utc))
        self._init_dt = _parse_ts(self._init_ts_iso)

    def _url(self, path: str) -> str:
        """Join *path* (no leading slash) onto the API root."""
        return urljoin(self._root, path.lstrip("/"))

    def _request(self, path: str, params: dict | None = None) -> requests.Response:
        """GET *path* with retry on 429/5xx; honour ``Retry-After``.

        DualEntry 429s carry a ``Retry-After`` header (seconds); waiting the
        advertised time (not just our own backoff) matters because the 429
        counts against the token bucket. Every request carries an explicit
        timeout so the connector cannot hang on a stuck socket; transport-level
        ``RequestException`` errors propagate so the framework retries the
        microbatch.
        """
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            resp = requests.get(
                self._url(path),
                headers=self._headers,
                params=params or {},
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            sleep_s = backoff
            retry_after = _retry_after_seconds(resp)
            if retry_after is not None:
                sleep_s = max(sleep_s, retry_after)
            logger.warning(
                "DualEntry %s returned %s; retrying in %.1fs (attempt %d/%d)",
                path,
                resp.status_code,
                sleep_s,
                attempt + 1,
                MAX_RETRIES,
            )
            if attempt < MAX_RETRIES - 1:
                _sleep(sleep_s)
                backoff *= 2

        assert resp is not None  # for type-checkers; always set inside the loop
        return resp

    def _get_json(self, path: str, params: dict | None = None):
        """Issue a GET, raise on non-2xx, return the decoded JSON body."""
        resp = self._request(path, params=params)
        if resp.status_code // 100 != 2:
            raise RuntimeError(
                f"DualEntry API GET {path} failed with HTTP {resp.status_code}: "
                f"{resp.text[:500]}"
            )
        return resp.json()

    @staticmethod
    def _unwrap_records(body) -> list:
        """Return the records list from a DualEntry response body.

        List responses wrap the record array in a top-level ``items`` key
        alongside a ``count`` field. Tolerate a bare array in case the envelope
        ever changes; return ``[]`` for anything else so callers can iterate
        without guarding.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            inner = body.get("items")
            if isinstance(inner, list):
                return inner
        return []

    # ------------------------------------------------------------------
    # LakeflowConnect API
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        # DualEntry has no discovery endpoint; tables are statically known.
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        # Return a fresh copy so callers cannot mutate the module-level dict.
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if TABLE_METADATA[table_name]["ingestion_type"] == "snapshot":
            return self._read_snapshot(table_name, start_offset, table_options)
        return self._read_incremental(table_name, start_offset, table_options)

    # ------------------------------------------------------------------
    # Snapshot reads (accounts / customers / vendors / items)
    # ------------------------------------------------------------------

    def _read_snapshot(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read for snapshot tables.

        Returns ``{"done": True}`` after the first call so subsequent calls
        within the same Trigger.AvailableNow trigger short-circuit (per the
        ``end_offset == start_offset`` termination contract). Records are yielded
        raw, exactly as the API returns them.
        """
        if start_offset and start_offset.get("done"):
            return iter([]), start_offset

        path = TABLE_ENDPOINTS[table_name]
        limit = _page_size(table_options)

        def generate() -> Iterator[dict]:
            row_offset = 0
            while True:
                params = {
                    "limit": str(limit),
                    "offset": str(row_offset),
                    "ordering": "id",
                }
                records = self._unwrap_records(self._get_json(path, params=params))
                yield from records
                if len(records) < limit:
                    return
                row_offset += limit

        return generate(), {"done": True}

    # ------------------------------------------------------------------
    # Incremental reads (journal_entries / invoices / bills)
    # ------------------------------------------------------------------

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Bounded ``updated_at``-range read with offset continuation.

        Offset shapes:
          ``{"cursor": <iso>}``                        — caught-up steady state.
          ``{"since": ..., "until": ..., "offset": N}`` — mid-range, produced
              only when ``max_records_per_batch`` split a range across
              microbatches. ``since``/``until`` stay pinned so the row offset
              stays stable for the rest of the range.

        A new range spans ``[cursor - lookback_seconds, _init_ts]``; when a page
        returns fewer than ``limit`` records the range is drained and the cursor
        advances to the range's upper bound. The very first sync has no lower
        bound (full backfill) unless ``start_timestamp`` is supplied. Records
        are yielded raw, exactly as the API returns them.
        """
        offset = dict(start_offset or {})
        limit = _page_size(table_options)
        max_records = int(table_options.get("max_records_per_batch", str(sys.maxsize)))

        if offset.get("until"):
            # Resume a partially-drained range with its bounds pinned.
            since = offset.get("since")
            until = offset["until"]
            row_offset = int(offset.get("offset", 0))
        else:
            cursor = offset.get("cursor")
            if cursor and _parse_ts(cursor) >= self._init_dt:
                # Caught up to init time — short-circuit so the trigger
                # terminates (end_offset == start_offset contract).
                return iter([]), start_offset
            until = self._init_ts_iso
            row_offset = 0
            if cursor:
                lookback = max(
                    0,
                    int(table_options.get("lookback_seconds", str(DEFAULT_LOOKBACK_SECONDS))),
                )
                since = _format_ts(_parse_ts(cursor) - timedelta(seconds=lookback))
            else:
                since = table_options.get("start_timestamp")

        path = TABLE_ENDPOINTS[table_name]
        params: dict[str, str] = {
            "updated_before": until,
            "limit": str(limit),
            "ordering": "updated_at",
        }
        if since:
            params["updated_after"] = since

        if max_records >= sys.maxsize:
            # Unbounded (the default): stream pages lazily so driver memory
            # tracks a single page, not the whole range. A short page drains
            # the range; the cursor then advances to its upper bound. Because
            # the range always fully drains here, the end offset is known up
            # front (like the snapshot path), so no accumulation is needed.
            def generate() -> Iterator[dict]:
                off = row_offset
                while True:
                    page_records = self._unwrap_records(
                        self._get_json(path, params={**params, "offset": str(off)})
                    )
                    yield from page_records
                    if len(page_records) < limit:
                        return
                    off += limit

            return generate(), {"cursor": until}

        # Bounded by ``max_records_per_batch``: accumulate up to the cap (memory
        # stays bounded by the cap) so the split offset can be computed before
        # the tuple is returned.
        records: list[dict] = []
        off = row_offset
        while True:
            page_records = self._unwrap_records(
                self._get_json(path, params={**params, "offset": str(off)})
            )
            records.extend(page_records)
            if len(page_records) < limit:
                # Range drained — advance the cursor to its upper bound.
                return iter(records), {"cursor": until}
            off += limit
            if len(records) >= max_records:
                # Split the range across microbatches; the cap applies at page
                # granularity so resuming at ``off`` never skips the tail of a
                # partially-emitted page.
                next_offset: dict = {"until": until, "offset": off}
                if since:
                    next_offset["since"] = since
                return iter(records), next_offset

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {sorted(TABLE_SCHEMAS)}"
            )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _sleep(seconds: float) -> None:
    """Indirection over ``time.sleep`` so retries are trivial to stub in tests."""
    import time

    time.sleep(seconds)


def _retry_after_seconds(resp: requests.Response) -> float | None:
    """Return the ``Retry-After`` wait (seconds) advertised on a response.

    DualEntry 429s carry a numeric ``Retry-After`` header; 5xx responses may
    carry one too. Returns ``None`` when the header is absent or unparseable so
    the caller falls back to exponential backoff.
    """
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _page_size(table_options: dict[str, str]) -> int:
    """Resolve the ``limit`` page-size option, clamped to the server max."""
    return max(1, min(int(table_options.get("limit", str(DEFAULT_PAGE_SIZE))), MAX_PAGE_SIZE))


def _format_ts(dt: datetime) -> str:
    """Format a datetime as the ISO-8601 UTC shape DualEntry uses (ms + Z)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string (Z or offset) into an aware datetime."""
    if not value:
        raise ValueError("empty timestamp string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
