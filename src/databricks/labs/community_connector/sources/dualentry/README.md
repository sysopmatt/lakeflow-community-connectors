# Lakeflow DualEntry Community Connector

This documentation provides setup instructions and reference information for the DualEntry source connector.

DualEntry (https://dualentry.com) is a cloud accounting / ERP platform. The
connector ingests accounts, journal entries, invoices, bills, customers,
vendors and items from the DualEntry public REST API
(`https://api.dualentry.com`, all V2 resources under `/public/v2/`).

## Prerequisites

- A DualEntry account with access to the public API.
- A DualEntry API key. The key is sent by the connector in an HTTP header
  literally named `X-API-KEY` carrying the **raw** key — there is no `Bearer`
  prefix and no OAuth flow.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_key` | string | Yes | DualEntry public API key, sent in the `X-API-KEY` header. Store as a Databricks secret. | `de_live_…` |
| `base_url` | string | No | API root, no trailing slash. Defaults to `https://api.dualentry.com`. Point at `https://api-dev.dualentry.com` for the dev environment. | `https://api.dualentry.com` |

The connector supports extra table-specific options (see
[Table Configurations](#table-configurations)), so `externalOptionsAllowList`
is a **required** connection option. Set it to exactly:

```
start_timestamp,lookback_seconds,limit,max_records_per_batch
```

### How to obtain the API key

1. Sign in to DualEntry.
2. Create / copy a public API key from your DualEntry API settings.
3. Store it as a Databricks secret and reference it from the connection.

The key is sent in the `X-API-KEY` header as the raw key (no `Bearer` prefix) —
this is DualEntry's documented authentication scheme; no other method is
supported.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Include `start_timestamp,lookback_seconds,limit,max_records_per_batch` in
   the connection's `externalOptionsAllowList`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

Primary keys differ per object (they are **not** uniformly `id`), taken from
the DualEntry OpenAPI list-item schemas. No object exposes deleted records, so
deletes do not propagate (re-ingest with a fresh pipeline if you need
hard-delete reconciliation).

| Object | Endpoint | Ingestion | Primary key | Cursor |
|---|---|---|---|---|
| `accounts` | `/public/v2/accounts/` | Snapshot (full refresh) | `id` | — |
| `journal_entries` | `/public/v2/journal-entries/` | CDC (incremental) | `internal_id` | `updated_at` |
| `invoices` | `/public/v2/invoices/` | CDC (incremental) | `internal_id` | `updated_at` |
| `bills` | `/public/v2/bills/` | CDC (incremental) | `number` | `updated_at` |
| `customers` | `/public/v2/customers/` | Snapshot (full refresh) | `id` | — |
| `vendors` | `/public/v2/vendors/` | Snapshot (full refresh) | `id` | — |
| `items` | `/public/v2/items/` | Snapshot (full refresh) | `id` | — |

Incremental strategy: CDC tables are read as bounded `updated_at` ranges
(`updated_after` / `updated_before` server-side filters, inclusive) from the
stored cursor (minus a small read-time lookback) up to the trigger's start
time, `offset`-page by `offset`-page until drained. The first sync is a full
backfill unless `start_timestamp` is set. Snapshot tables are re-listed in full
each run and upserted on their primary key.

Special columns (JSON-serialized to `STRING` to keep the column type stable):
- `custom_fields[].field` and `custom_fields[].value` (user-defined; present on
  journal entries, invoices, bills, customers, vendors).
- `next_approvers` (an untyped approvers array).
- `bills.tax.data` (a discriminated union keyed by `bills.tax.regime`) and
  `bills.tax_registration_numbers` (nested company/counterparty arrays).

Column names are kept exactly as the DualEntry API returns them (snake_case),
so rows map 1:1 to the OpenAPI resource schemas.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Special `table_configuration` options

| Option | Applies to | Required | Description |
|---|---|---|---|
| `start_timestamp` | CDC tables | No | ISO-8601 UTC lower bound for the very first sync (e.g. `2024-01-01T00:00:00.000Z`). Default: unbounded full backfill. |
| `lookback_seconds` | CDC tables | No | Seconds subtracted from the cursor at read time to re-capture records updated while a range was being paginated. Default `300`. |
| `limit` | All tables | No | Page size for the DualEntry list endpoints. Default `100` (also the server maximum). |
| `max_records_per_batch` | CDC tables | No | Per-microbatch cap on emitted rows, applied at page granularity. Default: drain the whole range in one microbatch. |

## Data Type Mapping

| DualEntry (OpenAPI) type | Databricks type |
|---|---|
| `integer` (ids, numbers) | `BIGINT` |
| `string` (enums, currency codes, free text) | `STRING` |
| `string` monetary (`amount`, `amount_due`, `paid_total`, `exchange_rate`) | `STRING` (preserves precision) |
| `string, format: date` | `DATE` |
| `string, format: date-time` (`*_at`) | `TIMESTAMP` |
| `boolean` | `BOOLEAN` |
| nested object | `STRUCT` |
| array | `ARRAY` of the mapped element type |
| user-defined / polymorphic fields | JSON-serialized `STRING` |

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Optionally set the table-specific options described above, e.g.:

```json
{
  "pipeline_spec": {
      "connection_name": "dualentry_connection",
      "object": [
        {
            "table": {
                "source_table": "accounts"
            }
        },
        {
            "table": {
                "source_table": "journal_entries",
                "table_configuration": {
                    "start_timestamp": "2024-01-01T00:00:00.000Z",
                    "max_records_per_batch": "50000"
                }
            }
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline.
- **Use Incremental Sync**: The three CDC tables (journal entries, invoices,
  bills) only fetch changes after the first backfill — prefer them over
  re-snapshotting where possible.
- **Respect Rate Limits**: DualEntry enforces a token-bucket rate limit and
  returns `Retry-After` on 429s. The connector backs off automatically and
  honors `Retry-After`, but avoid running many concurrent pipelines against the
  same API key.

#### Troubleshooting

**Common Issues:**

- **HTTP 401/403**: the API key is wrong or lacks read rights on the requested
  object. Confirm it is sent as the raw `X-API-KEY` value (no `Bearer` prefix).
- **HTTP 429**: rate limiting. The connector backs off automatically; if it
  persists, reduce pipeline concurrency against the same API key.
- **Deletes not reflected**: DualEntry exposes no deletions feed; deleted
  records simply stop appearing in the source and remain in the destination.

## References

- Developer docs: https://docs.dualentry.com
- OpenAPI (V2 resources): https://docs.dualentry.com/developers/openapi/resources-v2.json
- Connector API research notes: [`dualentry_api_doc.md`](dualentry_api_doc.md)
