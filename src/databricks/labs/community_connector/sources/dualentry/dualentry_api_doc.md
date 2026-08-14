# **DualEntry API Documentation**

DualEntry is a cloud accounting / ERP platform. Its public REST API lives at
`https://api.dualentry.com` (production) with a development host at
`https://api-dev.dualentry.com`. The machine-readable contract used to build
this connector is the public OpenAPI 3.1 document
`https://docs.dualentry.com/developers/openapi/resources-v2.json`. All V2
resource collections are served under `/public/v2/`.

## **Authorization**

- **Single supported method: static API key.**
  The key is sent in an HTTP header literally named `X-API-KEY` carrying the
  **raw** key — there is **no** `Bearer` prefix.
- Spec connection parameter: `api_key` (required, secret). An optional
  `base_url` overrides the host (defaults to `https://api.dualentry.com`);
  point it at `https://api-dev.dualentry.com` to test against the dev
  environment.
- There is no OAuth flow.

Example request:

```http
GET /public/v2/accounts/?limit=100&offset=0 HTTP/1.1
Host: api.dualentry.com
X-API-KEY: <API_KEY>
Accept: application/json
```

## **Pagination**

Every list endpoint uses `limit` + `offset` query parameters:

- `limit` — page size. **Default and maximum are both 100.** The connector
  requests `limit=100` by default.
- `offset` — zero-based row offset (default `0`).

List responses wrap the record array in a top-level `items` key alongside a
`count` field:

```json
{ "items": [ { ... }, { ... } ], "count": 1234 }
```

The connector detects the last page by receiving **fewer than `limit`**
records (or an empty page) — it does not rely on `count`.

## **Incremental reads**

Three resources are change-data-capture (CDC) streams. Their list endpoints
accept `updated_after` / `updated_before` query parameters — inclusive
lower/upper bounds that map to `updated_at__gte` / `updated_at__lte` on the
record's `updated_at`. Every incremental record carries a **required**
top-level `updated_at` (plus `created_at`), both ISO-8601 date-time strings.

Connector strategy (mirrors the repo's `remberg` connector):

- The cursor is `updated_at`. Each trigger reads the bounded range
  `[cursor - lookback_seconds, _init_ts]`, paginating with `offset` within
  the range. When a page comes back short the range is drained and the cursor
  advances to the range's upper bound.
- The upper bound is pinned at `_init_ts` — an ISO timestamp captured once in
  `__init__` — so `Trigger.AvailableNow` always terminates (records created
  after the trigger started are excluded until the next trigger).
- `lookback_seconds` (default 300) is applied at read time only, never
  stored, to re-capture records whose `updated_at` moved past the range's
  upper bound while it was being paged. Upserts on the primary key make the
  overlap harmless.
- The very first sync has no lower bound (full backfill) unless
  `start_timestamp` is supplied.
- `ordering=updated_at` is sent so offset pages are stable on the live API.

The other four resources have **no** `updated_after`/`updated_before` filter
and their records carry no `updated_at` field, so they are treated as
snapshots (full re-list each trigger, upserted on the primary key).
`ordering=id` is sent to keep their offset pages stable.

## **Object list & per-stream facts**

First-slice: 7 streams, all under `/public/v2/`. Primary keys and cursors are
taken from the OpenAPI list-item schemas (they are **not** uniformly `id`).

| Table | Endpoint | Item schema | Primary key | Ingestion | Cursor | Timestamp fields |
|---|---|---|---|---|---|---|
| `accounts` | `GET /public/v2/accounts/` | `PublicAccountSchemaOut` | `id` (int) | snapshot | — | — |
| `journal_entries` | `GET /public/v2/journal-entries/` | `PublicJournalEntryV2ListSchemaOut` | `internal_id` (int) | cdc | `updated_at` | `created_at`, `updated_at`, `date`, `transaction_date`, `reversal_date` |
| `invoices` | `GET /public/v2/invoices/` | `PublicInvoiceV2ListSchemaOut` | `internal_id` (int) | cdc | `updated_at` | `created_at`, `updated_at`, `amount_due_updated_at`, `date`, `transaction_date`, `due_date` |
| `bills` | `GET /public/v2/bills/` | `PublicBillV2ListSchemaOut` | `number` (int) | cdc | `updated_at` | `created_at`, `updated_at`, `date`, `transaction_date`, `due_date`, `supply_date` |
| `customers` | `GET /public/v2/customers/` | `CustomerListSchemaOut` | `id` (int) | snapshot | — | — |
| `vendors` | `GET /public/v2/vendors/` | `PublicVendorListSchemaOut` | `id` (int) | snapshot | — | — |
| `items` | `GET /public/v2/items/` | `PublicItemSchemaOut` | `id` (int) | snapshot | — | — |

Notes on primary keys (verified against the spec — these differ per stream):

- `journal_entries` and `invoices` are keyed on `internal_id`, not `id` (the
  list schemas expose `internal_id` as the stable identifier and have no
  bare `id`).
- `bills` is keyed on `number` (the list schema has no `id` either;
  `internal_id` and `number` are both required — `number` is the documented
  business key used across the product).
- `accounts`, `customers`, `vendors`, `items` are keyed on `id`.

## **Object schema**

Schemas are **static**, taken from the response DTOs referenced by each
`GET` operation's `200` response (`Paged*SchemaOut.items[]`). Field names are
kept exactly as the API returns them (snake_case). Monetary quantities
(`amount`, `amount_due`, `paid_total`, `exchange_rate`) are **strings** on the
wire and are typed `StringType` to preserve precision. `*_at` fields are
`TimestampType`; bare date fields (`date`, `due_date`, ...) are `DateType`.

A few fields are genuinely polymorphic / user-defined and are JSON-serialized
to `StringType` (or a `StringType` sub-field) by the connector so the column
type stays stable:

- `custom_fields[].field` and `custom_fields[].value` (user-defined custom
  fields; present on journal_entries, invoices, bills, customers, vendors).
- `bills.tax.data` (a discriminated union keyed by `bills.tax.regime`) and
  `bills.tax_registration_numbers` (nested company/counterparty arrays).
- `vendors.record_status` (declared as an untyped/`any` field in the spec).

Common nested structs (`created_by`/`updated_by` audit actors, addresses,
attachments, approvers, payment) are modelled as `StructType`.

## **Rate limits & retries**

DualEntry enforces a token-bucket rate limit; a `429` response carries a
`Retry-After` header (seconds). The connector retries on `429` and on
`500`/`502`/`503`/`504` with exponential backoff, honouring `Retry-After`
when present (taking the max of the advertised wait and the backoff). Every
request carries an explicit timeout so a stuck socket cannot hang a trigger.

## **Deferred objects (not in the first connector version)**

The API exposes more collections that are out of scope for the first slice —
e.g. `/public/v2/recurring/{invoices,bills,journal-entries}/`,
`/public/v2/intercompany-journal-entries/`, and the various `POST` create
endpoints (this connector is read-only). They can be added later following
the same pattern.
