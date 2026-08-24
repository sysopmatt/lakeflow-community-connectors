# Lakeflow HubSpot Extended Community Connector

This documentation provides setup instructions and reference information for the HubSpot Extended source connector.

HubSpot (https://www.hubspot.com) is a CRM, marketing, CMS and commerce
platform. This connector extends coverage well beyond core CRM objects,
ingesting CRM, engagements, commerce, marketing, CMS/content, lists,
conversations, behavioral events and aggregate analytics from the HubSpot
public API (`https://api.hubapi.com`). It authenticates with a HubSpot
**Private App** access token (Bearer auth) and reads 47 objects — a mix of
incremental (CDC) and full-refresh (snapshot) tables.

## Prerequisites

- A HubSpot account with a **Private App** you can create (requires the
  **Super Admin** role, or the **Developer tools access** permission).
- A HubSpot Private App **access token**. The connector sends it as an HTTP
  `Authorization: Bearer <token>` header — there is no OAuth client-id/secret
  flow and no legacy `hapikey` API key.
- The Private App must be granted the **read** scopes for the objects you want
  to ingest (see [How to obtain the access token](#how-to-obtain-the-access-token)).
  Some objects require a specific Hub tier (e.g. `tickets` needs Service Hub;
  `campaigns` and most marketing/analytics objects need Marketing Hub
  Pro/Enterprise).

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `access_token` | string | Yes | HubSpot Private App access token, sent as an `Authorization: Bearer` header. Store as a Databricks secret. | `pat-na1-…` |

This connector does not currently expose any table-specific source options, so
`externalOptionsAllowList` may be left empty. Incremental start is handled
automatically (first sync backfills; subsequent syncs read changes after the
stored cursor).

### How to obtain the access token

1. In HubSpot, go to **Settings → Integrations → Private Apps**.
2. Click **Create a private app** (needs Super Admin or "Developer tools
   access").
3. On the **Scopes** tab, grant the **read** scopes for the objects you want.
   As a guide:
   - CRM objects & engagements: `crm.objects.contacts.read`,
     `crm.objects.companies.read`, `crm.objects.deals.read`,
     `crm.objects.line_items.read`, `crm.objects.owners.read`,
     `crm.schemas.*.read`, `tickets` (Service Hub).
   - Commerce: `crm.objects.invoices.read`, `crm.objects.orders.read`,
     `crm.objects.carts.read`, `crm.objects.commercepayments.read`,
     `crm.objects.subscriptions.read`.
   - Marketing: `content`, `forms`, `marketing.campaigns.read`,
     `marketing-email` (Marketing Hub Pro/Enterprise for most of these).
   - CMS/content: `content`, `cms.knowledge_base.articles.read`.
   - Lists & subscriptions: `crm.lists.read`,
     `communication_preferences.read`.
   - Events, conversations & analytics: `behavioral_events.event_definitions.read_write`
     (events), `conversations.read`, `business-intelligence` (analytics).
4. Copy the generated token (shown once). Store it as a Databricks secret and
   reference it from the connection.

Only read scopes are needed — the connector never writes to HubSpot.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the Private App `access_token`. No `externalOptionsAllowList` entries
   are required.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

Column names are kept exactly as the HubSpot API returns them, so rows map 1:1
to the HubSpot resource schemas. Most objects are keyed on `id`, but some use a
different or composite key (taken from the endpoint's payload). No object
exposes a deletions feed, so hard deletes do not propagate — deleted records
stop appearing in the source and remain in the destination.

**CRM core objects & engagements** — HubSpot CRM v3 (`/crm/v3/objects/*`),
cursor pagination, incremental on the object `updatedAt`:

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `contacts` | CDC (incremental) | `id` | `updatedAt` |
| `companies` | CDC (incremental) | `id` | `updatedAt` |
| `deals` | CDC (incremental) | `id` | `updatedAt` |
| `tickets` | CDC (incremental) | `id` | `updatedAt` |
| `calls` | CDC (incremental) | `id` | `updatedAt` |
| `emails` | CDC (incremental) | `id` | `updatedAt` |
| `meetings` | CDC (incremental) | `id` | `updatedAt` |
| `tasks` | CDC (incremental) | `id` | `updatedAt` |
| `notes` | CDC (incremental) | `id` | `updatedAt` |
| `line_items` | CDC (incremental) | `id` | `updatedAt` |
| `products` | CDC (incremental) | `id` | `updatedAt` |
| `communications` | CDC (incremental) | `id` | `updatedAt` |
| `postal_mail` | CDC (incremental) | `id` | `updatedAt` |

**CRM extended & commerce** — CRM v3 objects, cursor pagination, incremental on
`updatedAt`:

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `leads` | CDC (incremental) | `id` | `updatedAt` |
| `quotes` | CDC (incremental) | `id` | `updatedAt` |
| `feedback_submissions` | CDC (incremental) | `id` | `updatedAt` |
| `appointments` | CDC (incremental) | `id` | `updatedAt` |
| `listings` | CDC (incremental) | `id` | `updatedAt` |
| `orders` | CDC (incremental) | `id` | `updatedAt` |
| `carts` | CDC (incremental) | `id` | `updatedAt` |
| `commerce_payments` | CDC (incremental) | `id` | `updatedAt` |
| `subscriptions` | CDC (incremental) | `id` | `updatedAt` |
| `invoices` | CDC (incremental) | `id` | `updatedAt` |

**CRM metadata** — schema/config objects, listed in full each run:

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `owners` | Snapshot (full refresh) | `id` | — |
| `pipelines` | Snapshot (full refresh) | `id` | — |
| `properties` | Snapshot (full refresh) | `objectType`, `name` | — |
| `crm_schemas` | Snapshot (full refresh) | `objectTypeId` | — |

`properties` fans out over contacts, companies, deals, tickets, products and
line items, injecting the parent `objectType` into each row so the composite
key stays unique.

**Marketing** — marketing APIs (`/marketing/v3/*`, plus the legacy email-events
feed):

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `marketing_emails` | CDC (incremental) | `id` | `updatedAt` |
| `marketing_events` | CDC (incremental) | `id` | `updatedAt` |
| `forms` | CDC (incremental) | `id` | `updatedAt` |
| `campaigns` | CDC (incremental) | `id` | `updatedAt` |
| `email_events` | CDC (incremental) | `id` | `created` |
| `form_submissions` | CDC (incremental) | `form_id`, `submittedAt`, `contact_id` | `submittedAt` |

`email_events` uses HubSpot's legacy offset/`hasMore` feed
(`/email/public/v1/events`) so recipient-level send/open/click/bounce rows are
preserved (the v3 statistics endpoint is aggregate-only). `form_submissions`
fans out by first listing forms, then reading each form's submissions endpoint,
injecting `form_id` into each row.

**CMS & content** — CMS v3 (`/cms/v3/*`):

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `blog_posts` | CDC (incremental) | `id` | `updated` |
| `blog_tags` | CDC (incremental) | `id` | `updated` |
| `blog_authors` | CDC (incremental) | `id` | `updated` |
| `landing_pages` | CDC (incremental) | `id` | `updatedAt` |
| `site_pages` | CDC (incremental) | `id` | `updatedAt` |
| `hubdb_tables` | Snapshot (full refresh) | `id` | — |
| `url_redirects` | Snapshot (full refresh) | `id` | — |

**Lists & subscriptions**:

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `lists` | Snapshot (full refresh) | `listId` | — |
| `subscription_definitions` | Snapshot (full refresh) | `id` | — |

**Events, conversations & analytics**:

| Object | Ingestion | Primary key | Cursor |
|---|---|---|---|
| `behavioral_events` | CDC (incremental) | `id` | `occurredAt` |
| `conversation_threads` | CDC (incremental) | `id` | `latestMessageTimestamp` |
| `conversation_messages` | CDC (incremental) | `thread_id`, `id` | `createdAt` |
| `conversation_inboxes` | Snapshot (full refresh) | `id` | — |
| `analytics_views` | Snapshot (full refresh) | `breakdown`, `period` | — |

`conversation_messages` fans out by first listing conversation threads, then
reading each thread's messages endpoint, injecting `thread_id`.
`analytics_views` are aggregate report snapshots keyed by breakdown and period —
HubSpot does not expose row-level traffic analytics through these report
endpoints, so these tables are snapshots rather than true CDC streams.

Incremental strategy: CDC tables read changes after the stored cursor up to the
trigger's start time, page by page until drained (the first sync is a full
backfill). For the v3-cursor tables, a server-side updated-since filter is used
as an efficiency hint where documented, with an unconditional client-side
cursor check as the correctness guard. Snapshot tables are re-listed in full
each run and upserted on their primary key.

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

This connector currently exposes no source-specific `table_configuration`
options. Incremental behavior (cursor tracking, first-sync backfill) is handled
automatically per table.

## Data Type Mapping

| HubSpot value | Databricks type |
|---|---|
| `id` and reference ids | `STRING` |
| `properties` map (per-object custom + default properties) | `STRUCT` |
| `createdAt` / `updatedAt` / other ISO-8601 date-time fields | `TIMESTAMP` |
| enum / free-text string | `STRING` |
| numeric string (HubSpot returns most numeric properties as strings) | `STRING` (preserves precision) |
| `archived` and other booleans | `BOOLEAN` |
| nested object | `STRUCT` |
| array | `ARRAY` of the mapped element type |
| user-defined / polymorphic fields | JSON-serialized `STRING` |

No column uses the `VARIANT` type: polymorphic leaves are JSON-serialized to
`STRING` so snapshot tables remain compatible with SCD upsert (`<=>` change
detection).

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Optionally set the table-specific options described above, e.g.:

```json
{
  "pipeline_spec": {
      "connection_name": "hubspot_extended_connection",
      "object": [
        {
            "table": {
                "source_table": "contacts"
            }
        },
        {
            "table": {
                "source_table": "deals",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1"
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

- **Start Small**: Begin by syncing a subset of objects (e.g. `contacts`,
  `companies`, `deals`) to test your pipeline before adding the full set.
- **Mind Hub tiers**: Objects gated by subscription (`tickets`, `campaigns`,
  most marketing/analytics tables) return HTTP `402`/`403` if your portal lacks
  the module. Deploy the objects you have access to; add gated ones once the
  relevant Hub is enabled.
- **Use Incremental Sync**: CDC tables only fetch changes after the first
  backfill — prefer them over re-snapshotting where possible.
- **Respect Rate Limits**: HubSpot enforces per-app rate limits and returns
  `429` with `Retry-After`. The connector backs off automatically; avoid
  running many concurrent pipelines against the same Private App token.

#### Troubleshooting

**Common Issues:**

- **HTTP 401**: the access token is missing or malformed. Confirm it is sent as
  `Authorization: Bearer <token>` and that the Private App is active.
- **HTTP 403**: the Private App lacks the read scope for the requested object.
  Add the scope in the app's Scopes tab and re-copy the token.
- **HTTP 402**: the object requires a higher HubSpot subscription (e.g.
  Marketing Hub Pro/Enterprise, Service Hub). This is a plan limit, not a token
  problem.
- **HTTP 429**: rate limiting. The connector backs off automatically; if it
  persists, reduce pipeline concurrency against the same token.
- **Deletes not reflected**: HubSpot exposes no deletions feed through these
  endpoints; deleted records stop appearing in the source and remain in the
  destination.

## References

- HubSpot API reference: https://developers.hubspot.com/docs/reference/api
- Private Apps: https://developers.hubspot.com/docs/guides/apps/private-apps/overview
- CRM v3 objects: https://developers.hubspot.com/docs/guides/api/crm/objects
