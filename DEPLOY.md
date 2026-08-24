# Deploying the DualEntry & HubSpot (Extended) Lakeflow Connectors

This repo branch (`polly/connectors-deploy`) contains **two Databricks Lakeflow
community connectors** plus ready-to-run deploy specs. Following this guide end to
end takes a fresh Mac to two live ingestion pipelines.

| Connector | Source name | Pipeline | Destination | Streams |
|---|---|---|---|---|
| DualEntry (accounting) | `dualentry` | `ingestion_dualentry` | `ingestion.dualentry` | 39 |
| HubSpot (extended) | `hubspot_extended` | `ingestion_hubspot` | `ingestion.hubspot` | 47 |

Each pipeline uses a **Unity Catalog connection** that holds the API credential, plus a
**pipeline spec** (`deploy/*_spec.yaml`) that lists which objects to ingest and where.
You never put a credential in a file — it lives in the UC connection.

---

## 0. What you need before starting
- A **Mac** (these steps are macOS/Homebrew; Linux is similar).
- Access to the **Databricks workspace** (`https://dbc-763a60c8-13b8.cloud.databricks.com`).
- A **DualEntry API key** (from a DualEntry org admin — header `X-API-KEY`).
- A **HubSpot Private App access token** (see §6b for how to create one). *Optional if you
  only want to deploy DualEntry first.*

---

## 1. Install Homebrew (skip if `brew --version` works)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# Then follow the "Next steps" it prints to add brew to your PATH, e.g.:
#   echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile && eval "$(/opt/homebrew/bin/brew shellenv)"
```

## 2. Install git, Python, and the Databricks CLI
```bash
brew install git python@3.12
brew tap databricks/tap
brew install databricks
databricks version    # confirm it prints a version (v1.12+)
```

## 3. Log in to the Databricks workspace (creates a CLI profile)
```bash
databricks auth login --host https://dbc-763a60c8-13b8.cloud.databricks.com --profile signalv-dev
# A browser opens — complete the login. Then:
export DATABRICKS_CONFIG_PROFILE=signalv-dev
databricks current-user me   # should show your name/email
```
> Tip: put `export DATABRICKS_CONFIG_PROFILE=signalv-dev` in your `~/.zshrc` so every new
> terminal uses this profile. If a command later fails with "refresh token is invalid",
> just re-run the `databricks auth login` line above.

## 4. Get the code
```bash
cd ~
git clone -b polly/connectors-deploy https://github.com/sysopmatt/lakeflow-community-connectors.git
cd lakeflow-community-connectors
```

## 5. Install the `community-connector` deploy CLI (one-time, in a venv)
> Do **not** use the system `pip` directly (macOS ships a broken shim). Always use a venv.
```bash
cd tools/community_connector
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install build          # needed for the wheel build step
cd ../..                    # back to the repo root — run all deploy commands from here
community-connector --help  # sanity check
```
Every new terminal that will run deploy commands must first:
```bash
cd ~/lakeflow-community-connectors
source tools/community_connector/.venv/bin/activate
export DATABRICKS_CONFIG_PROFILE=signalv-dev
```

---

## 6. Create the Unity Catalog connections (holds the credentials)

The target catalog `ingestion` and schemas `ingestion.dualentry` / `ingestion.hubspot`
already exist. Your principal needs these grants on them (ask a UC admin if a command
later says permission denied):
```sql
GRANT USE CATALOG ON CATALOG ingestion TO `you@example.com`;
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME ON SCHEMA ingestion.dualentry TO `you@example.com`;
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME ON SCHEMA ingestion.hubspot    TO `you@example.com`;
```

### 6a. DualEntry connection
```bash
community-connector create_connection dualentry dualentry -o '{"api_key":"<YOUR_DUALENTRY_API_KEY>"}'
```
> A connection named `dualentry` already exists in this workspace. If the command says it
> exists, you can reuse it and skip this step.

### 6b. HubSpot connection (Private App token)
Create a HubSpot **Private App** and copy its access token:
1. In HubSpot: **Settings → Integrations → Private Apps → Create a private app**
   (requires Super Admin, or the "Developer tools access" permission).
2. On the **Scopes** tab grant **read** scopes for what you want to ingest. For full
   coverage: `crm.objects.*.read`, `crm.schemas.*.read`, `crm.lists.read`,
   `crm.objects.owners.read`, `content`, `forms`, `marketing.campaigns.read`,
   `business-intelligence`, `communication_preferences.read`, `conversations.read`,
   `tickets`.
3. Copy the generated token (shown once).

Then (the connection can't be named `hubspot` — that name is taken by the native
connector — so we use `hubspot_extended`):
```bash
community-connector create_connection hubspot_extended hubspot_extended -o '{"access_token":"<YOUR_PRIVATE_APP_TOKEN>"}'
```

---

## 7. Deploy the pipelines

This builds two wheels, uploads them, and creates a **managed ingestion pipeline**.

### 7a. DualEntry → `ingestion_dualentry` → `ingestion.dualentry`
```bash
community-connector create_pipeline dualentry ingestion_dualentry \
  -ps deploy/dualentry_spec.yaml -n dualentry -c ingestion -t dualentry
community-connector run_pipeline ingestion_dualentry
community-connector show_pipeline ingestion_dualentry
```

### 7b. HubSpot → `ingestion_hubspot` → `ingestion.hubspot`
```bash
community-connector create_pipeline hubspot_extended ingestion_hubspot \
  -ps deploy/hubspot_spec.yaml -n hubspot_extended -c ingestion -t hubspot
community-connector run_pipeline ingestion_hubspot
community-connector show_pipeline ingestion_hubspot
```

`show_pipeline` prints a URL — open it to watch the run in the Databricks UI.

## 8. Verify the data
```sql
-- DualEntry
SELECT * FROM ingestion.dualentry.accounts LIMIT 100;
SELECT * FROM ingestion.dualentry.invoices LIMIT 100;
-- HubSpot
SELECT * FROM ingestion.hubspot.contacts LIMIT 100;
SELECT * FROM ingestion.hubspot.deals    LIMIT 100;
```

---

## 9. Trimming streams your account can't access ("deploy-all, then trim")

These are **managed** pipelines: the set of tables is fixed by the spec at create time.
Some streams require a subscription/module your org may not have, and will fail with
**HTTP 402 (subscription required)** or **403 (access denied)** — the *other* tables still
succeed. To get a fully green run, remove the failing streams and redeploy:

1. After a run, list the failed flows in the pipeline UI (or `show_pipeline`).
2. Delete those `- table:` blocks from the spec (`deploy/dualentry_spec.yaml` or
   `deploy/hubspot_spec.yaml`).
3. Redeploy the trimmed spec:
   ```bash
   community-connector update_pipeline ingestion_hubspot -ps deploy/hubspot_spec.yaml
   community-connector run_pipeline ingestion_hubspot
   ```

**DualEntry:** the 39 streams here are already the known-good set for this org (9 gated
streams — budgets, vat_rates, gst_tax_rates, product_tax_codes, workflows,
workflow_actions, workflow_execution_states, contracts, statistical_journals — were
already removed).

**HubSpot — streams most likely to be tier-gated** (trim if they 402/403):
- *Marketing Hub Pro/Enterprise:* `marketing_emails`, `email_events`, `forms`,
  `form_submissions`, `marketing_events`, `campaigns`, `lists`, `subscription_definitions`,
  `analytics_views`
- *CMS Hub:* `blog_posts`, `blog_tags`, `blog_authors`, `landing_pages`, `site_pages`,
  `hubdb_tables`, `url_redirects`
- *Service Hub:* `tickets`, `conversation_inboxes`, `conversation_threads`,
  `conversation_messages`
- *Commerce / Sales tiers:* `orders`, `carts`, `commerce_payments`, `subscriptions`,
  `invoices`, `quotes`, `leads`, `appointments`, `listings`
- *Pro+ behavioral events:* `behavioral_events`

Always-available (any portal): `contacts`, `companies`, `deals`, `calls`, `emails`,
`meetings`, `tasks`, `notes`, `line_items`, `products`, `owners`, `pipelines`,
`properties`, `crm_schemas`, `communications`, `postal_mail`, `feedback_submissions`.

---

## 10. Troubleshooting
| Symptom | Fix |
|---|---|
| `pip: bad interpreter` | Don't use bare `pip`; activate the venv (§5) or use `python3 -m pip`. |
| `refresh token is invalid` | Re-run `databricks auth login --profile signalv-dev`. |
| `CREATE VOLUME / USE SCHEMA` denied | Ask a UC admin to run the grants in §6. |
| `python -m build` error / stray `build/` | Ensure `pip install build` ran, and run deploy commands from the **repo root** (not a folder containing a `build/` dir). |
| Some HubSpot tables fail 402/403 | Expected for tiers you don't have — trim them (§9). |
| Connection already exists | Reuse it, or drop it in the UI and recreate. |

## 11. Reference
- Connector source: `src/databricks/labs/community_connector/sources/{dualentry,hubspot_extended}/`
- Per-connector READMEs (objects, auth, schemas): the `README.md` in each source folder.
- Deploy specs: `deploy/dualentry_spec.yaml`, `deploy/hubspot_spec.yaml`.
