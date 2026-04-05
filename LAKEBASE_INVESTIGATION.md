# Lakebase & Unity Catalog Investigation — Artemis Tracker

**Date:** 2026-04-04
**Workspace:** fevm-oil-pump-monitor.cloud.databricks.com
**Profile:** fe-vm-oil-pump-monitor

---

## Executive Summary

The Artemis Tracker app has **three separate issues** that need to be resolved in order:

1. **The UC catalog `artemis_tracker` does not exist** — the `setup_tables.py` notebook was never run
2. **The Lakebase `artemis-tracker-lb` lacks a proper `resources` block in app.yaml** — unlike `esp-pm`, PGHOST is hardcoded instead of using `valueFrom` resource injection
3. **`create-database-catalog` fails** because the app SP (or your user) lacks `CREATE CATALOG` on the metastore — this is a metastore admin permission

**Recommended path (fastest to get the app live):** Fix the app.yaml to use Lakebase resource injection (Path A), which also unlocks credential generation. In parallel, run the setup notebooks to create the UC catalog for the ingestion pipeline. The UC catalog registration of Lakebase (Path B) is a nice-to-have but not required.

---

## Issue 1: Missing UC Catalog `artemis_tracker`

### Root Cause
The notebook `notebooks/setup_tables.py` was never executed. Line 25:
```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")  # CATALOG = "artemis_tracker"
```
This needs to run on a cluster or via `databricks jobs` — it uses `spark.sql()` which requires a Spark runtime.

### Fix
Run the setup notebook on any available cluster:
```bash
# Option A: Run as a one-time job
databricks jobs create --json '{
  "name": "artemis-setup-tables",
  "tasks": [{"task_key": "setup", "notebook_task": {"notebook_path": "/Workspace/Users/reishin.toolsi@databricks.com/artemis-tracker/notebooks/setup_tables"}, "existing_cluster_id": "<CLUSTER_ID>"}]
}' --profile fe-vm-oil-pump-monitor

# Option B: Upload notebook and run interactively
databricks workspace import /Users/reishin.toolsi/artemis-tracker/notebooks/setup_tables.py /Workspace/Users/reishin.toolsi@databricks.com/artemis-tracker/notebooks/setup_tables --format SOURCE --language PYTHON --overwrite --profile fe-vm-oil-pump-monitor
# Then run it from the workspace UI
```

**Note:** `CREATE CATALOG IF NOT EXISTS` requires CREATE CATALOG permission on the metastore. If your user has it (you likely do as a workspace admin), this will work when run interactively. The app SP does NOT need this permission — only the notebook runner does.

---

## Issue 2: app.yaml Missing `resources` Block (Critical)

### Root Cause
The current `app.yaml` hardcodes PGHOST and has **no `resources` section**:
```yaml
# CURRENT (broken for credential injection)
env:
  - name: PGHOST
    value: "ep-purple-night-d2l0hrgr.database.us-east-1.cloud.databricks.com"
  - name: PGPORT
    value: "5432"
  - name: PGDATABASE
    value: "artemis_app"
  - name: LAKEBASE_INSTANCE
    value: "artemis-tracker-lb"
```

Compare with the **working esp-pm app.yaml**:
```yaml
# ESP-PM (working pattern)
env:
  - name: PGUSER
    valueFrom: esp-pm-lakebase      # <-- injected by platform
  - name: PGPORT
    valueFrom: esp-pm-lakebase      # <-- injected by platform
resources:
  - name: esp-pm-lakebase
    description: "Lakebase PostgreSQL for ESP PM"
    database:
      instance_name: esp-pm-db       # <-- provisioned instance name
      database_name: esp_pm_app
      permission: CAN_CONNECT_AND_CREATE
```

The `resources` block is what triggers the Databricks Apps platform to:
1. Grant the app SP `CAN_CONNECT` (or `CAN_CONNECT_AND_CREATE`) on the Lakebase instance
2. Inject `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` into the app environment
3. Auto-rotate credentials on each deployment

Without it, the app SP has **no permission** on the Lakebase instance, which is why:
- `generate_database_credential(instance_names=["artemis-tracker-lb"])` fails
- PGHOST is set but PGPASSWORD is never provided

### Fix — Updated app.yaml

**IMPORTANT:** The esp-pm app uses a **provisioned** Lakebase instance (`esp-pm-db`). The artemis-tracker uses an **autoscaling** instance (`artemis-tracker-lb`) under project `artemis-tracker-db`. The `resources` block syntax may differ for autoscaling. Two options:

#### Option 2A: Autoscaling Lakebase resource reference
```yaml
command:
  - uvicorn
  - app.main:app
  - --host=0.0.0.0
  - --port=8000

env:
  - name: APP_TITLE
    value: "Artemis II Mission Tracker"
  - name: PGUSER
    valueFrom: artemis-lakebase
  - name: PGPORT
    valueFrom: artemis-lakebase
  - name: PGHOST
    valueFrom: artemis-lakebase
  - name: PGPASSWORD
    valueFrom: artemis-lakebase
  - name: PGDATABASE
    value: "artemis_app"
  - name: LAKEBASE_INSTANCE
    value: "artemis-tracker-lb"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "87e069097741b56c"

resources:
  - name: artemis-lakebase
    description: "Lakebase Autoscaling PostgreSQL for Artemis Tracker"
    database:
      instance_name: artemis-tracker-lb
      database_name: artemis_app
      permission: CAN_CONNECT_AND_CREATE
```

#### Option 2B: If autoscaling needs project syntax
```yaml
resources:
  - name: artemis-lakebase
    description: "Lakebase Autoscaling PostgreSQL for Artemis Tracker"
    database:
      project_name: artemis-tracker-db
      branch_name: production
      database_name: artemis_app
      permission: CAN_CONNECT_AND_CREATE
```

**Try Option 2A first.** If the deploy fails because `instance_name` does not resolve an autoscaling instance, try 2B.

### Verification After Deploy
```bash
# Redeploy with updated app.yaml
databricks apps deploy artemis-tracker --source-code-path /Workspace/Users/reishin.toolsi@databricks.com/artemis-tracker --profile fe-vm-oil-pump-monitor

# Check the app health endpoint
curl -s https://artemis-tracker-<workspace-id>.aws.databricksapps.com/api/health | python -m json.tool
```

The health endpoint should now show `"backend": "postgres"` instead of `"backend": "databricks"`.

---

## Issue 3: CREATE CATALOG on Metastore

### Root Cause
`databricks database create-database-catalog` attempts to register the Lakebase PostgreSQL database as a Unity Catalog catalog (foreign catalog). This requires `CREATE CATALOG` on the metastore.

### Who Has This Permission?
Run:
```bash
databricks api get /api/2.0/unity-catalog/metastore_summary --profile fe-vm-oil-pump-monitor
```
The response will show a `metastore_id` and `owner` field. The metastore owner and any user/group with `CREATE CATALOG` grant can do this.

To check current grants:
```sql
-- Run via SQL warehouse
SHOW GRANTS ON METASTORE
```

To grant yourself permission (if you are metastore admin or know the admin):
```sql
GRANT CREATE CATALOG ON METASTORE TO `reishin.toolsi@databricks.com`
```

### Do You Actually Need This?
**Probably not for the app.** The `create-database-catalog` command creates a UC foreign catalog that mirrors Lakebase tables as Delta-readable tables in UC. This is useful for:
- Querying Lakebase tables from notebooks/SQL warehouse via UC
- Lineage tracking in UC
- Governance

But the Artemis Tracker app already has a dual-backend pattern (`app/db.py`) that can:
- Connect to Lakebase directly via psycopg2 (primary path)
- Fall back to Databricks SQL warehouse (secondary path)
- Fall back to JPL Horizons API (tertiary path for current status)

The UC catalog registration is only needed if you want the SQL warehouse to query Lakebase tables. Since the app reads from Lakebase directly, **skip this for now**.

---

## Issue 4: Lakebase Autoscaling vs. Provisioned Differences

### Key Difference
- **esp-pm** uses a **provisioned** Lakebase instance: `esp-pm-db`
  - Instance has a static hostname: `instance-144fec57-c1ae-40a9-9d3a-ed74397cc232.database.cloud.databricks.com`
  - The `resources` block references `instance_name: esp-pm-db`
  - Credential injection uses `generate_database_credential(instance_names=[...])`

- **artemis-tracker** uses an **autoscaling** Lakebase instance under project `artemis-tracker-db`
  - Autoscaling instances have endpoints under branches: `projects/artemis-tracker-db/branches/production/endpoints/primary`
  - The hostname is: `ep-purple-night-d2l0hrgr.database.us-east-1.cloud.databricks.com`
  - Credential generation for autoscaling uses endpoint path, not instance_name

### Credential Generation Difference
For **provisioned**:
```python
w.database.generate_database_credential(instance_names=["esp-pm-db"])
```

For **autoscaling**:
```python
# May need endpoint-based credential generation:
w.database.generate_database_credential(
    endpoint="projects/artemis-tracker-db/branches/production/endpoints/primary"
)
# OR the instance_name might be "artemis-tracker-lb" if the platform resolves it
```

The `sync_to_lakebase.py` notebook uses the CLI approach:
```bash
databricks postgres generate-database-credential \
  projects/artemis-tracker-db/branches/production/endpoints/primary \
  --output json
```

### What To Check
```bash
# List the autoscaling project details
databricks postgres list-projects --profile fe-vm-oil-pump-monitor -o json

# Get the specific instance
databricks database get-database-instance artemis-tracker-lb --profile fe-vm-oil-pump-monitor -o json

# List endpoints for the project
databricks postgres list-endpoints projects/artemis-tracker-db/branches/production --profile fe-vm-oil-pump-monitor -o json

# Test credential generation
databricks postgres generate-database-credential \
  projects/artemis-tracker-db/branches/production/endpoints/primary \
  --profile fe-vm-oil-pump-monitor -o json
```

---

## Recommended Action Plan (Priority Order)

### Step 1: Fix app.yaml with `resources` block (15 min)
This unblocks the app's Lakebase connection immediately.

1. Update `app.yaml` and `_deploy/app.yaml` with the `resources` block (Option 2A above)
2. Upload and redeploy:
   ```bash
   databricks workspace import-dir /Users/reishin.toolsi/artemis-tracker/_deploy /Workspace/Users/reishin.toolsi@databricks.com/artemis-tracker --overwrite --profile fe-vm-oil-pump-monitor
   databricks apps deploy artemis-tracker --source-code-path /Workspace/Users/reishin.toolsi@databricks.com/artemis-tracker --profile fe-vm-oil-pump-monitor
   ```
3. Verify via `/api/health` endpoint

### Step 2: Update db.py for autoscaling credential fallback (10 min)
If `valueFrom` injection works, no code changes needed. If the platform does not inject credentials for autoscaling instances, update `_try_postgres()` in `app/db.py` to use endpoint-based credential generation:

```python
# In _try_postgres(), change the credential generation block:
if not password:
    try:
        w = _get_workspace_client()
        if not user:
            user = w.current_user.me().user_name
        # Try instance-based first, then endpoint-based
        try:
            cred = w.database.generate_database_credential(
                instance_names=["artemis-tracker-lb"]
            )
        except Exception:
            cred = w.database.generate_database_credential(
                endpoint="projects/artemis-tracker-db/branches/production/endpoints/primary"
            )
        password = cred.token
    except Exception as e:
        ...
```

### Step 3: Run setup_tables.py notebook (5 min)
This creates the `artemis_tracker` UC catalog with bronze/silver/gold/serving schemas. Needed for the ingestion pipeline to populate Lakebase.

### Step 4: Run ingestion notebooks (15 min)
After the UC catalog exists:
1. `ingest_horizons.py` — fetch JPL Horizons data into bronze
2. `ingest_full_history.py` — backfill trajectory
3. `transform_silver.py` — process bronze to silver
4. `seed_milestones.py` — populate milestones
5. `sync_to_lakebase.py` — push gold data to Lakebase

### Step 5 (Optional): Register Lakebase as UC Catalog
Only do this if you need SQL warehouse to query Lakebase tables. Requires metastore admin to grant `CREATE CATALOG`:
```bash
# After getting the grant:
databricks database create-database-catalog artemis_lb artemis-tracker-lb artemis_app --profile fe-vm-oil-pump-monitor
```

---

## Alternative Path C: Skip Lakebase Entirely, Use UC Delta Tables

If Lakebase connectivity proves too complex for autoscaling, the app already has a Databricks SQL fallback. You could:

1. Run setup_tables.py to create `artemis_tracker` catalog
2. Run ingestion + transform notebooks to populate gold views
3. Change `app/api/current.py` to query `artemis_tracker.gold.current_status` via the SQL warehouse
4. Change the other API routes similarly

The SQL queries in the API routes currently use Postgres-style SQL (bare table names). For Databricks SQL, you would need to prefix with `artemis_tracker.gold.` or `artemis_tracker.serving.`.

**Trade-off:** SQL warehouse has ~2-5s cold query latency vs. ~50ms for Lakebase. For a real-time mission tracker, Lakebase is strongly preferred.

---

## Key Findings Summary

| Finding | Status | Impact |
|---------|--------|--------|
| `artemis_tracker` UC catalog does not exist | NOT RUN | Blocks all ingestion notebooks |
| app.yaml missing `resources` block | MISCONFIGURED | Blocks PGHOST/credential injection |
| `create-database-catalog` needs CREATE CATALOG | PERMISSION GAP | Blocks UC registration of Lakebase |
| esp-pm uses provisioned, artemis uses autoscaling | ARCHITECTURE DIFF | Different credential patterns |
| App has 3-tier fallback (Lakebase → SQL → Horizons API) | WORKING | App is live via Horizons fallback |
| Autoscaling Lakebase may need endpoint-based `valueFrom` | UNVERIFIED | Need to test Option 2A vs 2B |

---

## Commands To Run (Morning Checklist)

```bash
# 1. Check metastore permissions
databricks api get /api/2.0/unity-catalog/metastore_summary -p fe-vm-oil-pump-monitor

# 2. Check Lakebase instance details
databricks database get-database-instance artemis-tracker-lb -p fe-vm-oil-pump-monitor -o json

# 3. Check autoscaling project
databricks postgres list-projects -p fe-vm-oil-pump-monitor -o json

# 4. List endpoints
databricks postgres list-endpoints projects/artemis-tracker-db/branches/production -p fe-vm-oil-pump-monitor -o json

# 5. Test credential generation
databricks postgres generate-database-credential projects/artemis-tracker-db/branches/production/endpoints/primary -p fe-vm-oil-pump-monitor -o json

# 6. Check app SP permissions
databricks api get /api/2.0/preview/scim/v2/ServicePrincipals/76057771525515 -p fe-vm-oil-pump-monitor

# 7. After fixing app.yaml, redeploy and check health
curl -s https://artemis-tracker-7474647106303257.aws.databricksapps.com/api/health | python -m json.tool
```
