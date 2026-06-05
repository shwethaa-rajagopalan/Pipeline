Databricks PySpark Pipeline
===========================

Scaffolded PySpark project for Databricks CI/CD and a 10-step NII forecasting pipeline (based on 2024TM93028 report).

The pipeline materialises staging and curated Delta tables for configuration, actuals, forecast enrichments, yield calculations, and cost-centre NII allocation.

**Databricks Workspace:** https://dbc-2a897476-1b67.cloud.databricks.com

## Quick Start (Local Testing)

1. Create a Python 3.10+ environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run locally (parquet output):

```bash
./scripts/local_run.sh
```

## Deployment to Databricks

### Step 1: Generate a Personal Access Token (PAT)

1. Log into https://dbc-2a897476-1b67.cloud.databricks.com
2. Click your account icon → **Settings** → **Developer** → **Personal access tokens**
3. Click **Generate new token** (name: `nii_pipeline_token`, lifetime: 90 days)
4. Copy the token and save it securely (you'll need it for the next steps)

### Step 2: Use the Default Free Warehouse (Recommended)

For Databricks free tier, use the default Serverless Starter Warehouse in your workspace.

- Default warehouse: `Serverless Starter Warehouse`
- Warehouse ID: `6c5228901e0e783a`
- The default warehouse does not require a cluster ID.
- This repo-centric deployment can run without adding `cluster_id` to the configuration.

Optional: Create a dedicated cluster only if you need custom compute settings.
1. In the Databricks workspace, go to **Compute** → **Create Cluster**
2. Configure:
   - **Cluster name:** `nii-forecast-cluster`
   - **Runtime:** `15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)`
   - **Workers:** 2–4 (or as needed for your data volume)
3. After creation, note the **Cluster ID** from the cluster details page

If you use the default warehouse, leave `cluster_id` blank in the configuration.

### Step 3: Clone Repository to Databricks Repos

1. In your Databricks workspace, go to **Repos** (left sidebar)
2. Click **Add Repo** → **Clone from Git provider**
3. Select your Git provider (GitHub, GitLab, etc.) and authorize
4. Paste your repo URL: `https://github.com/your-org/nii_forecasting_pipeline` (adjust as needed)
5. After cloning, copy the path (e.g., `/Repos/your_email@company.com/nii_forecasting_pipeline`)

### Step 4: Update Configuration

1. Edit [conf/sample_config.yaml](conf/sample_config.yaml)
2. If you are using the default Databricks warehouse, leave `cluster_id` blank:
   ```yaml
   cluster_id: ""
   ```
3. If you want a dedicated cluster, set its ID:
   ```yaml
   cluster_id: "0123-456789-abcdef"
   ```
4. Verify the `repo_path` matches your cloned repo path from Step 3

### Step 5: Create a Databricks Job

1. Go to **Workflows** → **Jobs** → **Create Job**
2. Configure:
   - **Name:** `nii_forecast_pipeline_job`
   - **Task type:** `Notebook` or `PySpark Python script`
   - **Path:** `/Repos/<your_path>/pipelines/run_pipeline.py`
   - **Cluster:** Select the default warehouse or your dedicated cluster from Step 2
   - **Parameters:** (optional) pass config path
3. Set a schedule (e.g., daily at 2 AM) or trigger manually

### Step 6: Enable GitHub Actions CI/CD (Optional)

1. In your GitHub repo, go to **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
2. Add two secrets:
   - `DATABRICKS_TOKEN`: Paste your PAT from Step 1
   - (DATABRICKS_HOST is hardcoded in the workflow, but you can override if needed)
3. Push changes to `main` or `master` branch to trigger the workflow

### Step 7: Run the Pipeline

#### Option A: Manually via Databricks UI
1. Go to **Workflows** → **Jobs** → `nii_forecast_pipeline_job`
2. Click **Run Now**

#### Option B: Trigger via REST API
```bash
export DATABRICKS_TOKEN="<your_pat>"
export DATABRICKS_HOST="https://dbc-2a897476-1b67.cloud.databricks.com"
export JOB_ID="<your_job_id>"

curl -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"job_id\": $JOB_ID}"
```

## Project Structure

- **pipelines/steps/**: Individual PySpark step modules (10-step NII computation DAG)
- **calculations/**: Business logic and complex calculations
- **utils/**: Spark session helpers and DB utilities
- **conf/**: Configuration files (sample_config.yaml for local/prod setup)
- **scripts/**: Helper scripts for deployment and local runs
- **.github/workflows/**: GitHub Actions CI/CD workflow

## Configuration Tables (from Report)

The pipeline manages 10 configuration families that control the NII forecast:

1. **Forward market rate curves**
2. **Run scope** (business units, time horizons, parameter versions)
3. **Yield index mapping** (BU + product → market rate benchmark)
4. **Launchpoint windows** (observation period per BU)
5. **Proxy source mappings** (target-to-proxy relationships for sparse segments)
6. **Inflation adjustments** (lag structures for yield inflation correction)
7. **Custom yield overrides** (bespoke yields for structured products)
8. **Launchpoint override rules** (corrections for structural changes)
9. **Forecast substitution rules** (gap-filling logic for monthly disaggregation)
10. **Launchpoint period yield overrides** (actuals-to-forecast transition yields)

## Troubleshooting

**Authentication failed:**
- Check that DATABRICKS_TOKEN is set and not expired
- Verify DATABRICKS_HOST matches your workspace URL

**Cluster not found:**
- If you are using a dedicated cluster, update `conf/sample_config.yaml` with the correct `cluster_id`
- If you are using the default warehouse, this warning may be unrelated to your job cluster setup
- Ensure the cluster is running before job execution if using a dedicated cluster

**Delta tables not found on first run:**
- Verify the catalog `nii_forecast` exists in your Databricks workspace
- Manually create it via SQL: `CREATE CATALOG IF NOT EXISTS nii_forecast;`
- Create the schema: `CREATE SCHEMA IF NOT EXISTS nii_forecast.pipeline;`

## Next Steps

- Customize the dummy data in the step modules with your actual data sources
- Add error handling and retry logic as needed for production
- Integrate with SNS triggers (as described in the report) for event-driven execution
- Set up data quality monitoring and lineage tracking in Databricks Unity Catalog

