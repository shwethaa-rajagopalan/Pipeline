# Databricks Deployment Checklist

**Workspace:** https://dbc-2a897476-1b67.cloud.databricks.com

## Manual Setup Steps (Required Before First Deployment)

- [ ] **Step 1: Generate Personal Access Token (PAT)**
  - Log into Databricks workspace
  - Settings → Developer → Personal access tokens
  - Generate new token (name: `nii_pipeline_token`)
  - Copy and save securely

- [ ] **Step 2: Verify Default Warehouse or Create a Compute Cluster (Optional)**
  - For Databricks free tier, the default `Serverless Starter Warehouse` is usually available automatically
  - Warehouse ID: `6c5228901e0e783a`
  - If you prefer dedicated compute, create a cluster in **Compute** → **Create Cluster**
  - Runtime: 15.4 LTS (Spark 3.5.0 or later)
  - Workers: 2-4 (adjust to your data volume)
  - Note the **Cluster ID** if you create one

- [ ] **Step 3: Update Configuration**
  - Edit `conf/sample_config.yaml`
  - Leave `cluster_id: ""` when using the default warehouse
  - Set `cluster_id: "0123-456789-abcdef"` only if you provisioned a dedicated cluster

- [ ] **Step 4: Create Databricks Catalog and Schema**
  - In Databricks SQL, run:
    ```sql
    CREATE CATALOG IF NOT EXISTS nii_forecast;
    CREATE SCHEMA IF NOT EXISTS nii_forecast.pipeline;
    CREATE SCHEMA IF NOT EXISTS nii_forecast.curated;
    ```

- [ ] **Step 5: Import Repository to Databricks Repos**
  - Repos → Add Repo → Clone from Git
  - Authorize your Git provider
  - Paste repo URL and clone
  - Copy the repo path (e.g., `/Repos/user@company.com/nii_forecasting_pipeline`)

- [ ] **Step 6: Create a Databricks Job**
  - Workflows → Jobs → Create Job
  - Name: `nii_forecast_pipeline_job`
  - Task: Notebook or PySpark script
  - Path: `/Repos/<your_path>/pipelines/run_pipeline.py`
  - Cluster: Select the default warehouse or your dedicated cluster from Step 2
  - Schedule: Daily at 2 AM (or manual trigger)

- [ ] **Step 7: Set Up GitHub Actions Secrets (Optional)**
  - GitHub repo → Settings → Secrets and variables → Actions
  - Add secret: `DATABRICKS_TOKEN` = your PAT from Step 1
  - Workflows will now auto-deploy on push to main/master

## Testing

### Databricks Job Execution
1. Go to **Workflows** → **Jobs** → `nii_forecast_pipeline_job`
2. Click **Run Now**
3. Monitor logs in the run details page

### REST API Test
```bash
export DATABRICKS_TOKEN="<your_pat>"
export DATABRICKS_HOST="https://dbc-2a897476-1b67.cloud.databricks.com"
export JOB_ID="<job_id_from_step_6>"

curl -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"job_id\": $JOB_ID}"
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `AuthenticationError` | Verify PAT is valid and not expired; check DATABRICKS_TOKEN env var |
| `ClusterNotFound` | Update `conf/sample_config.yaml` with correct cluster_id |
| `TableNotFound` (Delta) | Create schema: `CREATE SCHEMA nii_forecast.pipeline;` |
| `PermissionDenied` | Ensure PAT user has workspace admin or job creation permissions |

## Next Deployments

After the initial setup, future deployments are automated:
1. Push changes to `main` branch
2. GitHub Actions validates and deploys
3. Deploy script authenticates and confirms Databricks access
4. Manual job creation remains (Databricks job API requires specific setup)

For fully automated job creation via API, contact Databricks support for workspace API documentation.
