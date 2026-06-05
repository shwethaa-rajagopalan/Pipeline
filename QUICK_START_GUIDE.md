# Deployment Summary & Quick Start Guide

## ✓ What's Been Set Up

Your workspace is now fully configured for **Databricks deployment** to `https://dbc-2a897476-1b67.cloud.databricks.com`

### Configured Infrastructure
- **Workspace URL:** https://dbc-2a897476-1b67.cloud.databricks.com
- **10-Step NII Forecasting Pipeline:** Based on 2024TM93028 report
- **Output Format:** Delta Lake (production-ready)
- **Configuration:** conf/sample_config.yaml (pre-filled for your workspace)
- **CI/CD Workflow:** GitHub Actions ready
- **Deployment Scripts:** Ready to use

### Pipeline Outputs
All steps write to Delta tables under the catalog `nii_forecast`:
- **Staging Zone:** `nii_forecast.pipeline.*` (steps 1–9 outputs)
- **Curated Zone:** `nii_forecast.curated.nii_stage_cc` (final NII by cost centre)

---

## 🚀 Quick Deployment Steps

### **Step 1: Generate Your Personal Access Token (PAT)**

1. Open https://dbc-2a897476-1b67.cloud.databricks.com
2. Click your **account icon** (top-right) → **Settings**
3. Go to **Developer** → **Personal access tokens**
4. Click **Generate new token**
   - **Token name:** `nii_pipeline_token`
   - **Lifetime:** 90 days
5. Copy the generated token and save it **securely** (you'll need it in Step 4)

---

### **Step 2: Default Warehouse Recommended (Optional Dedicated Cluster)**

For Databricks free tier, use the default `Serverless Starter Warehouse` (ID: `6c5228901e0e783a`).

If you need custom compute settings, you can optionally create a dedicated cluster:
1. In your workspace, go to **Compute** (left sidebar)
2. Click **Create cluster**
3. Configure:
   - **Cluster name:** `nii-forecast-cluster`
   - **Runtime:** `15.4 LTS (Apache Spark 3.5.0, Scala 2.12)` or later
   - **Workers:** 2–4 (adjust based on your data volume)
   - **Other settings:** Use defaults
4. Click **Create cluster** and wait for it to start
5. **Copy the Cluster ID** from the cluster details page (e.g., `0123-456789-abcdef01`)

---

### **Step 3: Create the Delta Catalog & Schema**

1. In your workspace, go to **SQL Editor** (left sidebar)
2. Create a new query and run:

```sql
CREATE CATALOG IF NOT EXISTS nii_forecast;
CREATE SCHEMA IF NOT EXISTS nii_forecast.pipeline;
CREATE SCHEMA IF NOT EXISTS nii_forecast.curated;
```

3. Click **Run**

---

### **Step 4: Set Your PAT in Environment Variables (Local)**

On your Mac terminal, set the environment variables:

```bash
export DATABRICKS_HOST="https://dbc-2a897476-1b67.cloud.databricks.com"
export DATABRICKS_TOKEN="<paste_your_pat_from_step_1>"
```

To verify authentication works:

```bash
cd /Users/shwethaarajagopalan/Documents/Study/DBX/Pipeline
chmod +x scripts/validate_workspace.sh
./scripts/validate_workspace.sh
```

You should see:
```
✓ Authentication successful
✓ Found X cluster(s)
✓ Repos directory accessible
✓ Workspace accessible
```

---

### **Step 5: Update Configuration**

Edit [conf/sample_config.yaml](conf/sample_config.yaml):

```yaml
databricks:
  cluster_id: ""  # Leave blank for the default warehouse
```

If you created a dedicated cluster, replace the empty string with its Cluster ID.

---

### **Step 6: Import Repository to Databricks Repos**

1. In your workspace, go to **Repos** (left sidebar)
2. Click **Add Repo** → **Clone from Git provider**
3. Authorize your Git provider (GitHub, GitLab, etc.)
4. Paste your repository URL and clone
5. **Note the full repo path** (e.g., `/Repos/your_email@company.com/nii_forecasting_pipeline`)

---

### **Step 7: Create a Databricks Job**

1. Go to **Workflows** → **Jobs** → **Create Job**
2. Fill in:
   - **Name:** `nii_forecast_pipeline_job`
   - **Task name:** `run_pipeline` (or any name)
   - **Type:** `Python script`
   - **Path:** `/Repos/<your_path_from_step_6>/pipelines/run_pipeline.py`
   - **Cluster:** Select `nii-forecast-cluster` (from Step 2)
3. **Optional:** Set a schedule (e.g., **Daily** at 2 AM)
4. Click **Create**

---

### **Step 8: Test the Job**

1. Go to **Workflows** → **Jobs** → `nii_forecast_pipeline_job`
2. Click **Run Now**
3. Monitor the run in the **Runs** tab
   - Logs appear in real-time
   - Expected runtime: 2–5 minutes (depending on cluster warm-up)
4. After completion, verify tables in **Catalog** → `nii_forecast` → `pipeline`

---

## 📋 Optional: GitHub Actions CI/CD

To enable automatic testing and deployment on every push:

1. Go to your **GitHub repo** → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add:
   - **Name:** `DATABRICKS_TOKEN`
   - **Value:** Your PAT from Step 1
3. The workflow in `.github/workflows/databricks-deploy.yml` will now:
   - Run tests on every push to `main` or `master`
   - Validate Databricks connectivity
   - Deploy automatically

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| `AuthenticationError: Unauthorized` | Verify PAT is valid; regenerate if expired |
| `ClusterNotFound` | Update `conf/sample_config.yaml` with correct cluster_id |
| `TableNotFound` (first run) | Run the SQL commands from Step 3 to create catalog/schema |
| `PermissionDenied` | Ensure your PAT user has workspace admin permissions |
| `RepoNotFound` | Verify repo path in Databricks Repos matches job config |

---

## 📊 What the Pipeline Does

The 10-step pipeline (from the report) computes Net Interest Income (NII) forecasts:

1. **Config Update** – Load governance-controlled parameters
2. **Actuals Enrichment** – Process historical balances and income
3. **Forecast Enrichment** – Expand annual forecasts to monthly
4. **Launchpoint Enrichment** – Establish forecast starting position
5. **Calculation Lookup** – Compute driver factors (currency mix, exposure, yield)
6. **Proxy Enrichment** – Fill gaps for sparse segments
7. **Inflation Adjustment Lookup** – Load inflation structures (parallel)
8. **Yield Delta** – Compute forward yield curves with adjustments
9. **NII Stage** – Calculate BU-level NII using core formula
10. **NII Stage CC** – Allocate BU NII to cost centres

**Output:** `nii_forecast.curated.nii_stage_cc` with NII by cost centre

---

## ✅ Validation Checklist

- [ ] PAT generated and saved securely
- [ ] Cluster created and noted (Cluster ID)
- [ ] Catalog & schema created in SQL
- [ ] Environment variables set (`DATABRICKS_TOKEN`, `DATABRICKS_HOST`)
- [ ] `./scripts/validate_workspace.sh` passes
- [ ] `conf/sample_config.yaml` updated with cluster_id
- [ ] Repository cloned to Databricks Repos
- [ ] Job created in Databricks Workflows
- [ ] Job test run completed successfully
- [ ] Delta tables appear in `nii_forecast` catalog

---

## 📞 Support

Refer to:
- **Detailed README:** [README.md](README.md)
- **Deployment Checklist:** [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)
- **Databricks Docs:** https://docs.databricks.com/

---

**You're ready to deploy! 🚀**
