#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_URL="${DATABRICKS_HOST:-https://dbc-2a897476-1b67.cloud.databricks.com}"
TOKEN="${DATABRICKS_TOKEN:-}"
REPO_PATH="/Repos/nii_forecasting_pipeline"
JOB_NAME="nii_forecast_pipeline_job"

if [ -z "$TOKEN" ]; then
  echo "Error: DATABRICKS_TOKEN environment variable not set."
  exit 1
fi

echo "Using Databricks workspace: $WORKSPACE_URL"
echo "Repository path: $REPO_PATH"

# Configure databricks-cli
export DATABRICKS_HOST="$WORKSPACE_URL"
export DATABRICKS_TOKEN="$TOKEN"

echo "Testing authentication..."
databricks workspace list /Repos 2>/dev/null || {
  echo "Error: Authentication failed. Check DATABRICKS_TOKEN and WORKSPACE_URL."
  exit 1
}

echo "✓ Authentication successful!"
echo ""
echo "Next steps to complete deployment:"
echo "  1. Ensure you have a Personal Access Token (PAT) in Databricks"
echo "  2. Use the default Databricks serverless warehouse or default job cluster"
echo "  3. Optionally update conf/sample_config.yaml with a cluster_id if you want a dedicated cluster"
echo "  4. Push this repo to GitHub and set up Databricks Repos import"
echo "  5. Create a Databricks job via the UI pointing to /Repos/nii_forecasting_pipeline/pipelines/run_pipeline.py"
echo ""
echo "For GitHub Actions CI/CD, add these secrets:"
echo "  - DATABRICKS_HOST: $WORKSPACE_URL"
echo "  - DATABRICKS_TOKEN: <your PAT>"
