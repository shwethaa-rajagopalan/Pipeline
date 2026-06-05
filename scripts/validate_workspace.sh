#!/usr/bin/env bash
set -euo pipefail

# Quick validation script for Databricks workspace connectivity

WORKSPACE_URL="${DATABRICKS_HOST:-https://dbc-2a897476-1b67.cloud.databricks.com}"
TOKEN="${DATABRICKS_TOKEN:-}"

echo "=========================================="
echo "Databricks Workspace Validation"
echo "=========================================="
echo ""
echo "Workspace URL: $WORKSPACE_URL"

if [ -z "$TOKEN" ]; then
  echo "ERROR: DATABRICKS_TOKEN not set."
  echo ""
  echo "To set it:"
  echo "  export DATABRICKS_TOKEN='<your_pat_token>'"
  exit 1
fi

export DATABRICKS_HOST="$WORKSPACE_URL"
export DATABRICKS_TOKEN="$TOKEN"

echo ""
echo "[1] Testing authentication..."
if databricks workspace list /Repos >/dev/null 2>&1; then
  echo "✓ Authentication successful"
else
  echo "✗ Authentication failed"
  echo "  Check DATABRICKS_TOKEN and workspace URL"
  exit 1
fi

echo ""
echo "[2] Checking for compute clusters..."
CLUSTERS=$(databricks clusters list --output json 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('clusters', [])))" 2>/dev/null || echo "0")
if [ "$CLUSTERS" -gt 0 ]; then
  echo "✓ Found $CLUSTERS cluster(s)"
else
  echo "⚠ No clusters found. This may be fine if you are using the default Databricks warehouse."
fi

echo ""
echo "[3] Checking Repos..."
if databricks workspace list /Repos >/dev/null 2>&1; then
  echo "✓ Repos directory accessible"
else
  echo "⚠ Cannot access Repos (may not be enabled)"
fi

echo ""
echo "[4] Checking workspace status..."
if databricks workspace get-status / >/dev/null 2>&1; then
  echo "✓ Workspace accessible"
else
  echo "✗ Cannot access workspace root"
  exit 1
fi

echo ""
echo "=========================================="
echo "✓ Pre-deployment validation complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. If using a dedicated cluster, ensure it is created and running"
echo "  2. Update conf/sample_config.yaml with cluster_id or leave it blank for the default warehouse"
echo "  3. Create a Databricks job pointing to:"
echo "     /Repos/.../pipelines/run_pipeline.py"
echo "  4. Run: bash scripts/deploy_to_databricks.sh"
