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
   - **Parameters:** none required for the default config
3. Set a schedule (e.g., daily at 2 AM) or trigger manually

> No additional Databricks UI configuration is required for the new task/processor architecture beyond importing the repo and creating the job.

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
- **pipelines/task_runner.py**: Orchestration engine loading task metadata and config inputs
- **pipelines/processors/**: Processor factory, base processor, and specialized NII forecast processor
- **calculations/**: Business logic and complex calculations
- **utils/**: Spark session helpers and DB utilities
- **conf/**: Configuration files (sample_config.yaml for local/prod setup)
- **conf/tasks/**: YAML task definitions for processor orchestration
- **scripts/**: Helper scripts for deployment and local runs
- **.github/workflows/**: GitHub Actions CI/CD workflow

## Data Model and Pipeline Tables

### Source / Input Tables
These tables contain the raw historical data and forecast inputs used to calculate NII.

#### `source.actuals_balance`
| Column | Type | Description |
|---|---|---|
| run_id | STRING | Unique pipeline run identifier |
| business_unit | STRING | Business unit code |
| product | STRING | Product family |
| currency | STRING | Currency code |
| period_date | DATE | Accounting period end date |
| balance_amount | DECIMAL(18,4) | Historical closing balance |
| interest_income | DECIMAL(18,4) | Actual interest income earned |
| avg_rate | DECIMAL(9,6) | Observed average funding rate |

Sample data:
| run_id | business_unit | product | currency | period_date | balance_amount | interest_income | avg_rate |
|---|---|---|---|---|---|---|---|
| run_202605 | Retail | Term Loan | USD | 2026-04-30 | 10250000.00 | 145000.00 | 0.01415 |
| run_202605 | Corporate | Revolving Credit | EUR | 2026-04-30 | 8750000.00 | 126500.00 | 0.01446 |

#### `source.actuals_income`
| Column | Type | Description |
|---|---|---|
| run_id | STRING | Pipeline run identifier |
| business_unit | STRING | Business unit code |
| product | STRING | Product family |
| accounting_element | STRING | Income/expense element code |
| period_date | DATE | Accounting period date |
| amount | DECIMAL(18,4) | Amount posted |
| posting_category | STRING | e.g. interest, fee, rebate |

Sample data:
| run_id | business_unit | product | accounting_element | period_date | amount | posting_category |
|---|---|---|---|---|---|---|
| run_202605 | Retail | Term Loan | INT_INC | 2026-04-30 | 145000.00 | interest |
| run_202605 | Corporate | Revolving Credit | INT_INC | 2026-04-30 | 126500.00 | interest |

#### `source.forecast_inputs`
| Column | Type | Description |
|---|---|---|
| forecast_id | STRING | Forecast batch identifier |
| business_unit | STRING | Business unit code |
| product | STRING | Product family |
| currency | STRING | Currency code |
| target_date | DATE | Forecast period end date |
| forecast_balance | DECIMAL(18,4) | Projected balance for period |
| forecast_yield | DECIMAL(9,6) | Forecasted yield rate |
| scenario | STRING | Scenario label |

Sample data:
| forecast_id | business_unit | product | currency | target_date | forecast_balance | forecast_yield | scenario |
|---|---|---|---|---|---|---|---|
| fcst_2027Q1 | Retail | Term Loan | USD | 2027-03-31 | 10800000.00 | 0.01525 | baseline |
| fcst_2027Q1 | Corporate | Revolving Credit | EUR | 2027-03-31 | 8900000.00 | 0.01480 | baseline |

#### `source.market_assumptions`
| Column | Type | Description |
|---|---|---|
| assumption_id | STRING | Input assumption batch identifier |
| curve_type | STRING | Rate curve type |
| tenor | STRING | Tenor bucket |
| currency | STRING | Currency code |
| value | DECIMAL(18,8) | Market assumption value |
| effective_date | DATE | Effective date for assumption |

Sample data:
| assumption_id | curve_type | tenor | currency | value | effective_date |
|---|---|---|---|---|---|---|
| mkt_2026Q3 | OIS | 6M | USD | 0.01750000 | 2026-06-01 |
| mkt_2026Q3 | IRS | 1Y | EUR | 0.01230000 | 2026-06-01 |

### Hierarchy / Bridge Tables
These tables define the organization, product, and financial element hierarchies used to roll and allocate NII.

#### `dim.business_unit_hierarchy`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Leaf business unit code |
| parent_unit | STRING | Parent unit code |
| hierarchy_level | INT | Hierarchy level depth |
| is_leaf | BOOLEAN | True for leaf nodes |
| effective_from | DATE | Validity start date |

Sample data:
| business_unit | parent_unit | hierarchy_level | is_leaf |
|---|---|---|---|
| Retail | Banking | 3 | true |
| Corporate | Commercial | 3 | true |
| Banking | Group | 2 | false |

#### `dim.product_hierarchy`
| Column | Type | Description |
|---|---|---|
| product | STRING | Leaf product code |
| product_group | STRING | Parent product group |
| hierarchy_level | INT | Depth in product hierarchy |
| is_leaf | BOOLEAN | Leaf indicator |

Sample data:
| product | product_group | hierarchy_level | is_leaf |
|---|---|---|---|
| Term Loan | Loans | 3 | true |
| Revolving Credit | Loans | 3 | true |
| Loans | Asset Products | 2 | false |

#### `dim.financial_element_hierarchy`
| Column | Type | Description |
|---|---|---|
| financial_element | STRING | Leaf financial element code |
| parent_element | STRING | Parent grouping code |
| hierarchy_level | INT | Depth in financial hierarchy |
| is_leaf | BOOLEAN | Leaf indicator |

Sample data:
| financial_element | parent_element | hierarchy_level | is_leaf |
|---|---|---|---|
| INT_INC | Net Interest Income | 2 | true |
| FEE_INC | Non-Interest Income | 2 | true |
| Net Interest Income | Income | 1 | false |

#### `dim.cost_centre_bridge`
| Column | Type | Description |
|---|---|---|
| cost_centre | STRING | Detailed cost centre code |
| business_unit | STRING | Linked business unit |
| product | STRING | Linked product |
| is_leaf | BOOLEAN | True for assignable cost centres |

Sample data:
| cost_centre | business_unit | product | is_leaf |
|---|---|---|---|
| CC001 | Retail | Term Loan | true |
| CC002 | Corporate | Revolving Credit | true |

### Configuration Tables
The configuration layer uses versioned key-value records for maximum flexibility in runtime parameterization.

#### `config.parameter_store`
| Column | Type | Description |
|---|---|---|
| config_key | STRING | Configuration code, e.g. CFG100 |
| config_version | STRING | Version label, e.g. v2026-06 |
| configtxt1 | STRING | Text parameter 1 |
| configtxt2 | STRING | Text parameter 2 |
| configtxt3 | STRING | Text parameter 3 |
| configtxt4 | STRING | Text parameter 4 |
| configtxt5 | STRING | Text parameter 5 |
| configtxt6 | STRING | Text parameter 6 |
| configtxt7 | STRING | Text parameter 7 |
| configtxt8 | STRING | Text parameter 8 |
| configtxt9 | STRING | Text parameter 9 |
| configint1 | INT | Integer parameter 1 |
| configint2 | INT | Integer parameter 2 |
| configint3 | INT | Integer parameter 3 |
| configfloat1 | FLOAT | Float parameter 1 |
| configdouble1 | DOUBLE | Double parameter 1 |
| effective_date | DATE | When this configuration becomes active |
| expiry_date | DATE | When this configuration expires |
| description | STRING | Business purpose description |

Sample data:
| config_key | config_version | configtxt1 | configtxt2 | configint1 | configfloat1 | configdouble1 | effective_date |
|---|---|---|---|---|---|---|---|
| CFG100 | v2026-06 | Retail | Loans | 1 | 0.98 | 0.05 | 2026-06-01 |
| CFG101 | v2026-06 | USD | EUR | 30 | 0.015 | 0.0123 | 2026-06-01 |

#### `config.active_config_versions`
| Column | Type | Description |
|---|---|---|
| config_key | STRING | Configuration code |
| config_version | STRING | Active version label |
| active_flag | BOOLEAN | True when current version is active |
| activated_at | TIMESTAMP | Activation timestamp |

Sample data:
| config_key | config_version | active_flag | activated_at |
|---|---|---|---|
| CFG100 | v2026-06 | true | 2026-06-01 00:00:00 |
| CFG101 | v2026-06 | true | 2026-06-01 00:00:00 |

#### `config.parameter_catalog`
| Column | Type | Description |
|---|---|---|
| config_key | STRING | Configuration code |
| config_category | STRING | Category such as pricing, proxy, inflation |
| description | STRING | Semantic description of the config type |
| example_usage | STRING | Example scenario or business rule |

Sample data:
| config_key | config_category | description | example_usage |
|---|---|---|---|
| CFG100 | Run Scope | Business unit and product scope for NII run | Filter segments for the current forecast cycle |
| CFG101 | Yield Curve Mapping | Maps products to benchmark curves | Select USD / EUR curves by product family |

#### Configuration Types
| Config Key | Purpose |
|---|---|
| CFG100 | Run scope filters and default BU/product selection |
| CFG101 | Yield curve mapping and benchmark selection |
| CFG102 | Currency conversion rules and FX mapping |
| CFG103 | Launchpoint window definitions |
| CFG104 | Proxy source and sparse-segment fill rules |
| CFG105 | Inflation adjustment lookup inputs |
| CFG106 | Yield override rules |
| CFG107 | Forecast substitution rules |
| CFG108 | Cost centre allocation mapping |
| CFG109 | Balance scaling and seasonality factors |
| CFG110 | Historical yield adjustment parameters |
| CFG111 | Product segmentation rules |
| CFG112 | Scenario tagging and override controls |
| CFG113 | Aggregation / rollup flags |
| CFG114 | Data quality tolerances and thresholds |
| CFG115 | Execution scheduling and window offsets |
| CFG116 | Reporting / output formatting controls |

### Intermediate / Lookup Tables
Each processing stage writes a separate table used by the next stage.

#### `staging.yield_parameters`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Business unit code |
| product | STRING | Product code |
| currency | STRING | Currency code |
| reference_date | DATE | Reference date for yield lookup |
| yield_curve | STRING | Selected curve name |
| forward_rate | DECIMAL(18,8) | Calculated forward yield |

#### `staging.balance_parameters`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Business unit code |
| product | STRING | Product code |
| period_date | DATE | Period for balance parameter |
| balance_scaler | DECIMAL(18,8) | Scale factor for balance projection |
| seasonality_flag | STRING | Seasonality category |

#### `lookup.proxy_relationships`
| Column | Type | Description |
|---|---|---|
| source_segment | STRING | Sparse segment code |
| proxy_segment | STRING | Proxy segment code |
| proxy_weight | DECIMAL(9,6) | Weight applied to proxy data |
| effective_date | DATE | Effective date |

#### `lookup.inflation_adjustments`
| Column | Type | Description |
|---|---|---|
| country | STRING | Country or region |
| inflation_curve | STRING | Inflation source curve |
| adjustment_factor | DECIMAL(9,6) | Inflation adjustment factor |
| as_of_date | DATE | Lookup date |

#### `lookup.scaling_factors`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Business unit code |
| scaling_category | STRING | Type of scaling factor |
| scale_value | DECIMAL(18,8) | Scaling multiplier |
| valid_from | DATE | Start date |
| valid_to | DATE | End date |

#### `enriched.launchpoint_positions`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Business unit code |
| product | STRING | Product code |
| launchpoint_balance | DECIMAL(18,4) | Starting balance for forecast launchpoint |
| launchpoint_yield | DECIMAL(9,6) | Starting yield |
| launch_date | DATE | Launchpoint date |

#### `enriched.nii_drivers`
| Column | Type | Description |
|---|---|---|
| business_unit | STRING | Business unit code |
| product | STRING | Product code |
| driver_type | STRING | Driver type |
| driver_value | DECIMAL(18,8) | Calculated driver value |
| effective_date | DATE | Driver effective date |

### Output Tables
Each pipeline step writes a separate table so downstream processing can consume the most recent intermediate result.

| Step | Output Table | Description |
|---|---|---|
| 1 | `staging.config_snapshot` | Snapshot of input configuration and active versions |
| 2 | `staging.actuals_enriched` | Enriched actuals and balance inputs |
| 3 | `staging.forecast_enriched` | Forecast inputs augmented with scenario and product attributes |
| 4 | `staging.launchpoint_enriched` | Launchpoint positions for each BU/product |
| 5 | `lookup.driver_parameters` | Enriched calculation lookup values and driver sets |
| 6 | `lookup.proxy_enriched` | Proxy relationships resolved for missing segments |
| 7 | `lookup.inflation_adjustment` | Inflation adjustments joined to forecast data |
| 8 | `lookup.yield_delta` | Yield delta and forward curve outputs |
| 9 | `curated.nii_stage` | NII by business unit and product |
| 10 | `curated.nii_stage_cc` | Final NII allocation by cost centre |

## Configuration Flow Overview
A seven-step runtime configuration flow ensures the pipeline is driven by JSON inputs, task metadata, and versioned configuration keys.

1. **Initial JSON input** — user provides system parameters, business parameters, and config version keys.
2. **Task definition loading** — YAML task definitions declare required args and processor class names.
3. **Task runner orchestration** — the runner validates required params, enriches inputs, and selects tasks.
4. **Processor factory instantiation** — factory resolves the processing class and creates the processor.
5. **Base processor initialization** — common parameters are assigned to base instance variables.
6. **Specialized processor initialization** — subclass-specific config version keys are loaded and validated.
7. **Execution** — processor executes, performs database lookups using versioned config keys, and writes output tables.

### Detailed Step-by-Step Breakdown

#### 1. Initial JSON input
User-provided JSON includes three parameter groups:
- `system_params`: runtime control values such as `job_name`, `run_date`, `environment`
- `business_params`: business filters and defaults such as `business_unit`, `product`, `currency`
- `configuration_versions`: version keys such as `CFG100`–`CFG116`

Example JSON:
```json
{
  "system_params": {
    "job_name": "nii_forecast",
    "run_date": "2026-06-05",
    "environment": "prod"
  },
  "business_params": {
    "business_unit": "Retail",
    "product": "Term Loan",
    "currency": "USD"
  },
  "configuration_versions": {
    "CFG100": "v2026-06",
    "CFG101": "v2026-06",
    "CFG102": "v2026-06"
  }
}
```

#### 2. YAML task definition files
Each task is defined in YAML with required parameters and processing class metadata.

Example YAML task definition:
```yaml
task_name: NiiForecastTask
required_params:
  - job_name
  - run_date
  - business_unit
  - product
processing_class: NiiForecastProcessor
```

#### 3. Task runner orchestration
The task runner:
- reads JSON inputs and YAML metadata
- validates that `required_params` are present
- enriches business params with defaults and active config versions
- builds a final task context for processor instantiation

#### 4. Processor factory pattern
A factory maps `processing_class` names to concrete processor classes.

Example factory behavior:
```python
processor_class = factory.resolve(task_definition['processing_class'])
processor = processor_class(task_context)
```

#### 5. Base processor initialization
A base processor initializes shared runtime values:
- `self.job_name`
- `self.run_date`
- `self.environment`
- `self.business_unit`
- `self.product`
- `self.currency`

This ensures consistency across all specialized processors.

#### 6. Specialized processor initialization
Specialized subclasses extract domain-specific configuration version keys, for example:
- `CFG100` for run scope
- `CFG101` for yield curve mapping
- `CFG105` for inflation adjustment lookup

The subclass loads the corresponding rows from `config.parameter_store` and materializes each versioned config value into its own parameter object.

#### 7. Execution and database lookup
During execution, the processor uses `configuration_versions` to lookup the active parameter records:
- join `config.parameter_store` on `config_key` and `config_version`
- apply `configtxt*`, `configint*`, `configfloat*`, `configdouble*` to processing logic
- write step-specific output to the designated Delta table

Example execution flow:
```python
config_row = load_config_row('CFG100', self.config_versions['CFG100'])
run_scope = config_row['configtxt1']
if self.business_unit not in run_scope:
    skip_processing()
```

## Configuration Types Summary
| Category | Parameter Examples |
|---|---|
| Runtime parameters | `job_name`, `run_date`, `environment`, `task_id` |
| Configuration version keys | `CFG100`, `CFG101`, `CFG102`, `CFG103`, `CFG104`, `CFG105`, `CFG106`, `CFG107`, `CFG108`, `CFG109`, `CFG110`, `CFG111`, `CFG112`, `CFG113`, `CFG114`, `CFG115`, `CFG116` |
| System-generated parameters | `execution_id`, `config_snapshot_id`, `run_timestamp`, `active_config_version`, `output_table`

The runtime pipeline uses these categories to ensure that:
- system parameters control job execution behavior,
- business parameters scope the forecast to the correct BU/product/currency,
- configuration version keys select the exact parameter set for the current run.

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

