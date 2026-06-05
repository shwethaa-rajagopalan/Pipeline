#!/usr/bin/env python3
"""Initialize Databricks Unity Catalog schemas required for the NII pipeline.

This script creates the necessary catalog and schemas in Databricks before running the pipeline.
Run this once before deploying the pipeline to a new Databricks workspace.

Usage:
    In Databricks: python scripts/init_databricks_schemas.py
    Locally: This script requires a Databricks environment to work.

"""

import os
from pyspark.sql import SparkSession


def init_schemas():
    """Create Databricks catalog and schemas for the NII pipeline."""
    spark = SparkSession.builder.appName("init_nii_schemas").getOrCreate()

    # Check if running in Databricks
    is_databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))

    if not is_databricks:
        print("⚠ Not running in Databricks environment. Skipping catalog/schema creation.")
        print("   Please run this script in a Databricks notebook or job.")
        return

    schemas = [
        "CREATE CATALOG IF NOT EXISTS nii_forecast",
        "CREATE SCHEMA IF NOT EXISTS nii_forecast.source",
        "CREATE SCHEMA IF NOT EXISTS nii_forecast.dim",
        "CREATE SCHEMA IF NOT EXISTS nii_forecast.config",
        "CREATE SCHEMA IF NOT EXISTS nii_forecast.pipeline",
        "CREATE SCHEMA IF NOT EXISTS nii_forecast.curated",
    ]

    for schema_sql in schemas:
        try:
            spark.sql(schema_sql)
            print(f"✓ {schema_sql}")
        except Exception as e:
            print(f"✗ {schema_sql}: {e}")

    print("\n✓ All schemas initialized successfully!")


if __name__ == "__main__":
    init_schemas()
