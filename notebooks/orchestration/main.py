# Databricks notebook source
# MAGIC %md
# MAGIC # Card Issue Pipeline Orchestration
# MAGIC
# MAGIC This notebook orchestrates the execution of the Card Issue Pipeline using Databricks Workflows.
# MAGIC
# MAGIC ## Parameters
# MAGIC - `pipeline_name`: Name of the pipeline to execute (card_issue)
# MAGIC - `environment`: Environment (dev, qa, prod)
# MAGIC - `date`: Processing date (YYYY-MM-DD)
# MAGIC
# MAGIC ## Pipeline Details
# MAGIC - **Source**: Eligibility data (data_nonpii_dev catalog)
# MAGIC - **Target**: Card Benefit Load CI (data_nonpii_dev.otcfunds.card_benefit_load_ci)
# MAGIC - **Verification**: Plaid member verification required
# MAGIC - **Client**: H225 (configurable)
# MAGIC - **Benefit Year**: 2026 (configurable)

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

dbutils.widgets.text("pipeline_name", "card_issue", "Pipeline Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("date", "", "Processing Date")

# COMMAND ----------

import sys
import os
from datetime import datetime

# Add src to Python path
sys.path.append("/Workspace/src")

# Get parameters
pipeline_name = dbutils.widgets.get("pipeline_name")
environment = dbutils.widgets.get("environment")
processing_date = dbutils.widgets.get("date") or datetime.now().strftime("%Y-%m-%d")

print(f"Pipeline: {pipeline_name}")
print(f"Environment: {environment}")
print(f"Processing Date: {processing_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Pipeline Module

# COMMAND ----------

from pipelines.card_issue_pipeline import CardIssuePipeline
from common.logging import get_logger

logger = get_logger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Card Issue Pipeline

# COMMAND ----------

def run_card_issue_pipeline():
    """Execute card issue pipeline."""
    try:
        logger.info("Starting Card Issue Pipeline")
        pipeline = CardIssuePipeline(spark, environment)
        pipeline.run()
        logger.info("Card Issue Pipeline completed successfully")
        return {"status": "SUCCESS", "pipeline": "card_issue"}
    except Exception as e:
        logger.error(f"Card Issue Pipeline failed: {str(e)}")
        return {"status": "FAILED", "pipeline": "card_issue", "error": str(e)}

# COMMAND ----------

# Execute pipeline
result = None

if pipeline_name.lower() == "card_issue":
    result = run_card_issue_pipeline()
else:
    error_msg = f"Invalid pipeline name: {pipeline_name}. Only 'card_issue' is supported."
    logger.error(error_msg)
    result = {"status": "FAILED", "pipeline": pipeline_name, "error": error_msg}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report Results

# COMMAND ----------

print("\n" + "="*60)
print("CARD ISSUE PIPELINE EXECUTION SUMMARY")
print("="*60)

print(f"\nPipeline: {result['pipeline']}")
print(f"Status: {result['status']}")
if result['status'] == "FAILED":
    print(f"Error: {result.get('error', 'Unknown error')}")

print("\n" + "="*60)

# Raise exception if pipeline failed
if result['status'] == "FAILED":
    raise Exception(f"Card Issue Pipeline failed. Check logs for details.")
else:
    print("Card Issue Pipeline completed successfully!")

# COMMAND ----------
