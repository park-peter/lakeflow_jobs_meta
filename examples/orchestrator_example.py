# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs Meta - Orchestrator Example
# MAGIC
# MAGIC This notebook demonstrates how to use the Lakeflow Jobs Meta framework to create and manage metadata-driven Databricks Lakeflow Jobs.
# MAGIC
# MAGIC ## ⚠️ Before Running
# MAGIC **Update the configuration below to match your environment:**
# MAGIC - `DEFAULT_CONTROL_TABLE`: Your catalog and schema
# MAGIC - `DEFAULT_YAML_PATH`: Path to your YAML file
# MAGIC - `DEFAULT_QUERIES_PATH`: Path for SQL queries
# MAGIC - Update paths in your YAML metadata file
# MAGIC
# MAGIC ## Features
# MAGIC - Supports multiple task types: Notebook, SQL Query, SQL File
# MAGIC - Dynamic job generation from metadata
# MAGIC - Job lifecycle management (create/update/track)
# MAGIC - Execution order and dependency management
# MAGIC - Optional continuous monitoring
# MAGIC

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk lakeflow-jobs-meta

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import framework modules
import sys
import os
import logging

import lakeflow_jobs_meta as jm

# COMMAND ----------

current_dir = os.getcwd()

DEFAULT_CONTROL_TABLE = "catalog.schema.jobs_metadata_control_table"
DEFAULT_YAML_PATH = current_dir + "/metadata_examples.yaml"
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# Or use a folder path (loads all YAML files):
# DEFAULT_YAML_PATH = current_dir
# Or use a volume path:
# DEFAULT_YAML_PATH = "/Volumes/catalog/schema/config_folder/"
DEFAULT_QUERIES_PATH = "/Workspace/Users/" + current_user + "/queries"
DEFAULT_WAREHOUSE_ID = "warehouse_id"

# Create widgets for configuration
dbutils.widgets.text("control_table", DEFAULT_CONTROL_TABLE, "Control Table")
dbutils.widgets.text("yaml_path", DEFAULT_YAML_PATH, "YAML Path (file, folder, or volume)")
dbutils.widgets.text(
    "default_warehouse_id", DEFAULT_WAREHOUSE_ID, "Default SQL Warehouse ID (optional, for SQL tasks)"
)
dbutils.widgets.text("default_queries_path", DEFAULT_QUERIES_PATH, "Default Queries Save Path (optional)")

# Get widget values
CONTROL_TABLE = dbutils.widgets.get("control_table")
YAML_PATH = dbutils.widgets.get("yaml_path") or None
DEFAULT_WAREHOUSE_ID = dbutils.widgets.get("default_warehouse_id") or None
DEFAULT_QUERIES_PATH = dbutils.widgets.get("default_queries_path") or None

# Validate required parameter
if not CONTROL_TABLE:
    raise ValueError("control_table widget is required. Please set it in the widget or via base_parameters.")

# COMMAND ----------
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

logger.info(f"Configuration:")
logger.info(f"  Control Table: {CONTROL_TABLE}")
logger.info(f"  YAML Path: {YAML_PATH or 'Not configured (will process all jobs in control table)'}")
if YAML_PATH:
    if YAML_PATH.startswith("/Volumes/"):
        logger.info(f"    Path Type: Unity Catalog Volume")
        logger.info(f"    Behavior: Load from volume and process ONLY those jobs")
    elif YAML_PATH.endswith((".yaml", ".yml")):
        logger.info(f"    Path Type: YAML File")
        logger.info(f"    Behavior: Load from YAML file and process ONLY those jobs")
    else:
        logger.info(f"    Path Type: Folder (will load all YAML files)")
        logger.info(f"    Behavior: Load all YAML files from folder and process ONLY those jobs")
else:
    logger.info(f"    Behavior: Process ALL jobs in control table")
logger.info(
    f"  Default Warehouse ID: {DEFAULT_WAREHOUSE_ID or 'Not configured (SQL tasks must specify warehouse_id)'}"
)
logger.info(f"  Default Queries Path: {DEFAULT_QUERIES_PATH or 'Not configured'}")

# COMMAND ----------

# Create and update jobs
# If yaml_path is provided: Loads metadata and processes ONLY those jobs
# If yaml_path is NOT provided: Processes ALL jobs in control table
# Supports YAML files, folders (recursive), and Unity Catalog volumes
vars = {"env": "prod", "source": "customers", "warehouse_id": "abc123", "catalog": "bronze", "schema": "raw"}

jobs = jm.create_or_update_jobs(
    yaml_path=YAML_PATH,  # Automatically detects YAML file, folder, or volume
    default_pause_status=True,  # False = new manual jobs auto-run, scheduled jobs active; True = no auto-run, scheduled jobs paused
    control_table=CONTROL_TABLE,
    default_warehouse_id=DEFAULT_WAREHOUSE_ID,
    default_queries_path=DEFAULT_QUERIES_PATH,
    var=vars,
)

logger.info(f"✅ Managed {len(jobs)} jobs successfully")

# COMMAND ----------

# Uncomment to enable continuous monitoring
# This will check for metadata changes every 60 seconds and auto-update jobs
# Note: For volume_path, use a Unity Catalog volume path (e.g., "/Volumes/catalog/schema/volume")

# volume_path = YAML_PATH if YAML_PATH and YAML_PATH.startswith("/Volumes/") else None
# if volume_path:
#     monitor = MetadataMonitor(
#         control_table=CONTROL_TABLE,
#         check_interval_seconds=60,
#         volume_path=volume_path,  # Watch Unity Catalog volume for YAML files
#         auto_update_jobs=True
#     )
#     monitor.run_continuous(max_iterations=None)  # None = run indefinitely
# else:
#     logger.warning("Volume path not configured. Monitoring requires a Unity Catalog volume path.")
