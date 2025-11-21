"""Metadata management functions and classes for loading and syncing metadata"""

import json
import logging
import os
from typing import Optional, Dict, Any, List
import yaml
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

logger = logging.getLogger(__name__)


def _get_spark():
    """Get active Spark session (always available in Databricks runtime)."""
    return SparkSession.getActiveSession()


def _get_dbutils():
    """Get dbutils instance (available in Databricks runtime).

    Returns:
        dbutils instance or None if not available
    """
    try:
        from pyspark.dbutils import DBUtils

        spark = _get_spark()
        if spark:
            return DBUtils(spark)
    except (ImportError, AttributeError):
        pass
    return None


def _get_current_user() -> str:
    """Get current username from Spark SQL context.

    Returns:
        Current username as string, or 'unknown' if unable to determine
    """
    try:
        spark = _get_spark()
        if spark:
            result = spark.sql("SELECT current_user() as user").first()
            if result:
                return result["user"] or "unknown"
    except Exception:
        pass
    return "unknown"


def _validate_no_circular_dependencies(job_name: str, tasks: List[Dict[str, Any]]) -> None:
    """Validate that there are no circular dependencies in tasks.

    Args:
        job_name: Name of the job
        tasks: List of task dictionaries

    Raises:
        ValueError: If circular dependencies are detected
    """
    # Build dependency graph
    graph: Dict[str, List[str]] = {}
    for task in tasks:
        task_key = task.get("task_key")
        depends_on = task.get("depends_on", [])
        if depends_on is None:
            depends_on = []
        graph[task_key] = depends_on

    # DFS to detect cycles
    visited = set()
    rec_stack = set()

    def has_cycle(node: str) -> bool:
        visited.add(node)
        rec_stack.add(node)

        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                if has_cycle(neighbor):
                    return True
            elif neighbor in rec_stack:
                return True

        rec_stack.remove(node)
        return False

    for task_key in graph:
        if task_key not in visited:
            if has_cycle(task_key):
                raise ValueError(f"Job '{job_name}' has circular dependencies detected")


class MetadataManager:
    """Manages metadata operations for the control table.

    Encapsulates all metadata management operations, reducing the need
    to pass control_table parameter repeatedly.

    Example:
        ```python
        manager = MetadataManager("catalog.schema.control_table")
        manager.ensure_exists()
        manager.load_yaml("/path/to/metadata.yaml")
        changes = manager.detect_changes(last_check_timestamp)
        ```
    """

    def __init__(self, control_table: str):
        """Initialize MetadataManager.

        Args:
            control_table: Name of the control table (e.g., "catalog.schema.table")
        """
        if not control_table or not isinstance(control_table, str):
            raise ValueError("control_table must be a non-empty string")
        self.control_table = control_table

    def ensure_exists(self) -> None:
        """Ensure the control table exists, create if it doesn't."""
        spark = _get_spark()
        try:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.control_table} (
                    resource_id STRING,
                    job_name STRING,
                    task_key STRING,
                    depends_on STRING,
                    task_type STRING,
                    job_config STRING,
                    task_config STRING,
                    disabled BOOLEAN DEFAULT false,
                    created_by STRING,
                    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    updated_by STRING,
                    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
                )
                TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')
            """
            )
            logger.info("Control table %s verified/created", self.control_table)
        except Exception as e:
            raise RuntimeError(f"Failed to create control table " f"'{self.control_table}': {str(e)}") from e

    def load_yaml(
        self, yaml_path: str, validate_file_exists: bool = True, var: Optional[Dict[str, Any]] = None
    ) -> tuple:
        """Load YAML metadata file into control table.

        Args:
            yaml_path: Path to YAML file
            validate_file_exists: Whether to check if file exists before loading
            var: Optional dictionary of variables for ${var.name} substitution

        Returns:
            Tuple of (num_tasks_loaded, job_names_loaded)
            - num_tasks_loaded: Number of tasks loaded
            - job_names_loaded: List of job names that were loaded

        Raises:
            FileNotFoundError: If YAML file doesn't exist and validate_file_exists=True
            ValueError: If YAML is invalid or if variable substitution fails
        """
        if validate_file_exists and not os.path.exists(yaml_path):
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        # Ensure table exists
        self.ensure_exists()

        try:
            with open(yaml_path, "r", encoding="utf-8") as file:
                yaml_content = file.read()
        except Exception as e:
            raise ValueError(f"Failed to read YAML file '{yaml_path}': {str(e)}") from e

        # Parse YAML first (without variable substitution to avoid issues with comments)
        try:
            config = yaml.safe_load(yaml_content)
        except Exception as e:
            raise ValueError(f"Failed to parse YAML file '{yaml_path}': {str(e)}") from e

        if not config or "jobs" not in config:
            raise ValueError("YAML file must contain 'jobs' key")

        # Flatten YAML structure into DataFrame (grouped by job)
        rows = []
        yaml_job_tasks = (
            {}
        )  # Track tasks per resource_id for deletion detection (key=resource_id, value=set of task_keys)
        resource_id_to_job_name = {}  # Track resource_id -> job_name mapping
        failed_jobs = []  # Track jobs that failed validation

        # Parse jobs - expect dict format {resource_id: job_config}
        if not isinstance(config["jobs"], dict):
            raise ValueError(
                "YAML 'jobs' must be a dictionary with resource IDs as keys. "
                "Example: jobs:\\n  my_job:\\n    tasks: [...]"
            )

        # Check for duplicate resource_ids in this file (shouldn't happen with dict, but validate)
        if len(config["jobs"]) != len(set(config["jobs"].keys())):
            duplicates = [k for k in config["jobs"].keys() if list(config["jobs"].keys()).count(k) > 1]
            raise ValueError(
                f"Duplicate resource_id(s) found in YAML file: {set(duplicates)}. " "Each resource_id must be unique."
            )

        for resource_id, job in config["jobs"].items():
            try:
                # Apply variable substitution to this job's config if variables provided
                if var:
                    from .utils import substitute_variables

                    try:
                        # Convert job config to JSON, substitute, then parse back
                        job_json = json.dumps(job)
                        job_json = substitute_variables(job_json, var)
                        job = json.loads(job_json)

                        # Also substitute in resource_id
                        resource_id = substitute_variables(resource_id, var)
                    except ValueError as e:
                        raise ValueError(f"Variable substitution failed: {str(e)}") from e

                # job_name is either the 'name' field or defaults to resource_id
                job_name = job.get("name", resource_id)

                tasks = job.get("tasks", [])
                if not tasks:
                    raise ValueError(f"Job '{job_name}' must have at least one task")

                job_level_keys = [
                    "tags",
                    "environments",
                    "parameters",
                    "timeout_seconds",
                    "max_concurrent_runs",
                    "queue",
                    "continuous",
                    "trigger",
                    "schedule",
                    "job_clusters",
                    "notification_settings",
                    "edit_mode",
                ]
                job_config_dict = {}
                for key in job_level_keys:
                    if key in job:
                        if key == "environments":
                            job_config_dict["_job_environments"] = job[key]
                        else:
                            job_config_dict[key] = job[key]

                job_config_json = json.dumps(job_config_dict) if job_config_dict else json.dumps({})

                task_keys_in_job = set()

                # First pass: collect all task_keys
                for task in tasks:
                    task_key = task.get("task_key")
                    if not task_key:
                        raise ValueError(f"Task must have 'task_key' field in job '{job_name}'")
                    task_keys_in_job.add(task_key)

                # Initialize yaml_job_tasks for this job (will be populated in second pass)
                job_task_keys = set()

                # Second pass: validate dependencies and build rows
                for task in tasks:
                    task_key = task.get("task_key")
                    task_type = task.get("task_type")
                    if not task_type:
                        raise ValueError(f"Task '{task_key}' must have 'task_type' field")

                    # Extract depends_on (list of task_key strings, default to empty list)
                    depends_on = task.get("depends_on", [])
                    if depends_on is None:
                        depends_on = []
                    if not isinstance(depends_on, list):
                        raise ValueError(f"Task '{task_key}' depends_on must be a list of task_key strings")

                    # Validate all dependencies exist
                    for dep_key in depends_on:
                        if not isinstance(dep_key, str):
                            raise ValueError(f"Task '{task_key}' depends_on must contain only task_key strings")
                        if dep_key not in task_keys_in_job:
                            raise ValueError(
                                f"Task '{task_key}' depends on '{dep_key}' which does not exist in job '{job_name}'"
                            )

                    # Extract task-specific config
                    # (file_path, sql_query, query_id, warehouse_id, run_if, environment_key, job_cluster_key, existing_cluster_id, notification_settings, etc.)
                    task_config = {}
                    if "file_path" in task:
                        task_config["file_path"] = task["file_path"]
                    if "sql_query" in task:
                        task_config["sql_query"] = task["sql_query"]
                    if "query_id" in task:
                        task_config["query_id"] = task["query_id"]
                    if "warehouse_id" in task:
                        task_config["warehouse_id"] = task["warehouse_id"]
                    if "timeout_seconds" in task:
                        task_config["timeout_seconds"] = task["timeout_seconds"]
                    if "run_if" in task:
                        task_config["run_if"] = task["run_if"]
                    if "environment_key" in task:
                        task_config["environment_key"] = task["environment_key"]
                    if "job_cluster_key" in task:
                        task_config["job_cluster_key"] = task["job_cluster_key"]
                    if "existing_cluster_id" in task:
                        task_config["existing_cluster_id"] = task["existing_cluster_id"]
                    if "notification_settings" in task:
                        task_config["notification_settings"] = task["notification_settings"]
                    if "package_name" in task:
                        task_config["package_name"] = task["package_name"]
                    if "entry_point" in task:
                        task_config["entry_point"] = task["entry_point"]
                    if "main_class_name" in task:
                        task_config["main_class_name"] = task["main_class_name"]
                    if "pipeline_id" in task:
                        task_config["pipeline_id"] = task["pipeline_id"]
                    if "commands" in task:
                        task_config["commands"] = task["commands"]
                    if "profiles_directory" in task and task["profiles_directory"]:
                        task_config["profiles_directory"] = task["profiles_directory"]
                    if "project_directory" in task and task["project_directory"]:
                        task_config["project_directory"] = task["project_directory"]
                    if "catalog" in task:
                        task_config["catalog"] = task["catalog"]
                    if "schema" in task:
                        task_config["schema"] = task["schema"]
                    if "parameters" in task:
                        task_config["parameters"] = task["parameters"]

                    current_user = _get_current_user()
                    disabled = task.get("disabled", False)

                    job_task_keys.add(task_key)

                    task_config_json = json.dumps(task_config)

                    rows.append(
                        {
                            "resource_id": resource_id,
                            "job_name": job_name,
                            "task_key": task_key,
                            "depends_on": json.dumps(depends_on),
                            "task_type": task_type,
                            "task_config": task_config_json,
                            "job_config": job_config_json,
                            "disabled": disabled,
                            "created_by": current_user,
                            "updated_by": current_user,
                        }
                    )

                # Validate no circular dependencies
                _validate_no_circular_dependencies(job_name, tasks)

                # All validation passed, add this job to yaml_job_tasks
                yaml_job_tasks[resource_id] = job_task_keys
                resource_id_to_job_name[resource_id] = job_name
                logger.debug(
                    f"Successfully processed job '{job_name}' (resource_id='{resource_id}') with {len(job_task_keys)} task(s): {sorted(job_task_keys)}"
                )

            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Failed to load job '{job_name}' (resource_id='{resource_id}'): {error_msg}")
                failed_jobs.append({"resource_id": resource_id, "job_name": job_name, "error": error_msg})
                # Remove any rows that were added for this job before the error
                rows = [row for row in rows if row["resource_id"] != resource_id]
                continue

        if failed_jobs:
            logger.warning(
                f"Failed to load {len(failed_jobs)} job(s): {failed_jobs}. "
                f"Continuing with {len(set(row['job_name'] for row in rows))} valid job(s)."
            )

        if not rows:
            logger.warning(f"No tasks found in YAML file '{yaml_path}'")
            return (0, [])

        spark = _get_spark()
        schema = StructType(
            [
                StructField("resource_id", StringType(), True),
                StructField("job_name", StringType(), True),
                StructField("task_key", StringType(), True),
                StructField("depends_on", StringType(), True),
                StructField("task_type", StringType(), True),
                StructField("job_config", StringType(), True),
                StructField("task_config", StringType(), True),
                StructField("disabled", BooleanType(), True),
                StructField("created_by", StringType(), True),
                StructField("updated_by", StringType(), True),
            ]
        )

        df = spark.createDataFrame(rows, schema)

        # Use merge to update existing records or insert new ones
        df.createOrReplaceTempView("yaml_data")

        spark.sql(
            f"""
            MERGE INTO {self.control_table} AS target
            USING yaml_data AS source
            ON target.resource_id = source.resource_id AND target.task_key = source.task_key
            WHEN MATCHED THEN
                UPDATE SET
                    job_name = source.job_name,
                    depends_on = source.depends_on,
                    task_type = source.task_type,
                    task_config = source.task_config,
                    job_config = source.job_config,
                    disabled = source.disabled,
                    updated_by = source.updated_by,
                    updated_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (
                    resource_id, job_name, task_key, depends_on, task_type,
                    task_config, job_config, disabled, created_by,
                    updated_by
                )
                VALUES (
                    source.resource_id, source.job_name, source.task_key, source.depends_on,
                    source.task_type, source.task_config, source.job_config,
                    source.disabled, source.created_by, source.updated_by
                )
        """
        )

        # Delete tasks that exist in control table but not in YAML
        deleted_count = 0
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.control_table)

        for resource_id, yaml_task_keys in yaml_job_tasks.items():
            existing_tasks = (
                spark.table(self.control_table)
                .filter(F.col("resource_id") == resource_id)
                .select("task_key")
                .collect()
            )
            existing_task_keys = {row["task_key"] for row in existing_tasks}
            tasks_to_delete = existing_task_keys - yaml_task_keys

            if tasks_to_delete:
                # Delete all tasks for this job that are not in YAML
                delete_conditions = [
                    (F.col("resource_id") == resource_id) & (F.col("task_key") == task_key)
                    for task_key in tasks_to_delete
                ]
                # Combine conditions with OR
                combined_condition = delete_conditions[0]
                for condition in delete_conditions[1:]:
                    combined_condition = combined_condition | condition

                delta_table.delete(combined_condition)
                deleted_count += len(tasks_to_delete)
                job_name = resource_id_to_job_name[resource_id]
                logger.info(
                    "Deleted %d task(s) from job '%s' (resource_id='%s') " "that were removed from YAML: %s",
                    len(tasks_to_delete),
                    job_name,
                    resource_id,
                    sorted(tasks_to_delete),
                )

        logger.info(
            "Successfully loaded %d task(s) from %d job(s) in '%s' into %s",
            len(rows),
            len(yaml_job_tasks),
            yaml_path,
            self.control_table,
        )
        if deleted_count > 0:
            logger.info(
                "Deleted %d task(s) that were removed from YAML",
                deleted_count,
            )
        # Return resource_ids (YAML dict keys) for orchestration
        return (len(rows), list(yaml_job_tasks.keys()))

    def load_from_folder(self, folder_path: str, var: Optional[Dict[str, Any]] = None) -> tuple:
        """Load all YAML files from a workspace folder into control table atomically.

        Lists all YAML files (.yaml, .yml) in the folder (including subdirectories),
        parses ALL files first to validate them, then loads all data in a single
        atomic operation. Either all files are loaded successfully or none are loaded.

        Args:
            folder_path: Path to workspace folder
                (e.g., '/Workspace/Users/user@example.com/metadata/')
            var: Optional dictionary of variables for ${var.name} substitution

        Returns:
            Tuple of (total_tasks_loaded, resource_ids_loaded)
            - total_tasks_loaded: Total number of tasks loaded across all YAML files
            - resource_ids_loaded: List of unique resource IDs that were loaded

        Raises:
            FileNotFoundError: If folder doesn't exist
            ValueError: If duplicate resource_ids found across files or validation fails
            RuntimeError: If folder operations fail
        """
        import glob

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        if not os.path.isdir(folder_path):
            raise ValueError(f"Path is not a folder: {folder_path}")

        # Ensure control table exists before loading
        self.ensure_exists()

        # Find all YAML files recursively
        yaml_files = []
        for ext in ["*.yaml", "*.yml"]:
            yaml_files.extend(glob.glob(os.path.join(folder_path, "**", ext), recursive=True))

        if not yaml_files:
            logger.warning("No YAML files found in folder: %s", folder_path)
            return (0, [])

        logger.info("Found %d YAML file(s) in folder '%s'", len(yaml_files), folder_path)

        # Phase 1: Parse all YAML files (validation only, no database writes)
        all_rows = []
        all_yaml_job_tasks = {}
        seen_resource_ids = set()

        for yaml_file in yaml_files:
            try:
                # Read and parse YAML
                with open(yaml_file, "r", encoding="utf-8") as file:
                    yaml_content = file.read()

                # Parse YAML
                config = yaml.safe_load(yaml_content)
                if not config or "jobs" not in config:
                    raise ValueError("YAML file must contain 'jobs' key")

                if not isinstance(config["jobs"], dict):
                    raise ValueError(
                        "YAML 'jobs' must be a dictionary with resource IDs as keys. "
                        "Example: jobs:\\n  my_job:\\n    tasks: [...]"
                    )

                # Check for duplicate resource_ids in this file
                if len(config["jobs"]) != len(set(config["jobs"].keys())):
                    duplicates = [k for k in config["jobs"].keys() if list(config["jobs"].keys()).count(k) > 1]
                    raise ValueError(
                        f"Duplicate resource_id(s) found in YAML file: {set(duplicates)}. "
                        "Each resource_id must be unique."
                    )

                # Process each job
                for resource_id, job in config["jobs"].items():
                    # Apply variable substitution
                    if var:
                        from .utils import substitute_variables

                        job_json = json.dumps(job)
                        job_json = substitute_variables(job_json, var)
                        job = json.loads(job_json)
                        resource_id = substitute_variables(resource_id, var)

                    # Check for duplicate resource_id across files
                    if resource_id in seen_resource_ids:
                        raise ValueError(
                            f"Duplicate resource_id '{resource_id}' found across multiple YAML files. "
                            f"This resource_id appears in '{yaml_file}' and was already defined in a previous file. "
                            "Each resource_id must be unique across all YAML files."
                        )
                    seen_resource_ids.add(resource_id)

                    # job_name is either the 'name' field or defaults to resource_id
                    job_name = job.get("name", resource_id)

                    tasks = job.get("tasks", [])
                    if not tasks:
                        raise ValueError(f"Job '{job_name}' must have at least one task")

                    # Extract job-level config
                    job_level_keys = [
                        "tags",
                        "environments",
                        "parameters",
                        "timeout_seconds",
                        "max_concurrent_runs",
                        "queue",
                        "continuous",
                        "trigger",
                        "schedule",
                        "job_clusters",
                        "notification_settings",
                        "edit_mode",
                    ]
                    job_config_dict = {}
                    for key in job_level_keys:
                        if key in job:
                            if key == "environments":
                                job_config_dict["_job_environments"] = job[key]
                            else:
                                job_config_dict[key] = job[key]

                    job_config_json = json.dumps(job_config_dict) if job_config_dict else json.dumps({})

                    # Collect all task_keys
                    task_keys_in_job = {task.get("task_key") for task in tasks if task.get("task_key")}
                    if len(task_keys_in_job) != len(tasks):
                        raise ValueError(f"All tasks must have 'task_key' field in job '{job_name}'")

                    # Process each task
                    job_task_keys = set()
                    for task in tasks:
                        task_key = task.get("task_key")
                        task_type = task.get("task_type")
                        if not task_type:
                            raise ValueError(f"Task '{task_key}' must have 'task_type' field")

                        # Validate depends_on
                        depends_on = task.get("depends_on", [])
                        if depends_on is None:
                            depends_on = []
                        if not isinstance(depends_on, list):
                            raise ValueError(f"Task '{task_key}' depends_on must be a list")

                        for dep_key in depends_on:
                            if dep_key not in task_keys_in_job:
                                raise ValueError(
                                    f"Task '{task_key}' depends on '{dep_key}', "
                                    f"but '{dep_key}' is not defined in job '{job_name}'"
                                )

                        if task_key in depends_on:
                            raise ValueError(f"Task '{task_key}' cannot depend on itself")

                        depends_on_json = json.dumps(depends_on)

                        # Extract task config
                        task_config_keys = [
                            k for k in task.keys() if k not in ["task_key", "task_type", "depends_on", "disabled"]
                        ]
                        task_config = {k: task[k] for k in task_config_keys}
                        task_config_json = json.dumps(task_config) if task_config else json.dumps({})

                        disabled = task.get("disabled", False)

                        # Add to rows
                        all_rows.append(
                            {
                                "resource_id": resource_id,
                                "job_name": job_name,
                                "task_key": task_key,
                                "depends_on": depends_on_json,
                                "task_type": task_type,
                                "job_config": job_config_json,
                                "task_config": task_config_json,
                                "disabled": disabled,
                            }
                        )

                        job_task_keys.add(task_key)

                    # Track tasks for this job
                    all_yaml_job_tasks[resource_id] = job_task_keys

                logger.debug("Validated %d task(s) from '%s'", len(job_task_keys), yaml_file)

            except Exception as e:
                raise RuntimeError(
                    f"Failed to load YAML files from folder '{folder_path}'. "
                    f"Error in file '{yaml_file}': {str(e)}. "
                    "No data has been loaded to the control table."
                ) from e

        if not all_rows:
            logger.warning("No valid tasks found in folder '%s'", folder_path)
            return (0, [])

        # Phase 2: All files validated successfully - now write to database atomically
        spark = _get_spark()
        current_user = _get_current_user()

        # Add audit fields
        for row in all_rows:
            row["created_by"] = current_user
            row["updated_by"] = current_user

        # Create DataFrame
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType

        schema = StructType(
            [
                StructField("resource_id", StringType(), False),
                StructField("job_name", StringType(), False),
                StructField("task_key", StringType(), False),
                StructField("depends_on", StringType(), False),
                StructField("task_type", StringType(), False),
                StructField("job_config", StringType(), False),
                StructField("task_config", StringType(), False),
                StructField("disabled", BooleanType(), False),
                StructField("created_by", StringType(), False),
                StructField("updated_by", StringType(), False),
            ]
        )

        df = spark.createDataFrame(all_rows, schema=schema)
        df.createOrReplaceTempView("yaml_data_folder")

        # MERGE into control table (atomic operation)
        spark.sql(
            f"""
            MERGE INTO {self.control_table} AS target
            USING yaml_data_folder AS source
            ON target.resource_id = source.resource_id AND target.task_key = source.task_key
            WHEN MATCHED THEN
                UPDATE SET
                    job_name = source.job_name,
                    depends_on = source.depends_on,
                    task_type = source.task_type,
                    task_config = source.task_config,
                    job_config = source.job_config,
                    disabled = source.disabled,
                    updated_by = source.updated_by,
                    updated_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (
                    resource_id, job_name, task_key, depends_on, task_type,
                    task_config, job_config, disabled, created_by,
                    updated_by
                )
                VALUES (
                    source.resource_id, source.job_name, source.task_key, source.depends_on,
                    source.task_type, source.task_config, source.job_config,
                    source.disabled, source.created_by, source.updated_by
                )
        """
        )

        # Delete tasks that exist in control table but not in any YAML file
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.control_table)

        for resource_id, yaml_task_keys in all_yaml_job_tasks.items():
            existing_tasks = (
                spark.table(self.control_table)
                .filter(F.col("resource_id") == resource_id)
                .select("task_key")
                .collect()
            )
            existing_task_keys = {row["task_key"] for row in existing_tasks}
            tasks_to_delete = existing_task_keys - yaml_task_keys

            if tasks_to_delete:
                delete_conditions = [
                    (F.col("resource_id") == resource_id) & (F.col("task_key") == task_key)
                    for task_key in tasks_to_delete
                ]
                combined_condition = delete_conditions[0]
                for condition in delete_conditions[1:]:
                    combined_condition = combined_condition | condition
                delta_table.delete(combined_condition)
                logger.debug(
                    "Deleted %d task(s) for resource_id '%s' that are no longer in YAML",
                    len(tasks_to_delete),
                    resource_id,
                )

        resource_ids = list(all_yaml_job_tasks.keys())
        logger.info(
            "Successfully loaded %d total task(s) from %d YAML file(s) in folder '%s' (%d unique job(s))",
            len(all_rows),
            len(yaml_files),
            folder_path,
            len(resource_ids),
        )

        return (len(all_rows), resource_ids)

    def sync_from_volume(self, volume_path: str, var: Optional[Dict[str, Any]] = None) -> tuple:
        """Load YAML files from Unity Catalog volume into control table atomically.

        Lists all YAML files (.yaml, .yml) in the volume path (including subdirectories),
        parses ALL files first to validate them, then loads all data in a single atomic
        operation. Either all files are loaded successfully or none are loaded.

        Args:
            volume_path: Path to Unity Catalog volume
                (e.g., '/Volumes/catalog/schema/volume' or
                '/Volumes/catalog/schema/volume/subfolder')
            var: Optional dictionary of variables for ${var.name} substitution

        Returns:
            Tuple of (total_tasks_loaded, resource_ids_loaded)
            - total_tasks_loaded: Total number of tasks loaded across all YAML files
            - resource_ids_loaded: List of unique resource IDs that were loaded

        Raises:
            ValueError: If duplicate resource_ids found across files or validation fails
            RuntimeError: If Spark is not available or volume operations fail
        """
        try:
            spark = _get_spark()
            if not spark:
                raise RuntimeError("Spark session not available. " "This function requires Databricks runtime.")

            # Ensure control table exists before syncing
            self.ensure_exists()

            # List YAML files in volume
            # Use dbutils for better compatibility across DBR versions
            yaml_files = []
            try:
                # Try using dbutils.fs.ls for recursive listing
                # This is more compatible across DBR versions than LIST RECURSIVE
                dbutils = _get_dbutils()

                if dbutils:

                    def list_files_recursive(path):
                        """Recursively list all files in a directory."""
                        files = []
                        try:
                            items = dbutils.fs.ls(path)
                            for item in items:
                                if item.isDir():
                                    # Recursively list subdirectory
                                    files.extend(list_files_recursive(item.path))
                                elif item.name.endswith((".yaml", ".yml")):
                                    files.append(item.path)
                        except Exception as e:
                            logger.warning(f"Failed to list directory {path}: {e}")
                        return files

                    yaml_files = list_files_recursive(volume_path)
                else:
                    # dbutils not available, try LIST command without RECURSIVE
                    logger.debug("dbutils not available, trying LIST command")
                    files_df = spark.sql(f"LIST '{volume_path}'")
                    files_list = files_df.collect()

                    for row in files_list:
                        file_name = row.get("name", "")
                        file_path = row.get("path", "")
                        file_type = row.get("type", "")

                        # Only process files (not directories) with YAML extensions
                        if file_type.lower() == "file" and file_name.endswith((".yaml", ".yml")):
                            yaml_files.append(file_path)

            except Exception as list_error:
                # Last resort fallback: try using file system operations
                logger.warning(
                    "Failed to list files using dbutils/LIST: %s. Trying alternative method.",
                    str(list_error),
                )
                try:
                    # Try LIST command one more time
                    files_df = spark.sql(f"LIST '{volume_path}'")
                    files_list = files_df.collect()

                    for row in files_list:
                        file_name = row.get("name", "")
                        file_path = row.get("path", "")
                        file_type = row.get("type", "")

                        if file_type.lower() == "file" and file_name.endswith((".yaml", ".yml")):
                            yaml_files.append(file_path)
                    if not yaml_files:
                        try:
                            yaml_df = spark.read.text(f"{volume_path}/*.yaml")
                            yaml_files.extend([row["path"] for row in yaml_df.select("path").distinct().collect()])
                        except Exception:
                            pass

                        try:
                            yml_df = spark.read.text(f"{volume_path}/*.yml")
                            yaml_files.extend([row["path"] for row in yml_df.select("path").distinct().collect()])
                        except Exception:
                            pass

                    if not yaml_files:
                        raise RuntimeError(
                            f"Could not list files in volume " f"'{volume_path}'. Error: {str(list_error)}"
                        ) from list_error
                except Exception as fallback_error:
                    raise RuntimeError(
                        f"Could not list files in volume '{volume_path}'. "
                        f"LIST error: {str(list_error)}. "
                        f"Fallback error: {str(fallback_error)}"
                    ) from fallback_error

            if not yaml_files:
                logger.warning("No YAML files found in %s", volume_path)
                return (0, [])

            logger.info("Found %d YAML file(s) in volume '%s'", len(yaml_files), volume_path)

            # Phase 1: Read and parse all YAML files (validation only, no database writes)
            all_rows = []
            all_yaml_job_tasks = {}
            seen_resource_ids = set()

            for yaml_file in yaml_files:
                try:
                    # Read file content from volume
                    # Try multiple methods for compatibility across different Spark modes
                    file_content = None

                    # Method 1: Try using dbutils (works in all modes including Spark Connect)
                    dbutils = _get_dbutils()
                    if dbutils:
                        try:
                            file_content = dbutils.fs.head(yaml_file, 10485760)  # Read up to 10MB
                        except Exception as e:
                            logger.debug("dbutils.fs.head failed: %s", e)

                    # Method 2: Try using spark.read.text (works in Spark Connect)
                    if not file_content:
                        try:
                            df = spark.read.text(yaml_file)
                            lines = [row.value for row in df.collect()]
                            file_content = "\n".join(lines)
                        except Exception as e:
                            logger.debug("spark.read.text failed: %s", e)

                    # Method 3: Try using sparkContext (only works in non-Connect mode)
                    if not file_content:
                        try:
                            file_content_lines = spark.sparkContext.textFile(yaml_file).collect()
                            file_content = "\n".join(file_content_lines)
                        except Exception as e:
                            logger.debug("sparkContext.textFile failed: %s", e)

                    if not file_content:
                        raise RuntimeError(f"Could not read file content from '{yaml_file}'")

                    # Parse YAML directly (no temp file needed for validation)
                    config = yaml.safe_load(file_content)
                    if not config or "jobs" not in config:
                        raise ValueError("YAML file must contain 'jobs' key")

                    if not isinstance(config["jobs"], dict):
                        raise ValueError(
                            "YAML 'jobs' must be a dictionary with resource IDs as keys. "
                            "Example: jobs:\\n  my_job:\\n    tasks: [...]"
                        )

                    # Check for duplicate resource_ids in this file
                    if len(config["jobs"]) != len(set(config["jobs"].keys())):
                        duplicates = [k for k in config["jobs"].keys() if list(config["jobs"].keys()).count(k) > 1]
                        raise ValueError(
                            f"Duplicate resource_id(s) found in YAML file: {set(duplicates)}. "
                            "Each resource_id must be unique."
                        )

                    # Process each job
                    for resource_id, job in config["jobs"].items():
                        # Apply variable substitution
                        if var:
                            from .utils import substitute_variables

                            job_json = json.dumps(job)
                            job_json = substitute_variables(job_json, var)
                            job = json.loads(job_json)
                            resource_id = substitute_variables(resource_id, var)

                        # Check for duplicate resource_id across files
                        if resource_id in seen_resource_ids:
                            raise ValueError(
                                f"Duplicate resource_id '{resource_id}' found across multiple YAML files. "
                                f"This resource_id appears in '{yaml_file}' and was already defined in a previous file. "
                                "Each resource_id must be unique across all YAML files."
                            )
                        seen_resource_ids.add(resource_id)

                        # job_name is either the 'name' field or defaults to resource_id
                        job_name = job.get("name", resource_id)

                        tasks = job.get("tasks", [])
                        if not tasks:
                            raise ValueError(f"Job '{job_name}' must have at least one task")

                        # Extract job-level config
                        job_level_keys = [
                            "tags",
                            "environments",
                            "parameters",
                            "timeout_seconds",
                            "max_concurrent_runs",
                            "queue",
                            "continuous",
                            "trigger",
                            "schedule",
                            "job_clusters",
                            "notification_settings",
                            "edit_mode",
                        ]
                        job_config_dict = {}
                        for key in job_level_keys:
                            if key in job:
                                if key == "environments":
                                    job_config_dict["_job_environments"] = job[key]
                                else:
                                    job_config_dict[key] = job[key]

                        job_config_json = json.dumps(job_config_dict) if job_config_dict else json.dumps({})

                        # Collect all task_keys
                        task_keys_in_job = {task.get("task_key") for task in tasks if task.get("task_key")}
                        if len(task_keys_in_job) != len(tasks):
                            raise ValueError(f"All tasks must have 'task_key' field in job '{job_name}'")

                        # Process each task
                        job_task_keys = set()
                        for task in tasks:
                            task_key = task.get("task_key")
                            task_type = task.get("task_type")
                            if not task_type:
                                raise ValueError(f"Task '{task_key}' must have 'task_type' field")

                            # Validate depends_on
                            depends_on = task.get("depends_on", [])
                            if depends_on is None:
                                depends_on = []
                            if not isinstance(depends_on, list):
                                raise ValueError(f"Task '{task_key}' depends_on must be a list")

                            for dep_key in depends_on:
                                if dep_key not in task_keys_in_job:
                                    raise ValueError(
                                        f"Task '{task_key}' depends on '{dep_key}', "
                                        f"but '{dep_key}' is not defined in job '{job_name}'"
                                    )

                            if task_key in depends_on:
                                raise ValueError(f"Task '{task_key}' cannot depend on itself")

                            depends_on_json = json.dumps(depends_on)

                            # Extract task config
                            task_config_keys = [
                                k for k in task.keys() if k not in ["task_key", "task_type", "depends_on", "disabled"]
                            ]
                            task_config = {k: task[k] for k in task_config_keys}
                            task_config_json = json.dumps(task_config) if task_config else json.dumps({})

                            disabled = task.get("disabled", False)

                            # Add to rows
                            all_rows.append(
                                {
                                    "resource_id": resource_id,
                                    "job_name": job_name,
                                    "task_key": task_key,
                                    "depends_on": depends_on_json,
                                    "task_type": task_type,
                                    "job_config": job_config_json,
                                    "task_config": task_config_json,
                                    "disabled": disabled,
                                }
                            )

                            job_task_keys.add(task_key)

                        # Track tasks for this job
                        all_yaml_job_tasks[resource_id] = job_task_keys

                    logger.debug("Validated %d task(s) from '%s'", len(job_task_keys), yaml_file)

                except Exception as e:
                    raise RuntimeError(
                        f"Failed to load YAML files from volume '{volume_path}'. "
                        f"Error in file '{yaml_file}': {str(e)}. "
                        "No data has been loaded to the control table."
                    ) from e

            if not all_rows:
                logger.warning("No valid tasks found in volume '%s'", volume_path)
                return (0, [])

            # Phase 2: All files validated successfully - now write to database atomically
            current_user = _get_current_user()

            # Add audit fields
            for row in all_rows:
                row["created_by"] = current_user
                row["updated_by"] = current_user

            # Create DataFrame
            from pyspark.sql.types import StructType, StructField, StringType, BooleanType

            schema = StructType(
                [
                    StructField("resource_id", StringType(), False),
                    StructField("job_name", StringType(), False),
                    StructField("task_key", StringType(), False),
                    StructField("depends_on", StringType(), False),
                    StructField("task_type", StringType(), False),
                    StructField("job_config", StringType(), False),
                    StructField("task_config", StringType(), False),
                    StructField("disabled", BooleanType(), False),
                    StructField("created_by", StringType(), False),
                    StructField("updated_by", StringType(), False),
                ]
            )

            df = spark.createDataFrame(all_rows, schema=schema)
            df.createOrReplaceTempView("yaml_data_volume")

            # MERGE into control table (atomic operation)
            spark.sql(
                f"""
                MERGE INTO {self.control_table} AS target
                USING yaml_data_volume AS source
                ON target.resource_id = source.resource_id AND target.task_key = source.task_key
                WHEN MATCHED THEN
                    UPDATE SET
                        job_name = source.job_name,
                        depends_on = source.depends_on,
                        task_type = source.task_type,
                        task_config = source.task_config,
                        job_config = source.job_config,
                        disabled = source.disabled,
                        updated_by = source.updated_by,
                        updated_timestamp = current_timestamp()
                WHEN NOT MATCHED THEN
                    INSERT (
                        resource_id, job_name, task_key, depends_on, task_type,
                        task_config, job_config, disabled, created_by,
                        updated_by
                    )
                    VALUES (
                        source.resource_id, source.job_name, source.task_key, source.depends_on,
                        source.task_type, source.task_config, source.job_config,
                        source.disabled, source.created_by, source.updated_by
                    )
            """
            )

            # Delete tasks that exist in control table but not in any YAML file
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forName(spark, self.control_table)

            for resource_id, yaml_task_keys in all_yaml_job_tasks.items():
                existing_tasks = (
                    spark.table(self.control_table)
                    .filter(F.col("resource_id") == resource_id)
                    .select("task_key")
                    .collect()
                )
                existing_task_keys = {row["task_key"] for row in existing_tasks}
                tasks_to_delete = existing_task_keys - yaml_task_keys

                if tasks_to_delete:
                    delete_conditions = [
                        (F.col("resource_id") == resource_id) & (F.col("task_key") == task_key)
                        for task_key in tasks_to_delete
                    ]
                    combined_condition = delete_conditions[0]
                    for condition in delete_conditions[1:]:
                        combined_condition = combined_condition | condition
                    delta_table.delete(combined_condition)
                    logger.debug(
                        "Deleted %d task(s) for resource_id '%s' that are no longer in YAML",
                        len(tasks_to_delete),
                        resource_id,
                    )

            resource_ids = list(all_yaml_job_tasks.keys())
            logger.info(
                "Successfully loaded %d total task(s) from %d YAML file(s) in volume '%s' (%d unique job(s))",
                len(all_rows),
                len(yaml_files),
                volume_path,
                len(resource_ids),
            )

            return (len(all_rows), resource_ids)

        except Exception as e:
            raise RuntimeError(f"Failed to sync YAML files from volume '{volume_path}': {str(e)}") from e

    def detect_changes(self, last_check_timestamp: Optional[str] = None) -> Dict[str, Any]:
        """Detect changes in control table since last check.

        Args:
            last_check_timestamp: ISO timestamp of last check (optional)

        Returns:
            Dictionary with change information:
            {
                'new_jobs': List[str],
                'updated_jobs': List[str],
                'disabled_jobs': List[str],
                'changed_tasks': List[Dict]
            }
        """
        spark = _get_spark()
        try:
            table_df = spark.table(self.control_table)

            changes = {
                "new_jobs": [],
                "updated_jobs": [],
                "disabled_jobs": [],
                "changed_tasks": [],
            }

            if last_check_timestamp:
                changed_df = table_df.filter(F.col("updated_timestamp") > F.lit(last_check_timestamp))
                changed_rows = changed_df.collect()

                if changed_rows:
                    updated_job_set = {row["job_name"] for row in changed_rows}
                    changes["updated_jobs"] = list(updated_job_set)

                    new_tasks = [row for row in changed_rows if row["created_timestamp"] == row["updated_timestamp"]]
                    changes["changed_tasks"] = [
                        {
                            "task_key": row["task_key"],
                            "job_name": row["job_name"],
                            "action": "new",
                        }
                        for row in new_tasks
                    ]

                    disabled_rows = [row for row in changed_rows if row.get("disabled", False)]
                    if disabled_rows:
                        disabled_job_set = {row["job_name"] for row in disabled_rows}
                        changes["disabled_jobs"] = list(disabled_job_set)
            else:
                # First run: treat all jobs as new
                all_jobs = table_df.select("job_name").distinct().collect()
                changes["new_jobs"] = [row["job_name"] for row in all_jobs]

            return changes

        except Exception as e:
            logger.error("Error detecting metadata changes: %s", str(e))
            return {
                "new_jobs": [],
                "updated_jobs": [],
                "disabled_jobs": [],
                "changed_tasks": [],
            }

    def get_all_jobs(self) -> List[str]:
        """Get list of all resource IDs in control table.

        Returns:
            List of resource IDs (YAML dict keys)
        """
        spark = _get_spark()
        try:
            jobs = spark.table(self.control_table).select("resource_id").distinct().collect()
            return [row["resource_id"] for row in jobs]
        except Exception as e:
            logger.error(
                "Failed to get jobs from control table '%s': %s",
                self.control_table,
                str(e),
            )
            raise RuntimeError(f"Failed to get jobs from control table " f"'{self.control_table}': {str(e)}") from e

    def get_job_tasks(self, job_name: str) -> List[Dict[str, Any]]:
        """Get all tasks for a specific job.

        Args:
            job_name: Name of the job

        Returns:
            List of task dictionaries
        """
        spark = _get_spark()
        try:
            tasks = spark.table(self.control_table).filter(F.col("job_name") == job_name).collect()
            return [row.asDict() if hasattr(row, "asDict") else dict(row) for row in tasks]
        except Exception as e:
            logger.error(
                "Failed to get tasks for job '%s': %s",
                job_name,
                str(e),
            )
            raise RuntimeError(f"Failed to get tasks for job '{job_name}': {str(e)}") from e
