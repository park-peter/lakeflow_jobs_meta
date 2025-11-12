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

    def load_yaml(self, yaml_path: str, validate_file_exists: bool = True) -> tuple:
        """Load YAML metadata file into control table.

        Args:
            yaml_path: Path to YAML file
            validate_file_exists: Whether to check if file exists before loading

        Returns:
            Tuple of (num_tasks_loaded, job_names_loaded)
            - num_tasks_loaded: Number of tasks loaded
            - job_names_loaded: List of job names that were loaded

        Raises:
            FileNotFoundError: If YAML file doesn't exist and validate_file_exists=True
            ValueError: If YAML is invalid
        """
        if validate_file_exists and not os.path.exists(yaml_path):
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        # Ensure table exists
        self.ensure_exists()

        try:
            with open(yaml_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
        except Exception as e:
            raise ValueError(f"Failed to parse YAML file '{yaml_path}': {str(e)}") from e

        if not config or "jobs" not in config:
            raise ValueError("YAML file must contain 'jobs' key")

        # Flatten YAML structure into DataFrame (grouped by job)
        rows = []
        yaml_job_tasks = {}  # Track tasks per job for deletion detection

        for job in config["jobs"]:
            job_name = job["job_name"]
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
            ]
            job_config_dict = {}
            for key in job_level_keys:
                if key in job:
                    if key == "environments":
                        job_config_dict["_job_environments"] = job[key]
                    else:
                        job_config_dict[key] = job[key]

            job_config_json = json.dumps(job_config_dict) if job_config_dict else json.dumps({})

            yaml_job_tasks[job_name] = set()
            task_keys_in_job = set()

            # First pass: collect all task_keys
            for task in tasks:
                task_key = task.get("task_key")
                if not task_key:
                    raise ValueError(f"Task must have 'task_key' field in job '{job_name}'")
                task_keys_in_job.add(task_key)

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

                yaml_job_tasks[job_name].add(task_key)

                task_config_json = json.dumps(task_config)

                rows.append(
                    {
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

        if not rows:
            logger.warning(f"No tasks found in YAML file '{yaml_path}'")
            return (0, [])

        spark = _get_spark()
        schema = StructType(
            [
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
            ON target.task_key = source.task_key AND target.job_name = source.job_name
            WHEN MATCHED THEN
                UPDATE SET
                    depends_on = source.depends_on,
                    task_type = source.task_type,
                    task_config = source.task_config,
                    job_config = source.job_config,
                    disabled = source.disabled,
                    updated_by = source.updated_by,
                    updated_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (
                    job_name, task_key, depends_on, task_type,
                    task_config, job_config, disabled, created_by,
                    updated_by
                )
                VALUES (
                    source.job_name, source.task_key, source.depends_on,
                    source.task_type, source.task_config, source.job_config,
                    source.disabled, source.created_by, source.updated_by
                )
        """
        )

        # Delete tasks that exist in control table but not in YAML
        deleted_count = 0
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(spark, self.control_table)

        for job_name, yaml_task_keys in yaml_job_tasks.items():
            existing_tasks = (
                spark.table(self.control_table).filter(F.col("job_name") == job_name).select("task_key").collect()
            )
            existing_task_keys = {row["task_key"] for row in existing_tasks}
            tasks_to_delete = existing_task_keys - yaml_task_keys

            if tasks_to_delete:
                # Delete all tasks for this job that are not in YAML
                delete_conditions = [
                    (F.col("job_name") == job_name) & (F.col("task_key") == task_key) for task_key in tasks_to_delete
                ]
                # Combine conditions with OR
                combined_condition = delete_conditions[0]
                for condition in delete_conditions[1:]:
                    combined_condition = combined_condition | condition

                delta_table.delete(combined_condition)
                deleted_count += len(tasks_to_delete)
                logger.info(
                    "Deleted %d task(s) from job '%s' " "that were removed from YAML: %s",
                    len(tasks_to_delete),
                    job_name,
                    sorted(tasks_to_delete),
                )

        logger.info(
            "Successfully loaded %d tasks from '%s' into %s",
            len(rows),
            yaml_path,
            self.control_table,
        )
        if deleted_count > 0:
            logger.info(
                "Deleted %d task(s) that were removed from YAML",
                deleted_count,
            )
        return (len(rows), list(yaml_job_tasks.keys()))

    def load_from_folder(self, folder_path: str) -> tuple:
        """Load all YAML files from a workspace folder into control table.

        Lists all YAML files (.yaml, .yml) in the folder (including subdirectories),
        reads each file, and loads them into the control table using load_yaml().
        Each file is processed independently and errors in one file don't stop
        processing of others.

        Args:
            folder_path: Path to workspace folder
                (e.g., '/Workspace/Users/user@example.com/metadata/')

        Returns:
            Tuple of (total_tasks_loaded, job_names_loaded)
            - total_tasks_loaded: Total number of tasks loaded across all YAML files
            - job_names_loaded: List of unique job names that were loaded

        Raises:
            FileNotFoundError: If folder doesn't exist
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

        total_loaded = 0
        all_job_names = set()
        failed_files = []

        for yaml_file in yaml_files:
            try:
                num_tasks, job_names = self.load_yaml(yaml_file, validate_file_exists=False)
                total_loaded += num_tasks
                all_job_names.update(job_names)
                logger.debug("Loaded %d task(s) from '%s'", num_tasks, yaml_file)
            except Exception as e:
                logger.error("Failed to load YAML file '%s': %s", yaml_file, str(e))
                failed_files.append((yaml_file, str(e)))
                continue

        if failed_files:
            logger.warning(
                "Failed to load %d file(s): %s",
                len(failed_files),
                [f[0] for f in failed_files],
            )

        logger.info(
            "Successfully loaded %d total task(s) from %d YAML file(s) in folder '%s'",
            total_loaded,
            len(yaml_files) - len(failed_files),
            folder_path,
        )
        return (total_loaded, list(all_job_names))

    def sync_from_volume(self, volume_path: str) -> tuple:
        """Load YAML files from Unity Catalog volume into control table.

        Lists all YAML files (.yaml, .yml) in the volume path (including
        subdirectories), reads each file, and loads them into the control table
        using load_yaml(). Each file is processed independently and errors in
        one file don't stop processing of others.

        Args:
            volume_path: Path to Unity Catalog volume
                (e.g., '/Volumes/catalog/schema/volume' or
                '/Volumes/catalog/schema/volume/subfolder')

        Returns:
            Tuple of (total_tasks_loaded, job_names_loaded)
            - total_tasks_loaded: Total number of tasks loaded across all YAML files
            - job_names_loaded: List of unique job names that were loaded

        Raises:
            RuntimeError: If Spark is not available or volume operations fail
        """
        try:
            spark = _get_spark()
            if not spark:
                raise RuntimeError("Spark session not available. " "This function requires Databricks runtime.")

            # Ensure control table exists before syncing
            self.ensure_exists()

            # List YAML files in volume using LIST command
            # This handles both files and subdirectories recursively
            yaml_files = []
            try:
                # Use LIST command to get all files recursively
                files_df = spark.sql(f"LIST '{volume_path}' RECURSIVE")
                files_list = files_df.collect()

                # Filter for YAML files only
                for row in files_list:
                    file_name = row.get("name", "")
                    file_path = row.get("path", "")
                    file_type = row.get("type", "")

                    # Only process files (not directories) with YAML extensions
                    if file_type.lower() == "file" and file_name.endswith((".yaml", ".yml")):
                        yaml_files.append(file_path)

                # If no files found, try non-recursive listing
                if not yaml_files:
                    files_df = spark.sql(f"LIST '{volume_path}'")
                    files_list = files_df.collect()
                    for row in files_list:
                        file_name = row.get("name", "")
                        file_path = row.get("path", "")
                        file_type = row.get("type", "")

                        if file_type.lower() == "file" and file_name.endswith((".yaml", ".yml")):
                            yaml_files.append(file_path)

            except Exception as list_error:
                # Fallback: try using glob patterns with Spark read
                try:
                    logger.debug(
                        "LIST command failed, trying glob pattern: %s",
                        str(list_error),
                    )
                    # Try reading with glob pattern
                    try:
                        yaml_df = spark.read.text(f"{volume_path}/**/*.yaml")
                        yaml_files.extend([row["path"] for row in yaml_df.select("path").distinct().collect()])
                    except Exception:
                        pass

                    try:
                        yml_df = spark.read.text(f"{volume_path}/**/*.yml")
                        yaml_files.extend([row["path"] for row in yml_df.select("path").distinct().collect()])
                    except Exception:
                        pass

                    # If still no files, try without recursive pattern
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

            total_loaded = 0
            all_job_names = set()
            failed_files = []

            for yaml_file in yaml_files:
                try:
                    # Read file content from volume using Spark
                    # Read as text file - each line becomes a row
                    file_content_lines = spark.sparkContext.textFile(yaml_file).collect()
                    file_content = "\n".join(file_content_lines)

                    # Write to temp file for parsing
                    # (yaml.safe_load requires file-like object)
                    import tempfile

                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(
                            mode="w",
                            suffix=".yaml",
                            delete=False,
                            encoding="utf-8",
                        ) as tmp:
                            tmp.write(file_content)
                            tmp_path = tmp.name

                        # Load YAML into control table
                        num_tasks, job_names = self.load_yaml(tmp_path, validate_file_exists=False)
                        total_loaded += num_tasks
                        all_job_names.update(job_names)
                        logger.debug("Loaded %d task(s) from '%s'", num_tasks, yaml_file)
                    finally:
                        # Clean up temp file
                        if tmp_path and os.path.exists(tmp_path):
                            try:
                                os.unlink(tmp_path)
                            except Exception as cleanup_error:
                                logger.warning(
                                    "Could not clean up temp file '%s': %s",
                                    tmp_path,
                                    str(cleanup_error),
                                )

                except Exception as e:
                    logger.error("Failed to load YAML file '%s': %s", yaml_file, str(e))
                    failed_files.append((yaml_file, str(e)))
                    continue

            if failed_files:
                logger.warning(
                    "Failed to load %d file(s): %s",
                    len(failed_files),
                    [f[0] for f in failed_files],
                )

            logger.info(
                "Successfully loaded %d total task(s) from %d YAML file(s) " "in volume '%s'",
                total_loaded,
                len(yaml_files) - len(failed_files),
                volume_path,
            )
            return (total_loaded, list(all_job_names))

        except Exception as e:
            logger.error("Error syncing YAML from volume '%s': %s", volume_path, str(e))
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
        """Get list of all job names in control table.

        Returns:
            List of job names
        """
        spark = _get_spark()
        try:
            jobs = spark.table(self.control_table).select("job_name").distinct().collect()
            return [row["job_name"] for row in jobs]
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
