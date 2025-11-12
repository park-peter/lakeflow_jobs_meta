"""Main orchestration functions and classes for managing Databricks jobs"""

import json
import logging
from typing import Optional, List, Dict, Any
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobSettings,
    SqlTaskQuery,
    Continuous,
    QueueSettings,
    TriggerSettings,
    CronSchedule,
    PauseStatus,
    JobCluster,
    TaskNotificationSettings,
    JobEmailNotifications,
)

try:
    from databricks.sdk.service.compute import (
        ClusterSpec,
        Environment as ComputeEnvironment,
        AwsAttributes,
        AwsAvailability,
        DataSecurityMode,
        RuntimeEngine,
    )
except ImportError:
    ClusterSpec = None
    ComputeEnvironment = None
    AwsAttributes = None
    AwsAvailability = None
    DataSecurityMode = None
    RuntimeEngine = None
try:
    from databricks.sdk.service.jobs import JobEnvironment
except ImportError:
    JobEnvironment = None

try:
    from databricks.sdk.service.jobs import TaskRetryMode
except ImportError:
    TaskRetryMode = None
from delta.tables import DeltaTable

from lakeflow_jobs_meta.task_builders import (
    create_task_from_config,
    convert_task_config_to_sdk_task,
    serialize_task_for_api,
)
from lakeflow_jobs_meta.metadata_manager import MetadataManager


class JobSettingsWithDictTasks(JobSettings):
    """Custom JobSettings that handles pre-serialized dict tasks.

    Overrides as_dict() to correctly handle tasks that are already dictionaries
    (serialized for API calls) rather than Task objects. This is necessary when
    updating jobs, as tasks may be serialized dicts with query_id references
    for SQL queries.
    """

    def as_dict(self) -> Dict[str, Any]:
        """Override as_dict() to handle dict tasks correctly."""
        result: Dict[str, Any] = {}
        if self.name:
            result["name"] = self.name
        if self.tasks:
            result["tasks"] = []
            for task in self.tasks:
                if isinstance(task, dict):
                    result["tasks"].append(task)
                elif hasattr(task, "as_dict"):
                    try:
                        result["tasks"].append(task.as_dict())
                    except (AttributeError, TypeError):
                        result["tasks"].append(serialize_task_for_api(task))
                else:
                    result["tasks"].append(task)
        if self.max_concurrent_runs is not None:
            result["max_concurrent_runs"] = self.max_concurrent_runs
        if self.timeout_seconds is not None:
            result["timeout_seconds"] = self.timeout_seconds
        if hasattr(self, "queue") and self.queue is not None:
            if isinstance(self.queue, dict):
                result["queue"] = self.queue
            elif hasattr(self.queue, "as_dict"):
                try:
                    result["queue"] = self.queue.as_dict()
                except AttributeError:
                    result["queue"] = self.queue
            else:
                result["queue"] = self.queue
        if hasattr(self, "continuous") and self.continuous is not None:
            if isinstance(self.continuous, dict):
                result["continuous"] = self.continuous
            elif hasattr(self.continuous, "as_dict"):
                try:
                    result["continuous"] = self.continuous.as_dict()
                except AttributeError:
                    result["continuous"] = self.continuous
            else:
                result["continuous"] = self.continuous
        if hasattr(self, "trigger") and self.trigger is not None:
            if isinstance(self.trigger, dict):
                result["trigger"] = self.trigger
            elif hasattr(self.trigger, "as_dict"):
                try:
                    result["trigger"] = self.trigger.as_dict()
                except AttributeError:
                    result["trigger"] = self.trigger
            else:
                result["trigger"] = self.trigger
        if hasattr(self, "schedule") and self.schedule is not None:
            if isinstance(self.schedule, dict):
                result["schedule"] = self.schedule
            elif hasattr(self.schedule, "as_dict"):
                try:
                    result["schedule"] = self.schedule.as_dict()
                except AttributeError:
                    result["schedule"] = self.schedule
            else:
                result["schedule"] = self.schedule
        if hasattr(self, "tags") and self.tags is not None:
            result["tags"] = self.tags
        if hasattr(self, "parameters") and self.parameters is not None:
            result["parameters"] = self.parameters
        if hasattr(self, "job_clusters") and self.job_clusters is not None:
            result["job_clusters"] = []
            for cluster in self.job_clusters:
                if isinstance(cluster, dict):
                    result["job_clusters"].append(cluster)
                elif hasattr(cluster, "as_dict"):
                    try:
                        result["job_clusters"].append(cluster.as_dict())
                    except (AttributeError, TypeError):
                        result["job_clusters"].append(cluster)
                else:
                    result["job_clusters"].append(cluster)
        if hasattr(self, "environments") and self.environments is not None:
            result["environments"] = []
            for env in self.environments:
                if isinstance(env, dict):
                    result["environments"].append(env)
                elif hasattr(env, "as_dict"):
                    try:
                        result["environments"].append(env.as_dict())
                    except AttributeError:
                        result["environments"].append(env)
                else:
                    result["environments"].append(env)
        if hasattr(self, "notification_settings") and self.notification_settings is not None:
            if isinstance(self.notification_settings, dict):
                result["notification_settings"] = self.notification_settings
            elif hasattr(self.notification_settings, "as_dict"):
                try:
                    result["notification_settings"] = self.notification_settings.as_dict()
                except AttributeError:
                    result["notification_settings"] = self.notification_settings
            else:
                result["notification_settings"] = self.notification_settings
        return result


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


def _task_row_to_dict(task_row: Any) -> Dict[str, Any]:
    """Convert Spark Row or dict to dictionary.

    Args:
        task_row: Spark Row object or dict

    Returns:
        Dictionary representation of the task row
    """
    if hasattr(task_row, "asDict"):
        return task_row.asDict()
    elif isinstance(task_row, dict):
        return task_row
    else:
        return dict(task_row)


def _apply_default_pause_status(
    job_config: Dict[str, Any],
    default_pause_status: bool,
    is_update: bool = False,
) -> Dict[str, Any]:
    """Apply default pause status to job settings with triggers/schedules.

    Args:
        job_config: Job configuration dictionary
        default_pause_status: Default pause status to apply
        is_update: Whether this is an update operation (don't apply default if True)

    Returns:
        Modified job configuration
    """
    # For updates, only apply pause_status if explicitly set in metadata
    if is_update:
        return job_config

    # For creates, apply default_pause_status if not explicitly set
    for setting_key in ["continuous", "schedule", "trigger"]:
        if setting_key in job_config:
            setting = job_config[setting_key]
            if isinstance(setting, dict) and "pause_status" not in setting:
                # Apply default only if not explicitly set
                if default_pause_status:
                    setting["pause_status"] = "PAUSED"

    return job_config


def _convert_job_setting_to_sdk_object(setting_type: str, setting_dict: Dict[str, Any]) -> Any:
    """Convert job setting dict to SDK object with proper enum handling.

    Args:
        setting_type: Type of setting
            ('queue', 'continuous', 'trigger', 'schedule')
        setting_dict: Dictionary with setting values

    Returns:
        SDK object or original dict if conversion fails
    """
    if not isinstance(setting_dict, dict):
        return setting_dict

    try:
        if setting_type == "continuous":
            pause_status = setting_dict.get("pause_status")
            task_retry_mode = setting_dict.get("task_retry_mode")
            if pause_status and isinstance(pause_status, str):
                pause_status = PauseStatus(pause_status)
            if task_retry_mode and isinstance(task_retry_mode, str) and TaskRetryMode:
                try:
                    task_retry_mode = TaskRetryMode(task_retry_mode)
                except (TypeError, ValueError, AttributeError):
                    pass
            continuous_kwargs = {
                "pause_status": pause_status or PauseStatus.UNPAUSED,
            }
            if task_retry_mode:
                continuous_kwargs["task_retry_mode"] = task_retry_mode
            elif TaskRetryMode:
                continuous_kwargs["task_retry_mode"] = TaskRetryMode.ON_FAILURE
            return Continuous(**continuous_kwargs)
        elif setting_type == "queue":
            return QueueSettings(**setting_dict)
        elif setting_type == "trigger":
            return TriggerSettings(**setting_dict)
        elif setting_type == "schedule":
            return CronSchedule(**setting_dict)
    except (TypeError, ValueError, AttributeError):
        return setting_dict

    return setting_dict


class JobOrchestrator:
    """Orchestrates Databricks Jobs based on metadata in control table.

    Encapsulates job creation, updates, and management operations.
    Reduces parameter passing by maintaining state for control_table,
    workspace_client, and jobs_table.

    Example:
        ```python
        # Use default control table name
        orchestrator = JobOrchestrator()

        # Or specify custom control table
        orchestrator = JobOrchestrator(control_table="catalog.schema.control_table")

        # Or specify both control table and jobs table
        orchestrator = JobOrchestrator(
            control_table="catalog.schema.control_table",
            jobs_table="catalog.schema.custom_jobs_table"
        )

        job_id = orchestrator.create_or_update_job("my_job")

        # Or create/update all jobs
        results = orchestrator.create_or_update_jobs(default_pause_status=False)
        ```
    """

    def __init__(
        self,
        control_table: Optional[str] = None,
        jobs_table: Optional[str] = None,
        workspace_client: Optional[WorkspaceClient] = None,
        default_warehouse_id: Optional[str] = None,
        default_queries_path: Optional[str] = None,
    ):
        """Initialize JobOrchestrator.

        Args:
            control_table: Name of the control table
                (e.g., "catalog.schema.table").
                If not provided, defaults to
                "main.default.job_metadata_control_table".
            jobs_table: Optional custom name for the jobs tracking table.
                If not provided, defaults to "{control_table}_jobs".
            workspace_client: Optional WorkspaceClient instance
                (creates new if not provided)
            default_warehouse_id: Optional default SQL warehouse ID for SQL tasks.
                This acts as a fallback when SQL tasks don't specify
                warehouse_id in their task_config. If neither is provided,
                task creation will fail. Useful when all SQL tasks in your
                jobs use the same warehouse.
            default_queries_path: Optional directory path where inline SQL queries
                will be saved (e.g., "/Workspace/Shared/LakeflowQueriesMeta").
                If not provided, queries are saved to the default workspace location.
        """
        # Set default control_table if not provided
        if control_table is None:
            control_table = "main.default.job_metadata_control_table"

        if not isinstance(control_table, str) or not control_table.strip():
            raise ValueError("control_table must be a non-empty string")

        self.control_table = control_table

        # Set jobs_table - use custom name if provided,
        # otherwise default to {control_table}_jobs
        if jobs_table is None:
            self.jobs_table = f"{control_table}_jobs"
        else:
            if not isinstance(jobs_table, str) or not jobs_table.strip():
                raise ValueError("jobs_table must be a non-empty string")
            self.jobs_table = jobs_table

        self.workspace_client = workspace_client or WorkspaceClient()
        self.metadata_manager = MetadataManager(control_table)
        self.default_warehouse_id = default_warehouse_id
        self.default_queries_path = default_queries_path

    def _create_job_tracking_table(self) -> None:
        """Create Delta table to track job IDs for each module (internal)."""
        spark = _get_spark()
        try:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.jobs_table} (
                    job_id BIGINT,
                    job_name STRING,
                    created_by STRING,
                    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    updated_by STRING,
                    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
                )
                TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')
            """
            )
            logger.info(
                "Job tracking table %s created/verified successfully",
                self.jobs_table,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create job tracking table " f"'{self.jobs_table}': {str(e)}") from e

    def ensure_setup(self) -> None:
        """Ensure control table and jobs tracking table exist."""
        self.metadata_manager.ensure_exists()
        self._create_job_tracking_table()

    def _get_stored_job_id(self, job_name: str) -> Optional[int]:
        """Get stored job_id for a job from Delta table (internal)."""
        spark = _get_spark()
        try:
            job_id_rows = (
                spark.table(self.jobs_table).filter(F.col("job_name") == job_name).select("job_id").limit(1).collect()
            )

            if job_id_rows:
                return job_id_rows[0]["job_id"]
            return None
        except Exception as e:
            logger.warning(
                "Job '%s': Could not retrieve job_id: %s",
                job_name,
                str(e),
            )
            return None

    def _store_job_id(self, job_name: str, job_id: int) -> None:
        """Store/update job_id for a job in Delta table (internal)."""
        spark = _get_spark()
        try:
            current_user = _get_current_user()
            source_data = [
                Row(
                    job_name=job_name,
                    job_id=job_id,
                    created_by=current_user,
                    updated_by=current_user,
                )
            ]
            source_df = spark.createDataFrame(source_data).withColumn("updated_timestamp", F.current_timestamp())
            source_df.createOrReplaceTempView("source_data")

            spark.sql(
                f"""
                MERGE INTO {self.jobs_table} AS target
                USING source_data AS source
                ON target.job_name = source.job_name
                WHEN MATCHED THEN
                    UPDATE SET
                        updated_by = source.updated_by,
                        updated_timestamp = source.updated_timestamp
                WHEN NOT MATCHED THEN
                    INSERT (
                        job_name, job_id, created_by, updated_by,
                        updated_timestamp
                    )
                    VALUES (
                        source.job_name, source.job_id, source.created_by,
                        source.updated_by, source.updated_timestamp
                    )
            """
            )

            logger.info("Job '%s': Stored job_id %d", job_name, job_id)
        except Exception as e:
            logger.error("Could not store job_id: %s", str(e))
            raise RuntimeError(f"Failed to store job_id for job '{job_name}': {str(e)}") from e

    def get_job_settings_for_job(self, job_name: str, first_task: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get job-level settings for a job from metadata.

        Retrieves job_config stored in the first task's task_config.
        Falls back to defaults if not found.

        Args:
            job_name: Name of the job
            first_task: Optional first task dict to avoid extra query

        Returns:
            Dictionary with job settings (timeout_seconds,
            max_concurrent_runs, queue, continuous, trigger, schedule)
        """
        from lakeflow_jobs_meta.constants import (
            JOB_TIMEOUT_SECONDS,
            MAX_CONCURRENT_RUNS,
        )

        default_settings = {
            "timeout_seconds": JOB_TIMEOUT_SECONDS,
            "max_concurrent_runs": MAX_CONCURRENT_RUNS,
            "queue": None,
            "continuous": None,
            "trigger": None,
            "schedule": None,
            "tags": None,
            "job_clusters": None,
            "environments": None,
            "notification_settings": None,
            "parameters": None,
        }

        if first_task:
            job_config_str = first_task.get("job_config", "{}")
        else:
            spark = _get_spark()
            try:
                job_tasks = spark.table(self.control_table).filter(F.col("job_name") == job_name).limit(1).collect()
                if not job_tasks:
                    return default_settings
                task = _task_row_to_dict(job_tasks[0])
                job_config_str = task.get("job_config", "{}")
            except Exception as e:
                logger.warning(
                    "Could not retrieve job settings for job '%s': %s. " "Using defaults.",
                    job_name,
                    str(e),
                )
                return default_settings

        try:
            job_config = json.loads(job_config_str) if isinstance(job_config_str, str) else job_config_str
            if job_config:
                # Update standard settings
                default_settings.update(
                    {
                        k: v
                        for k, v in job_config.items()
                        if k
                        in [
                            "timeout_seconds",
                            "max_concurrent_runs",
                            "queue",
                            "continuous",
                            "trigger",
                            "schedule",
                            "parameters",
                        ]
                    }
                )
                # Extract new job-level settings
                if "tags" in job_config:
                    default_settings["tags"] = job_config["tags"]
                if "job_clusters" in job_config:
                    default_settings["job_clusters"] = job_config["job_clusters"]
                if "_job_environments" in job_config:
                    default_settings["environments"] = job_config["_job_environments"]
                if "notification_settings" in job_config:
                    default_settings["notification_settings"] = job_config["notification_settings"]
        except (json.JSONDecodeError, TypeError):
            pass

        return default_settings

    def generate_tasks_for_job(self, job_name: str, return_first_task: bool = False):
        """Generate task configurations for a job based on metadata.

        Uses dependency resolution to determine execution order.

        Args:
            job_name: Name of the job to generate tasks for
            return_first_task: If True, also return first task dict for optimization

        Returns:
            List of task configuration dictionaries, optionally with first task dict

        Raises:
            ValueError: If job_name is invalid, no tasks found, circular dependencies, or invalid config
            RuntimeError: If control table doesn't exist or is inaccessible
        """
        if not job_name or not isinstance(job_name, str):
            raise ValueError("job_name must be a non-empty string")

        spark = _get_spark()
        try:
            job_tasks = spark.table(self.control_table).filter(F.col("job_name") == job_name).collect()
        except Exception as e:
            raise RuntimeError(f"Failed to read control table " f"'{self.control_table}': {str(e)}") from e

        if not job_tasks:
            raise ValueError(f"No tasks found for job '{job_name}'")

        # Convert rows to dicts and parse depends_on
        task_dicts: Dict[str, Dict[str, Any]] = {}
        for task_row in job_tasks:
            task = _task_row_to_dict(task_row)
            task_key = task["task_key"]

            # Parse depends_on JSON string
            depends_on_str = task.get("depends_on", "[]")
            try:
                depends_on = json.loads(depends_on_str) if isinstance(depends_on_str, str) else depends_on_str
                if depends_on is None:
                    depends_on = []
                if not isinstance(depends_on, list):
                    raise ValueError(f"Task '{task_key}' has invalid depends_on format")
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError(f"Task '{task_key}' has invalid depends_on JSON: {str(e)}") from e

            task["depends_on"] = depends_on
            task_dicts[task_key] = task

        # Validate all dependencies exist
        all_task_keys = set(task_dicts.keys())
        for task_key, task in task_dicts.items():
            for dep_key in task["depends_on"]:
                if dep_key not in all_task_keys:
                    raise ValueError(f"Task '{task_key}' depends on '{dep_key}' which does not exist")

        # Detect circular dependencies
        visited = set()
        rec_stack = set()

        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in task_dicts[node]["depends_on"]:
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for task_key in task_dicts:
            if task_key not in visited:
                if has_cycle(task_key):
                    raise ValueError(f"Job '{job_name}' has circular dependencies detected")

        # Topological sort to determine execution order
        in_degree: Dict[str, int] = {task_key: 0 for task_key in task_dicts}
        for task_key, task in task_dicts.items():
            for dep_key in task["depends_on"]:
                in_degree[task_key] += 1

        queue = [task_key for task_key, degree in in_degree.items() if degree == 0]
        sorted_tasks: List[str] = []

        while queue:
            task_key = queue.pop(0)
            sorted_tasks.append(task_key)

            for other_task_key, other_task in task_dicts.items():
                if task_key in other_task["depends_on"]:
                    in_degree[other_task_key] -= 1
                    if in_degree[other_task_key] == 0:
                        queue.append(other_task_key)

        if len(sorted_tasks) != len(task_dicts):
            raise ValueError(f"Job '{job_name}' has circular dependencies or invalid dependency graph")

        # Create task configurations in topological order
        tasks: List[Dict[str, Any]] = []
        first_task_dict = None

        for task_key in sorted_tasks:
            task = task_dicts[task_key]
            depends_on_list = task["depends_on"]

            try:
                task_config = create_task_from_config(
                    task_data=task,
                    control_table=self.control_table,
                    depends_on_task_keys=depends_on_list,
                    default_warehouse_id=self.default_warehouse_id,
                )
                tasks.append(task_config)

                if first_task_dict is None:
                    first_task_dict = task
            except Exception as e:
                logger.error(
                    "Failed to create task for task_key '%s': %s",
                    task_key,
                    str(e),
                )
                raise

        if return_first_task:
            return tasks, first_task_dict
        return tasks

    def create_or_update_job(
        self,
        job_name: str,
        cluster_id: Optional[str] = None,
        default_pause_status: bool = False,
    ) -> int:
        """Create new job or update existing job using stored job_id.

        Follows Databricks Jobs API requirements:
        - Uses JobSettings for both create and update operations
        - Tasks are required (will raise ValueError if empty)
        - Job name in Databricks will be the same as job_name from metadata

        Args:
            job_name: Name of the job (from job_name in YAML)
            cluster_id: Optional cluster ID to use for tasks (applies to all tasks)
            default_pause_status: Default pause state for jobs with triggers/schedules.
                Only applies if pause_status not explicitly set in metadata.
                For job updates, only applies if pause_status is explicitly set.

        Returns:
            The job ID (either updated or newly created)

        Raises:
            ValueError: If inputs are invalid or no tasks found
            RuntimeError: If job creation/update fails
        """
        if not job_name or not isinstance(job_name, str):
            raise ValueError("job_name must be a non-empty string")

        # Get stored job_id
        stored_job_id = self._get_stored_job_id(job_name)

        # Generate task definitions
        task_definitions, first_task_dict = self.generate_tasks_for_job(job_name, return_first_task=True)

        if not task_definitions or len(task_definitions) == 0:
            raise ValueError(f"No tasks found for job '{job_name}'. " f"Cannot create job without tasks.")

        # Get job settings (optimized: use first_task_dict to avoid extra query)
        job_settings_config = self.get_job_settings_for_job(job_name, first_task=first_task_dict)

        # Apply default pause status for jobs with triggers/schedules (create only)
        if not stored_job_id:
            job_settings_config = _apply_default_pause_status(
                job_settings_config, default_pause_status, is_update=False
            )

        sdk_task_objects = []
        sdk_task_dicts = []
        for task_def in task_definitions:
            sdk_task = convert_task_config_to_sdk_task(task_def)

            needs_query_creation = (
                hasattr(sdk_task, "sql_task")
                and sdk_task.sql_task
                and not sdk_task.sql_task.file
                and isinstance(sdk_task.sql_task.query, dict)
                and "query" in sdk_task.sql_task.query
                and "query_id" not in sdk_task.sql_task.query
            )
            if needs_query_creation:
                query_text = sdk_task.sql_task.query["query"]
                warehouse_id = sdk_task.sql_task.warehouse_id

                try:
                    from databricks.sdk.service.sql import CreateQueryRequestQuery

                    query_name = f"LakeflowJobMeta_{job_name}_{sdk_task.task_key}"

                    query_request_kwargs = {
                        "display_name": query_name,
                        "warehouse_id": warehouse_id,
                        "query_text": query_text,
                    }
                    if self.default_queries_path:
                        query_request_kwargs["parent_path"] = self.default_queries_path

                    created_query = self.workspace_client.queries.create(
                        query=CreateQueryRequestQuery(**query_request_kwargs)
                    )
                    query_id = created_query.id

                    if self.default_queries_path:
                        logger.debug(
                            "Created query '%s' (ID: %s) at path: %s",
                            query_name,
                            query_id,
                            self.default_queries_path,
                        )
                    else:
                        logger.debug("Created query '%s' (ID: %s)", query_name, query_id)

                    sdk_task.sql_task.query = SqlTaskQuery(query_id=query_id)
                except Exception as e:
                    logger.error(
                        "Failed to create query for task '%s': %s",
                        sdk_task.task_key,
                        str(e),
                    )
                    raise RuntimeError(f"Failed to create query for task " f"'{sdk_task.task_key}': {str(e)}") from e

            sdk_task_objects.append(sdk_task)
            task_dict = serialize_task_for_api(sdk_task)
            sdk_task_dicts.append(task_dict)

        # Try to update existing job first
        if stored_job_id:
            try:
                job_settings = JobSettingsWithDictTasks(
                    name=f"[Lakeflow Jobs Meta] {job_name}",
                    tasks=sdk_task_dicts,
                    max_concurrent_runs=job_settings_config["max_concurrent_runs"],
                    timeout_seconds=job_settings_config["timeout_seconds"],
                )

                # Add queue, continuous, trigger, and schedule if specified
                if job_settings_config.get("queue"):
                    job_settings.queue = _convert_job_setting_to_sdk_object("queue", job_settings_config["queue"])
                if job_settings_config.get("continuous"):
                    job_settings.continuous = _convert_job_setting_to_sdk_object(
                        "continuous",
                        job_settings_config["continuous"],
                    )
                if job_settings_config.get("trigger"):
                    job_settings.trigger = _convert_job_setting_to_sdk_object(
                        "trigger", job_settings_config["trigger"]
                    )
                if job_settings_config.get("schedule"):
                    job_settings.schedule = _convert_job_setting_to_sdk_object(
                        "schedule", job_settings_config["schedule"]
                    )

                # Add tags, job_clusters, environments, and notification_settings if specified
                if job_settings_config.get("tags"):
                    job_settings.tags = job_settings_config["tags"]
                job_clusters_config = job_settings_config.get("job_clusters")
                if job_clusters_config and isinstance(job_clusters_config, list):
                    job_clusters_list = []
                    for cluster_dict in job_clusters_config:
                        if isinstance(cluster_dict, dict):
                            new_cluster_dict = cluster_dict.get("new_cluster", {})
                            if ClusterSpec and new_cluster_dict:
                                try:
                                    new_cluster_dict_copy = new_cluster_dict.copy()

                                    aws_attrs_dict = new_cluster_dict_copy.pop("aws_attributes", None)
                                    aws_attrs_obj = None
                                    if AwsAttributes and aws_attrs_dict:
                                        try:
                                            aws_attrs_dict_copy = aws_attrs_dict.copy()
                                            if "availability" in aws_attrs_dict_copy and AwsAvailability:
                                                availability_str = aws_attrs_dict_copy["availability"]
                                                if isinstance(availability_str, str):
                                                    aws_attrs_dict_copy["availability"] = AwsAvailability(
                                                        availability_str
                                                    )
                                            aws_attrs_obj = AwsAttributes(**aws_attrs_dict_copy)
                                        except Exception:
                                            pass

                                    if aws_attrs_obj:
                                        new_cluster_dict_copy["aws_attributes"] = aws_attrs_obj

                                    if "data_security_mode" in new_cluster_dict_copy and DataSecurityMode:
                                        data_security_str = new_cluster_dict_copy["data_security_mode"]
                                        if isinstance(data_security_str, str):
                                            new_cluster_dict_copy["data_security_mode"] = DataSecurityMode(
                                                data_security_str
                                            )

                                    if "runtime_engine" in new_cluster_dict_copy and RuntimeEngine:
                                        runtime_str = new_cluster_dict_copy["runtime_engine"]
                                        if isinstance(runtime_str, str):
                                            new_cluster_dict_copy["runtime_engine"] = RuntimeEngine(runtime_str)

                                    new_cluster_obj = ClusterSpec(**new_cluster_dict_copy)
                                    job_clusters_list.append(
                                        JobCluster(
                                            job_cluster_key=cluster_dict["job_cluster_key"],
                                            new_cluster=new_cluster_obj,
                                        )
                                    )
                                except Exception:
                                    pass
                        elif hasattr(cluster_dict, "job_cluster_key"):
                            job_clusters_list.append(cluster_dict)
                    if job_clusters_list:
                        job_settings.job_clusters = job_clusters_list
                environments_config = job_settings_config.get("environments")
                if environments_config and isinstance(environments_config, list):
                    environments_list = []
                    for env_dict in environments_config:
                        if isinstance(env_dict, dict):
                            if JobEnvironment and ComputeEnvironment:
                                try:
                                    spec_dict = env_dict.get("spec", {})
                                    if spec_dict:
                                        spec = ComputeEnvironment(**spec_dict)
                                        env = JobEnvironment(environment_key=env_dict["environment_key"], spec=spec)
                                        environments_list.append(env)
                                except Exception:
                                    pass
                        elif hasattr(env_dict, "as_dict"):
                            environments_list.append(env_dict)
                        else:
                            environments_list.append(env_dict)
                    if environments_list:
                        job_settings.environments = environments_list
                notif_config = job_settings_config.get("notification_settings")
                if notif_config and isinstance(notif_config, dict):
                    email_notif_obj = None
                    if "email_notifications" in notif_config:
                        email_config = notif_config["email_notifications"]
                        if isinstance(email_config, dict):
                            email_notif_obj = JobEmailNotifications(
                                on_start=email_config.get("on_start", []),
                                on_success=email_config.get("on_success", []),
                                on_failure=email_config.get("on_failure", []),
                                on_duration_warning_threshold_exceeded=email_config.get(
                                    "on_duration_warning_threshold_exceeded", []
                                ),
                            )
                    job_settings.notification_settings = TaskNotificationSettings(
                        no_alert_for_skipped_runs=notif_config.get("no_alert_for_skipped_runs"),
                        no_alert_for_canceled_runs=notif_config.get("no_alert_for_canceled_runs"),
                        alert_on_last_attempt=notif_config.get("alert_on_last_attempt"),
                    )
                    if email_notif_obj:
                        job_settings.notification_settings.email_notifications = email_notif_obj

                if job_settings_config.get("parameters"):
                    job_settings.parameters = job_settings_config["parameters"]

                self.workspace_client.jobs.update(
                    job_id=stored_job_id,
                    new_settings=job_settings,
                )
                logger.info(
                    "Job '%s': Job updated successfully (Job ID: %d)",
                    job_name,
                    stored_job_id,
                )
                self._store_job_id(job_name, stored_job_id)
                return stored_job_id

            except Exception as e:
                error_str = str(e).lower()
                if "does not exist" in error_str or "not found" in error_str:
                    logger.warning(
                        "Job '%s': Stored job ID %d no longer exists. " "Creating new job...",
                        job_name,
                        stored_job_id,
                    )
                    try:
                        spark_del = _get_spark()
                        delta_table = DeltaTable.forName(spark_del, self.jobs_table)
                        delta_table.delete(F.col("job_name") == job_name)
                    except Exception as delete_error:
                        logger.warning(
                            "Could not delete invalid job_id record: %s",
                            str(delete_error),
                        )
                else:
                    logger.error(
                        "Job '%s': Error updating job %d: %s",
                        job_name,
                        stored_job_id,
                        str(e),
                    )
                    raise RuntimeError(
                        f"Failed to update job {stored_job_id} " f"for job '{job_name}': {str(e)}"
                    ) from e

        # Create new job
        try:
            job_settings_kwargs = {
                "name": f"[Lakeflow Jobs Meta] {job_name}",
                "tasks": sdk_task_objects,
                "max_concurrent_runs": job_settings_config["max_concurrent_runs"],
                "timeout_seconds": job_settings_config["timeout_seconds"],
            }

            if job_settings_config.get("queue"):
                job_settings_kwargs["queue"] = _convert_job_setting_to_sdk_object(
                    "queue", job_settings_config["queue"]
                )
            if job_settings_config.get("continuous"):
                job_settings_kwargs["continuous"] = _convert_job_setting_to_sdk_object(
                    "continuous", job_settings_config["continuous"]
                )
            if job_settings_config.get("trigger"):
                job_settings_kwargs["trigger"] = _convert_job_setting_to_sdk_object(
                    "trigger", job_settings_config["trigger"]
                )
            if job_settings_config.get("schedule"):
                job_settings_kwargs["schedule"] = _convert_job_setting_to_sdk_object(
                    "schedule", job_settings_config["schedule"]
                )

            # Add tags, job_clusters, environments, and notification_settings if specified
            if job_settings_config.get("tags"):
                job_settings_kwargs["tags"] = job_settings_config["tags"]
            job_clusters_config = job_settings_config.get("job_clusters")
            if job_clusters_config and isinstance(job_clusters_config, list):
                job_settings_kwargs["job_clusters"] = job_clusters_config
            environments_config = job_settings_config.get("environments")
            if environments_config and isinstance(environments_config, list):
                environments_list = []
                for env_dict in environments_config:
                    if isinstance(env_dict, dict):
                        if JobEnvironment and ComputeEnvironment:
                            try:
                                spec_dict = env_dict.get("spec", {})
                                if spec_dict:
                                    spec = ComputeEnvironment(**spec_dict)
                                    env = JobEnvironment(environment_key=env_dict["environment_key"], spec=spec)
                                    environments_list.append(env)
                            except Exception:
                                environments_list.append(env_dict)
                        else:
                            environments_list.append(env_dict)
                    elif hasattr(env_dict, "as_dict"):
                        environments_list.append(env_dict)
                    else:
                        environments_list.append(env_dict)
                if environments_list:
                    job_settings_kwargs["environments"] = environments_list
            notif_config = job_settings_config.get("notification_settings")
            if notif_config and isinstance(notif_config, dict):
                email_notif_obj = None
                if "email_notifications" in notif_config:
                    email_config = notif_config["email_notifications"]
                    if isinstance(email_config, dict):
                        email_notif_obj = JobEmailNotifications(
                            on_start=email_config.get("on_start", []),
                            on_success=email_config.get("on_success", []),
                            on_failure=email_config.get("on_failure", []),
                            on_duration_warning_threshold_exceeded=email_config.get(
                                "on_duration_warning_threshold_exceeded", []
                            ),
                        )
                job_settings_kwargs["notification_settings"] = TaskNotificationSettings(
                    no_alert_for_skipped_runs=notif_config.get("no_alert_for_skipped_runs"),
                    no_alert_for_canceled_runs=notif_config.get("no_alert_for_canceled_runs"),
                    alert_on_last_attempt=notif_config.get("alert_on_last_attempt"),
                )
                if email_notif_obj:
                    job_settings_kwargs["notification_settings"].email_notifications = email_notif_obj

            if job_settings_config.get("parameters"):
                job_settings_kwargs["parameters"] = job_settings_config["parameters"]

            original_cluster_spec_as_dict = ClusterSpec.as_dict if ClusterSpec else None
            original_job_cluster_as_dict = JobCluster.as_dict

            def patched_cluster_spec_as_dict(self):
                result = {}
                for key in dir(self):
                    if key.startswith("_") or callable(getattr(self, key, None)):
                        continue
                    try:
                        value = getattr(self, key, None)
                        if value is None:
                            continue
                        if isinstance(value, dict):
                            result[key] = value
                        elif hasattr(value, "as_dict"):
                            try:
                                result[key] = value.as_dict()
                            except (AttributeError, TypeError):
                                result[key] = value
                        elif hasattr(value, "value"):
                            result[key] = value.value
                        else:
                            result[key] = value
                    except Exception:
                        continue
                return result

            def patched_job_cluster_as_dict(self):
                result = {}
                if self.job_cluster_key:
                    result["job_cluster_key"] = self.job_cluster_key
                if hasattr(self, "new_cluster") and self.new_cluster:
                    if isinstance(self.new_cluster, dict):
                        result["new_cluster"] = self.new_cluster
                    elif hasattr(self.new_cluster, "as_dict"):
                        try:
                            result["new_cluster"] = self.new_cluster.as_dict()
                        except (AttributeError, TypeError):
                            result["new_cluster"] = self.new_cluster
                    else:
                        result["new_cluster"] = self.new_cluster
                return result

            if ClusterSpec:
                ClusterSpec.as_dict = patched_cluster_spec_as_dict
            JobCluster.as_dict = patched_job_cluster_as_dict

            try:
                if "job_clusters" in job_settings_kwargs:
                    job_clusters_list = []
                    for cluster_dict in job_settings_kwargs["job_clusters"]:
                        new_cluster_dict = cluster_dict.get("new_cluster", {})

                        if ClusterSpec and new_cluster_dict:
                            aws_attrs_dict = new_cluster_dict.pop("aws_attributes", None)
                            aws_attrs_obj = None
                            if AwsAttributes and aws_attrs_dict:
                                try:
                                    aws_attrs_dict_copy = aws_attrs_dict.copy()
                                    if "availability" in aws_attrs_dict_copy and AwsAvailability:
                                        availability_str = aws_attrs_dict_copy["availability"]
                                        if isinstance(availability_str, str):
                                            aws_attrs_dict_copy["availability"] = AwsAvailability(availability_str)
                                    aws_attrs_obj = AwsAttributes(**aws_attrs_dict_copy)
                                except Exception:
                                    new_cluster_dict["aws_attributes"] = aws_attrs_dict

                            try:
                                if aws_attrs_obj:
                                    new_cluster_dict["aws_attributes"] = aws_attrs_obj

                                if "data_security_mode" in new_cluster_dict and DataSecurityMode:
                                    data_security_str = new_cluster_dict["data_security_mode"]
                                    if isinstance(data_security_str, str):
                                        new_cluster_dict["data_security_mode"] = DataSecurityMode(data_security_str)

                                if "runtime_engine" in new_cluster_dict and RuntimeEngine:
                                    runtime_str = new_cluster_dict["runtime_engine"]
                                    if isinstance(runtime_str, str):
                                        new_cluster_dict["runtime_engine"] = RuntimeEngine(runtime_str)

                                new_cluster_obj = ClusterSpec(**new_cluster_dict)
                                cluster_dict["new_cluster"] = new_cluster_obj
                            except Exception:
                                pass

                        job_cluster = JobCluster(**cluster_dict)
                        job_clusters_list.append(job_cluster)
                    job_settings_kwargs["job_clusters"] = job_clusters_list

                created_job = self.workspace_client.jobs.create(**job_settings_kwargs)
            finally:
                if ClusterSpec and original_cluster_spec_as_dict:
                    ClusterSpec.as_dict = original_cluster_spec_as_dict
                JobCluster.as_dict = original_job_cluster_as_dict
            created_job_id = created_job.job_id
            if not created_job_id:
                raise RuntimeError(f"Job creation succeeded but no job_id returned: " f"{created_job}")

            logger.info(
                "Job '%s': Job created successfully (Job ID: %d)",
                job_name,
                created_job_id,
            )

            self._store_job_id(job_name, created_job_id)

            return created_job_id

        except Exception as e:
            logger.error("Job '%s': Error creating job: %s", job_name, str(e))
            raise RuntimeError(f"Failed to create job for job '{job_name}': {str(e)}") from e

    def create_or_update_jobs(
        self,
        default_pause_status: bool = False,
        yaml_path: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Create and optionally run jobs.

        Args:
            default_pause_status: Default pause state for jobs with triggers/schedules.
                When False (default): Jobs are created active and run immediately.
                When True: Jobs with continuous/schedule/trigger are created paused.
                Can be overridden by explicit pause_status in YAML metadata.
                For updates, only applies if pause_status is explicitly set in metadata.
            yaml_path: Optional path to load metadata from before orchestrating.
                Can be:
                - Path to a YAML file (e.g., "/Workspace/path/to/metadata.yaml")
                - Path to a folder (e.g., "/Workspace/path/to/metadata/") - loads all YAML files
                - Path to a Unity Catalog volume (e.g., "/Volumes/catalog/schema/volume")
                If provided: Only jobs from the yaml_path are processed.
                If not provided: All jobs in control table are processed.

        Returns:
            List of dictionaries with job names and job IDs
        """
        logger.info(
            "Starting orchestration with control_table: %s",
            self.control_table,
        )

        # Ensure tables exist
        self.ensure_setup()

        # Load metadata and track which jobs to process
        jobs_to_process = None  # None = process all jobs

        if yaml_path:
            try:
                # Detect path type and load accordingly
                if yaml_path.startswith("/Volumes/"):
                    # Load from Unity Catalog volume
                    tasks_loaded, job_names = self.sync_from_volume(yaml_path)
                    jobs_to_process = job_names
                    logger.info(
                        "Loaded %d tasks from volume: %s (Jobs: %s)",
                        tasks_loaded,
                        yaml_path,
                        ", ".join(job_names) if job_names else "none",
                    )
                elif yaml_path.endswith((".yaml", ".yml")):
                    # Load from specific YAML file
                    tasks_loaded, job_names = self.metadata_manager.load_yaml(yaml_path)
                    jobs_to_process = job_names
                    logger.info(
                        "Loaded %d tasks from YAML file: %s (Jobs: %s)",
                        tasks_loaded,
                        yaml_path,
                        ", ".join(job_names) if job_names else "none",
                    )
                else:
                    # Check if it's a folder
                    import os

                    if os.path.isdir(yaml_path):
                        # Load from folder (all YAML files in folder)
                        tasks_loaded, job_names = self.metadata_manager.load_from_folder(yaml_path)
                        jobs_to_process = job_names
                        logger.info(
                            "Loaded %d tasks from folder: %s (Jobs: %s)",
                            tasks_loaded,
                            yaml_path,
                            ", ".join(job_names) if job_names else "none",
                        )
                    else:
                        raise ValueError(
                            f"Path '{yaml_path}' is not a valid YAML file, folder, or volume path. "
                            f"Expected: .yaml/.yml file, folder path, or /Volumes/... path"
                        )
            except FileNotFoundError:
                logger.warning(
                    "Path not found: %s. No jobs will be processed.",
                    yaml_path,
                )
                return []
            except Exception as e:
                logger.warning(
                    "Failed to load from path: %s. Error: %s. No jobs will be processed.",
                    yaml_path,
                    str(e),
                )
                return []

        # Get jobs to process
        if jobs_to_process is not None:
            # Only process jobs that were loaded from yaml_path
            jobs = jobs_to_process
            if not jobs:
                logger.warning("No jobs found in provided path '%s'", yaml_path)
                return []
        else:
            # Process all jobs in control table
            jobs = self.metadata_manager.get_all_jobs()

        if not jobs:
            logger.warning("No jobs found in control table '%s'", self.control_table)
            return []

        created_jobs = []
        failed_jobs = []

        for job_name in jobs:
            try:
                job_id = self.create_or_update_job(job_name, default_pause_status=default_pause_status)
                created_jobs.append({"job": job_name, "job_id": job_id})

                # Run jobs immediately unless default_pause_status is True
                if not default_pause_status:
                    try:
                        run_result = self.workspace_client.jobs.run_now(job_id=job_id)
                        logger.info(
                            "Job '%s': Started job run " "(Job ID: %d, Run ID: %d)",
                            job_name,
                            job_id,
                            run_result.run_id,
                        )
                    except Exception as run_error:
                        logger.error(
                            "Job '%s': Failed to start job run " "(Job ID: %d): %s",
                            job_name,
                            job_id,
                            str(run_error),
                        )

            except Exception as e:
                logger.error(
                    "Job '%s': Failed to create/run job: %s",
                    job_name,
                    str(e),
                )
                failed_jobs.append({"job": job_name, "error": str(e)})

        if failed_jobs:
            logger.warning(
                "Failed to process %d job(s): %s",
                len(failed_jobs),
                failed_jobs,
            )

        logger.info(
            "Orchestration completed. Managed %d job(s) successfully",
            len(created_jobs),
        )
        return created_jobs
