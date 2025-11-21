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
    JobEditMode,
    JobParameterDefinition,
)

try:
    from databricks.sdk.service.jobs import (
        FileArrivalTriggerConfiguration,
        PeriodicTriggerConfiguration,
        TableUpdateTriggerConfiguration,
    )
except ImportError:
    FileArrivalTriggerConfiguration = None
    PeriodicTriggerConfiguration = None
    TableUpdateTriggerConfiguration = None

try:
    from databricks.sdk.service.compute import (
        ClusterSpec,
        Environment as ComputeEnvironment,
        AwsAttributes,
        AwsAvailability,
        EbsVolumeType,
        AzureAttributes,
        AzureAvailability,
        GcpAttributes,
        GcpAvailability,
        DataSecurityMode,
        RuntimeEngine,
    )
except ImportError:
    ClusterSpec = None
    ComputeEnvironment = None
    AwsAttributes = None
    AwsAvailability = None
    EbsVolumeType = None
    AzureAttributes = None
    AzureAvailability = None
    GcpAttributes = None
    GcpAvailability = None
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
            # Convert JobParameterDefinition objects to dicts
            if isinstance(self.parameters, list):
                result["parameters"] = []
                for param in self.parameters:
                    if isinstance(param, dict):
                        result["parameters"].append(param)
                    elif hasattr(param, "as_dict"):
                        try:
                            result["parameters"].append(param.as_dict())
                        except (AttributeError, TypeError):
                            result["parameters"].append(param)
                    else:
                        result["parameters"].append(param)
            else:
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

    Logic:
    - If pause_status is explicitly set in continuous/schedule/trigger: Use that value (create & update)
    - If pause_status is NOT set and is_update=False (creation): Apply default_pause_status
    - If pause_status is NOT set and is_update=True (update): Don't modify (leave unchanged)

    Args:
        job_config: Job configuration dictionary
        default_pause_status: Default pause status to apply (only for creation)
        is_update: Whether this is an update operation

    Returns:
        Modified job configuration
    """
    # For updates, only apply pause_status if explicitly set in metadata
    # If not explicitly set, leave it alone (don't modify existing job's pause_status)
    if is_update:
        return job_config

    # For creates, apply default_pause_status if not explicitly set in the setting
    for setting_key in ["continuous", "schedule", "trigger"]:
        if setting_key in job_config:
            setting = job_config[setting_key]
            if isinstance(setting, dict) and "pause_status" not in setting:
                pause_value = "PAUSED" if default_pause_status else "UNPAUSED"
                setting["pause_status"] = pause_value

    return job_config


def _convert_cloud_attributes(new_cluster_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Convert cloud provider attributes to SDK objects with enum handling.

    Args:
        new_cluster_dict: Dictionary with new_cluster configuration

    Returns:
        Modified dictionary with cloud attributes as SDK objects
    """
    new_cluster_dict_copy = new_cluster_dict.copy()

    # Process AWS attributes
    aws_attrs_dict = new_cluster_dict_copy.pop("aws_attributes", None)
    if AwsAttributes and aws_attrs_dict:
        try:
            aws_attrs_dict_copy = aws_attrs_dict.copy()
            if "availability" in aws_attrs_dict_copy and AwsAvailability:
                availability_str = aws_attrs_dict_copy["availability"]
                if isinstance(availability_str, str):
                    aws_attrs_dict_copy["availability"] = AwsAvailability(availability_str)
            if "ebs_volume_type" in aws_attrs_dict_copy and EbsVolumeType:
                ebs_type_str = aws_attrs_dict_copy["ebs_volume_type"]
                if isinstance(ebs_type_str, str):
                    aws_attrs_dict_copy["ebs_volume_type"] = EbsVolumeType(ebs_type_str)
            new_cluster_dict_copy["aws_attributes"] = AwsAttributes(**aws_attrs_dict_copy)
        except Exception:
            new_cluster_dict_copy["aws_attributes"] = aws_attrs_dict

    # Process Azure attributes
    azure_attrs_dict = new_cluster_dict_copy.pop("azure_attributes", None)
    if AzureAttributes and azure_attrs_dict:
        try:
            azure_attrs_dict_copy = azure_attrs_dict.copy()
            if "availability" in azure_attrs_dict_copy and AzureAvailability:
                availability_str = azure_attrs_dict_copy["availability"]
                if isinstance(availability_str, str):
                    azure_attrs_dict_copy["availability"] = AzureAvailability(availability_str)
            new_cluster_dict_copy["azure_attributes"] = AzureAttributes(**azure_attrs_dict_copy)
        except Exception:
            new_cluster_dict_copy["azure_attributes"] = azure_attrs_dict

    # Process GCP attributes
    gcp_attrs_dict = new_cluster_dict_copy.pop("gcp_attributes", None)
    if GcpAttributes and gcp_attrs_dict:
        try:
            gcp_attrs_dict_copy = gcp_attrs_dict.copy()
            if "availability" in gcp_attrs_dict_copy and GcpAvailability:
                availability_str = gcp_attrs_dict_copy["availability"]
                if isinstance(availability_str, str):
                    gcp_attrs_dict_copy["availability"] = GcpAvailability(availability_str)
            new_cluster_dict_copy["gcp_attributes"] = GcpAttributes(**gcp_attrs_dict_copy)
        except Exception:
            new_cluster_dict_copy["gcp_attributes"] = gcp_attrs_dict

    # Process data security mode
    if "data_security_mode" in new_cluster_dict_copy and DataSecurityMode:
        data_security_str = new_cluster_dict_copy["data_security_mode"]
        if isinstance(data_security_str, str):
            new_cluster_dict_copy["data_security_mode"] = DataSecurityMode(data_security_str)

    # Process runtime engine
    if "runtime_engine" in new_cluster_dict_copy and RuntimeEngine:
        runtime_str = new_cluster_dict_copy["runtime_engine"]
        if isinstance(runtime_str, str):
            new_cluster_dict_copy["runtime_engine"] = RuntimeEngine(runtime_str)

    return new_cluster_dict_copy


def _process_job_clusters(job_clusters_config: List[Dict[str, Any]], job_name: str) -> List[JobCluster]:
    """Process job clusters configuration into SDK objects.

    Args:
        job_clusters_config: List of job cluster dictionaries
        job_name: Name of the job (for logging)

    Returns:
        List of JobCluster SDK objects
    """
    job_clusters_list = []

    for cluster_dict in job_clusters_config:
        if isinstance(cluster_dict, dict):
            new_cluster_dict = cluster_dict.get("new_cluster", {})
            if ClusterSpec and new_cluster_dict:
                try:
                    new_cluster_dict_converted = _convert_cloud_attributes(new_cluster_dict)
                    new_cluster_obj = ClusterSpec(**new_cluster_dict_converted)
                    job_clusters_list.append(
                        JobCluster(
                            job_cluster_key=cluster_dict["job_cluster_key"],
                            new_cluster=new_cluster_obj,
                        )
                    )
                except Exception as e:
                    logger.warning(
                        f"Job '{job_name}': Failed to create job cluster "
                        f"'{cluster_dict.get('job_cluster_key')}': {e}"
                    )
            else:
                try:
                    job_clusters_list.append(JobCluster(**cluster_dict))
                except Exception:
                    pass
        elif hasattr(cluster_dict, "job_cluster_key"):
            job_clusters_list.append(cluster_dict)

    return job_clusters_list


def _process_environments(environments_config: List[Dict[str, Any]]) -> List:
    """Process environments configuration into SDK objects.

    Args:
        environments_config: List of environment dictionaries

    Returns:
        List of JobEnvironment SDK objects or dicts
    """
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
                    else:
                        environments_list.append(env_dict)
                except Exception:
                    environments_list.append(env_dict)
            else:
                environments_list.append(env_dict)
        elif hasattr(env_dict, "as_dict"):
            environments_list.append(env_dict)
        else:
            environments_list.append(env_dict)

    return environments_list


def _process_notification_settings(notif_config: Dict[str, Any]) -> TaskNotificationSettings:
    """Process notification settings configuration into SDK object.

    Args:
        notif_config: Notification settings dictionary

    Returns:
        TaskNotificationSettings SDK object
    """
    email_notif_obj = None
    if "email_notifications" in notif_config:
        email_config = notif_config["email_notifications"]
        if isinstance(email_config, dict):
            email_notif_obj = JobEmailNotifications(
                on_start=email_config.get("on_start", []),
                on_success=email_config.get("on_success", []),
                on_failure=email_config.get("on_failure", []),
                on_duration_warning_threshold_exceeded=email_config.get("on_duration_warning_threshold_exceeded", []),
            )

    notification_settings = TaskNotificationSettings(
        no_alert_for_skipped_runs=notif_config.get("no_alert_for_skipped_runs"),
        no_alert_for_canceled_runs=notif_config.get("no_alert_for_canceled_runs"),
        alert_on_last_attempt=notif_config.get("alert_on_last_attempt"),
    )

    if email_notif_obj:
        notification_settings.email_notifications = email_notif_obj

    return notification_settings


def _process_parameters(params_list: List) -> List:
    """Process parameters configuration into SDK objects.

    Args:
        params_list: List of parameter dictionaries or objects

    Returns:
        List of JobParameterDefinition SDK objects
    """
    job_params = []

    for param_dict in params_list:
        if isinstance(param_dict, dict):
            job_param = JobParameterDefinition(
                name=param_dict.get("name"),
                default=param_dict.get("default"),
            )
            job_params.append(job_param)
        else:
            job_params.append(param_dict)

    return job_params


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
            continuous_kwargs = {}
            if pause_status:
                continuous_kwargs["pause_status"] = pause_status
            if task_retry_mode:
                continuous_kwargs["task_retry_mode"] = task_retry_mode
            elif TaskRetryMode:
                continuous_kwargs["task_retry_mode"] = TaskRetryMode.ON_FAILURE
            return Continuous(**continuous_kwargs)
        elif setting_type == "queue":
            return QueueSettings(**setting_dict)
        elif setting_type == "trigger":
            # Handle pause_status enum conversion and nested trigger configurations
            trigger_kwargs = {}

            # Handle pause_status
            if "pause_status" in setting_dict and isinstance(setting_dict["pause_status"], str):
                trigger_kwargs["pause_status"] = PauseStatus(setting_dict["pause_status"])
            elif "pause_status" in setting_dict:
                trigger_kwargs["pause_status"] = setting_dict["pause_status"]

            # Handle file_arrival
            if "file_arrival" in setting_dict and FileArrivalTriggerConfiguration:
                file_arrival_dict = setting_dict["file_arrival"]
                if isinstance(file_arrival_dict, dict):
                    trigger_kwargs["file_arrival"] = FileArrivalTriggerConfiguration(**file_arrival_dict)
                else:
                    trigger_kwargs["file_arrival"] = file_arrival_dict

            # Handle periodic
            if "periodic" in setting_dict and PeriodicTriggerConfiguration:
                periodic_dict = setting_dict["periodic"]
                if isinstance(periodic_dict, dict):
                    trigger_kwargs["periodic"] = PeriodicTriggerConfiguration(**periodic_dict)
                else:
                    trigger_kwargs["periodic"] = periodic_dict

            # Handle table_update
            if "table_update" in setting_dict and TableUpdateTriggerConfiguration:
                table_update_dict = setting_dict["table_update"]
                if isinstance(table_update_dict, dict):
                    trigger_kwargs["table_update"] = TableUpdateTriggerConfiguration(**table_update_dict)
                else:
                    trigger_kwargs["table_update"] = table_update_dict

            return TriggerSettings(**trigger_kwargs)
        elif setting_type == "schedule":
            # Handle pause_status enum conversion
            schedule_kwargs = dict(setting_dict)
            if "pause_status" in schedule_kwargs and isinstance(schedule_kwargs["pause_status"], str):
                schedule_kwargs["pause_status"] = PauseStatus(schedule_kwargs["pause_status"])
            return CronSchedule(**schedule_kwargs)
    except (TypeError, ValueError, AttributeError) as e:
        logger.warning(f"Failed to convert {setting_type} to SDK object: {e}. Using dict representation.")
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
                    resource_id STRING,
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

    def _get_stored_job_id(self, resource_id: str) -> Optional[int]:
        """Get stored job_id for a job from Delta table using resource_id (internal)."""
        spark = _get_spark()
        try:
            job_id_rows = (
                spark.table(self.jobs_table)
                .filter(F.col("resource_id") == resource_id)
                .select("job_id")
                .limit(1)
                .collect()
            )

            if job_id_rows:
                return job_id_rows[0]["job_id"]
            return None
        except Exception as e:
            logger.warning(
                "Job (resource_id='%s'): Could not retrieve job_id: %s",
                resource_id,
                str(e),
            )
            return None

    def _store_job_id(self, resource_id: str, job_name: str, job_id: int) -> None:
        """Store/update job_id for a job in Delta table (internal)."""
        spark = _get_spark()
        try:
            current_user = _get_current_user()
            source_data = [
                Row(
                    resource_id=resource_id,
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
                ON target.resource_id = source.resource_id
                WHEN MATCHED THEN
                    UPDATE SET
                        job_name = source.job_name,
                        job_id = source.job_id,
                        updated_by = source.updated_by,
                        updated_timestamp = source.updated_timestamp
                WHEN NOT MATCHED THEN
                    INSERT (
                        resource_id, job_name, job_id, created_by, updated_by,
                        updated_timestamp
                    )
                    VALUES (
                        source.resource_id, source.job_name, source.job_id, source.created_by,
                        source.updated_by, source.updated_timestamp
                    )
            """
            )

            logger.info("Job '%s' (resource_id='%s'): Stored job_id %d", job_name, resource_id, job_id)
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
            "edit_mode": None,
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
                            "edit_mode",
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

    def generate_tasks_for_job(self, resource_id: str, return_first_task: bool = False):
        """Generate task configurations for a job based on metadata.

        Uses dependency resolution to determine execution order.

        Args:
            resource_id: Resource ID of the job (YAML dict key)
            return_first_task: If True, also return first task dict for optimization

        Returns:
            List of task configuration dictionaries, optionally with first task dict

        Raises:
            ValueError: If resource_id is invalid, no tasks found, circular dependencies, or invalid config
            RuntimeError: If control table doesn't exist or is inaccessible
        """
        if not resource_id or not isinstance(resource_id, str):
            raise ValueError("resource_id must be a non-empty string")

        spark = _get_spark()
        try:
            job_tasks = spark.table(self.control_table).filter(F.col("resource_id") == resource_id).collect()
        except Exception as e:
            raise RuntimeError(f"Failed to read control table " f"'{self.control_table}': {str(e)}") from e

        if not job_tasks:
            raise ValueError(f"No tasks found for resource_id '{resource_id}'")

        # Get job_name from the first task (all tasks in a job have the same job_name)
        job_name = job_tasks[0]["job_name"]

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

    def sync_from_volume(self, volume_path: str, var: Optional[Dict[str, Any]] = None):
        """Load metadata from Unity Catalog volume.

        Delegates to MetadataManager.sync_from_volume.

        Args:
            volume_path: Path to Unity Catalog volume (e.g., "/Volumes/catalog/schema/volume")
            var: Optional dictionary of variables for ${var.name} substitution

        Returns:
            Tuple of (tasks_loaded, job_names)
        """
        return self.metadata_manager.sync_from_volume(volume_path, var=var)

    def create_or_update_job(
        self,
        resource_id: str,
        default_pause_status: bool = False,
    ) -> int:
        """Create new job or update existing job using stored job_id.

        Follows Databricks Jobs API requirements:
        - Uses JobSettings for both create and update operations
        - Tasks are required (will raise ValueError if empty)
        - Job name in Databricks comes from 'name' field or defaults to resource_id

        Args:
            resource_id: Resource ID of the job (YAML dict key)
            default_pause_status: Default pause state for jobs with triggers/schedules.
                Only applies if pause_status not explicitly set in metadata.
                For job updates, only applies if pause_status is explicitly set.

        Returns:
            The job ID (either updated or newly created)

        Raises:
            ValueError: If inputs are invalid or no tasks found
            RuntimeError: If job creation/update fails
        """
        if not resource_id or not isinstance(resource_id, str):
            raise ValueError("resource_id must be a non-empty string")

        # Get job_name from control table (first task's job_name)
        spark = _get_spark()
        try:
            first_task_row = (
                spark.table(self.control_table).filter(F.col("resource_id") == resource_id).limit(1).collect()
            )
            if not first_task_row:
                raise ValueError(f"No tasks found for resource_id '{resource_id}'")
            first_task = _task_row_to_dict(first_task_row[0])
            job_name = first_task["job_name"]
        except Exception as e:
            raise ValueError(f"Failed to get job_name for resource_id '{resource_id}': {str(e)}") from e

        # Get stored job_id from jobs table using resource_id
        stored_job_id = self._get_stored_job_id(resource_id)

        # Verify if job actually exists in Databricks
        job_exists_in_databricks = False
        if stored_job_id:
            try:
                self.workspace_client.jobs.get(job_id=stored_job_id)
                job_exists_in_databricks = True
            except Exception as e:
                error_str = str(e).lower()
                if "does not exist" in error_str or "not found" in error_str:
                    logger.warning(
                        "Job '%s': stored_job_id=%s does not exist in Databricks (will CREATE with new job_id)",
                        job_name,
                        stored_job_id,
                    )
                    stored_job_id = None
                else:
                    raise

        # Generate task definitions
        task_definitions, first_task_dict = self.generate_tasks_for_job(resource_id, return_first_task=True)

        if not task_definitions or len(task_definitions) == 0:
            raise ValueError(f"No tasks found for job '{job_name}'. " f"Cannot create job without tasks.")

        # Get job settings (optimized: use first_task_dict to avoid extra query)
        job_settings_config = self.get_job_settings_for_job(job_name, first_task=first_task_dict)

        # Apply default pause status for jobs with triggers/schedules (create only)
        if not job_exists_in_databricks:
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

        # Update existing job
        if job_exists_in_databricks:
            # Get edit_mode from job config, default to UI_LOCKED
            edit_mode_str = job_settings_config.get("edit_mode", "UI_LOCKED")
            if isinstance(edit_mode_str, str):
                edit_mode = JobEditMode.UI_LOCKED if edit_mode_str == "UI_LOCKED" else JobEditMode.EDITABLE
            else:
                edit_mode = JobEditMode.UI_LOCKED

            job_settings = JobSettingsWithDictTasks(
                name=f"[Lakeflow Jobs Meta] {job_name}",
                tasks=sdk_task_dicts,
                max_concurrent_runs=job_settings_config["max_concurrent_runs"],
                timeout_seconds=job_settings_config["timeout_seconds"],
                edit_mode=edit_mode,
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
                job_settings.trigger = _convert_job_setting_to_sdk_object("trigger", job_settings_config["trigger"])
            if job_settings_config.get("schedule"):
                job_settings.schedule = _convert_job_setting_to_sdk_object("schedule", job_settings_config["schedule"])

            # Add tags, job_clusters, environments, and notification_settings if specified
            if job_settings_config.get("tags"):
                job_settings.tags = job_settings_config["tags"]

            # Process job clusters
            job_clusters_config = job_settings_config.get("job_clusters")
            if job_clusters_config and isinstance(job_clusters_config, list):
                job_clusters_list = _process_job_clusters(job_clusters_config, job_name)
                if job_clusters_list:
                    job_settings.job_clusters = job_clusters_list

            # Process environments
            environments_config = job_settings_config.get("environments")
            if environments_config and isinstance(environments_config, list):
                environments_list = _process_environments(environments_config)
                if environments_list:
                    job_settings.environments = environments_list

            # Process notification settings
            notif_config = job_settings_config.get("notification_settings")
            if notif_config and isinstance(notif_config, dict):
                job_settings.notification_settings = _process_notification_settings(notif_config)

            # Process parameters
            if job_settings_config.get("parameters"):
                params_list = job_settings_config["parameters"]
                if isinstance(params_list, list):
                    job_settings.parameters = _process_parameters(params_list)
                else:
                    job_settings.parameters = params_list

            self.workspace_client.jobs.reset(
                job_id=stored_job_id,
                new_settings=job_settings,
            )
            logger.info(
                "Job '%s': Job updated successfully (Job ID: %d)",
                job_name,
                stored_job_id,
            )
            self._store_job_id(resource_id, job_name, stored_job_id)
            return stored_job_id

        # Create new job
        else:
            try:
                # Get edit_mode from job config, default to UI_LOCKED
                edit_mode_str = job_settings_config.get("edit_mode", "UI_LOCKED")
                if isinstance(edit_mode_str, str):
                    edit_mode = JobEditMode.UI_LOCKED if edit_mode_str == "UI_LOCKED" else JobEditMode.EDITABLE
                else:
                    edit_mode = JobEditMode.UI_LOCKED

                job_settings_kwargs = {
                    "name": f"[Lakeflow Jobs Meta] {job_name}",
                    "tasks": sdk_task_objects,
                    "max_concurrent_runs": job_settings_config["max_concurrent_runs"],
                    "timeout_seconds": job_settings_config["timeout_seconds"],
                    "edit_mode": edit_mode,
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

                # Process job clusters
                job_clusters_config = job_settings_config.get("job_clusters")
                if job_clusters_config and isinstance(job_clusters_config, list):
                    job_clusters_list = _process_job_clusters(job_clusters_config, job_name)
                    if job_clusters_list:
                        job_settings_kwargs["job_clusters"] = job_clusters_list

                # Process environments
                environments_config = job_settings_config.get("environments")
                if environments_config and isinstance(environments_config, list):
                    environments_list = _process_environments(environments_config)
                    if environments_list:
                        job_settings_kwargs["environments"] = environments_list

                # Process notification settings
                notif_config = job_settings_config.get("notification_settings")
                if notif_config and isinstance(notif_config, dict):
                    job_settings_kwargs["notification_settings"] = _process_notification_settings(notif_config)

                # Process parameters
                if job_settings_config.get("parameters"):
                    params_list = job_settings_config["parameters"]
                    if isinstance(params_list, list):
                        job_settings_kwargs["parameters"] = _process_parameters(params_list)
                    else:
                        job_settings_kwargs["parameters"] = params_list

                created_job = self.workspace_client.jobs.create(**job_settings_kwargs)

                created_job_id = created_job.job_id
                if not created_job_id:
                    raise RuntimeError(f"Job creation succeeded but no job_id returned: " f"{created_job}")

                logger.info(
                    "Job '%s': Job created successfully (Job ID: %d)",
                    job_name,
                    created_job_id,
                )

                self._store_job_id(resource_id, job_name, created_job_id)

                # Auto-run newly created jobs if default_pause_status=False
                # Only for jobs without schedule/trigger/continuous (manual/on-demand jobs)
                if not default_pause_status:
                    has_schedule_or_trigger = (
                        job_settings_config.get("schedule") is not None
                        or job_settings_config.get("trigger") is not None
                        or job_settings_config.get("continuous") is not None
                    )
                    if not has_schedule_or_trigger:
                        try:
                            run_result = self.workspace_client.jobs.run_now(job_id=created_job_id)
                            logger.info(
                                "Job '%s': Started initial job run (Job ID: %d, Run ID: %d)",
                                job_name,
                                created_job_id,
                                run_result.run_id,
                            )
                        except Exception as run_error:
                            logger.warning(
                                "Job '%s': Failed to start initial job run (Job ID: %d): %s",
                                job_name,
                                created_job_id,
                                str(run_error),
                            )

                return created_job_id

            except Exception as e:
                logger.error("Job '%s': Error creating job: %s", job_name, str(e))
                raise RuntimeError(f"Failed to create job for job '{job_name}': {str(e)}") from e

    def create_or_update_jobs(
        self,
        default_pause_status: bool = False,
        yaml_path: Optional[str] = None,
        var: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Create and optionally run jobs.

        Args:
            default_pause_status: Controls initial behavior for newly created jobs.
                When False (default):
                    - Manual jobs (no schedule/trigger/continuous): Auto-run immediately after creation
                    - Jobs with schedule/trigger/continuous: Created active (UNPAUSED)
                When True:
                    - Manual jobs: Do NOT auto-run
                    - Jobs with schedule/trigger/continuous: Created paused (PAUSED)
                For job UPDATES: Has NO effect (never auto-runs on updates).
                Can be overridden by explicit pause_status in YAML metadata.
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
                    tasks_loaded, job_names = self.sync_from_volume(yaml_path, var=var)
                    jobs_to_process = job_names
                    logger.info(
                        "Loaded %d tasks from volume: %s (Jobs: %s)",
                        tasks_loaded,
                        yaml_path,
                        ", ".join(job_names) if job_names else "none",
                    )
                elif yaml_path.endswith((".yaml", ".yml")):
                    # Load from specific YAML file
                    tasks_loaded, job_names = self.metadata_manager.load_yaml(yaml_path, var=var)
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
                        tasks_loaded, job_names = self.metadata_manager.load_from_folder(yaml_path, var=var)
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
                error_msg = str(e)
                if "Failed to load YAML files from" in error_msg and "Error in file" in error_msg:
                    import re

                    match = re.search(r"Error in file '[^']+': (.+?)\. No data", error_msg)
                    if match:
                        core_error = match.group(1).rstrip(".")  # Remove trailing period
                        logger.error(
                            "Failed to load YAML from '%s': %s. No jobs will be processed.",
                            yaml_path,
                            core_error,
                        )
                    else:
                        logger.error("Failed to load from '%s': %s. No jobs will be processed.", yaml_path, error_msg)
                else:
                    logger.error("Failed to load from '%s': %s. No jobs will be processed.", yaml_path, error_msg)
                return []

        # Get resource_ids to process
        if jobs_to_process is not None:
            # Only process jobs that were loaded from yaml_path (these are resource_ids now)
            resource_ids = jobs_to_process
            if not resource_ids:
                logger.warning("No jobs found in provided path '%s'", yaml_path)
                return []
        else:
            # Process all jobs in control table (get distinct resource_ids)
            resource_ids = self.metadata_manager.get_all_jobs()

        if not resource_ids:
            logger.warning("No jobs found in control table '%s'", self.control_table)
            return []

        created_jobs = []
        failed_jobs = []

        for resource_id in resource_ids:
            try:
                job_id = self.create_or_update_job(resource_id, default_pause_status=default_pause_status)
                # Get job_name for logging
                spark = _get_spark()
                first_task = (
                    spark.table(self.control_table).filter(F.col("resource_id") == resource_id).limit(1).collect()
                )
                job_name = _task_row_to_dict(first_task[0])["job_name"] if first_task else resource_id
                created_jobs.append({"resource_id": resource_id, "job_name": job_name, "job_id": job_id})

            except Exception as e:
                logger.error(
                    "Job (resource_id='%s'): Failed to create/run job: %s",
                    resource_id,
                    str(e),
                )
                failed_jobs.append({"resource_id": resource_id, "error": str(e)})

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
