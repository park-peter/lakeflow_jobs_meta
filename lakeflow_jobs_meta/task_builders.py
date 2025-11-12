"""Task builder functions for creating different types of Databricks tasks"""

import json
import logging
from typing import Dict, Any, Optional, List
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    SqlTask,
    SqlTaskQuery,
    SqlTaskFile,
    TaskDependency,
    Source,
    RunIf,
    JobEmailNotifications,
    TaskNotificationSettings,
    PythonWheelTask,
    SparkJarTask,
    PipelineTask,
    DbtTask,
)
from lakeflow_jobs_meta.constants import (
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
    TASK_TYPE_PYTHON_WHEEL,
    TASK_TYPE_SPARK_JAR,
    TASK_TYPE_PIPELINE,
    TASK_TYPE_DBT,
    TASK_TIMEOUT_SECONDS,
)
from lakeflow_jobs_meta.utils import sanitize_task_key, validate_notebook_path

logger = logging.getLogger(__name__)


def create_task_from_config(
    task_data: Dict[str, Any],
    control_table: str,
    depends_on_task_keys: Optional[List[str]] = None,
    default_warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a task configuration from task metadata.

    Args:
        task_data: Task dictionary from control table (can be dict or Row-like object)
        depends_on_task_keys: List of task_key strings this task depends on
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks

    Returns:
        Task configuration dictionary

    Raises:
        ValueError: If task configuration is invalid
    """
    if not isinstance(task_data, dict):
        if hasattr(task_data, "asDict"):
            task_data = task_data.asDict()
        else:
            try:
                task_data = dict(task_data)
            except (TypeError, ValueError):
                pass

    task_key_raw = task_data.get("task_key")
    if not task_key_raw:
        raise ValueError("Task data must have 'task_key' field")
    task_key = sanitize_task_key(task_key_raw)

    task_type = task_data.get("task_type")
    if not task_type:
        raise ValueError(f"Task '{task_key}' must have 'task_type' field")

    # Parse task_config JSON string
    try:
        task_config_str = task_data.get("task_config", "{}")
        task_config_json = json.loads(task_config_str) if isinstance(task_config_str, str) else task_config_str
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Invalid task_config JSON for task_key '{task_key}': {str(e)}")

    if task_type == TASK_TYPE_NOTEBOOK:
        task_config = create_notebook_task_config(task_key, task_config_json)
    elif task_type == TASK_TYPE_SQL_QUERY:
        task_config = create_sql_query_task_config(task_key, task_config_json, default_warehouse_id)
    elif task_type == TASK_TYPE_SQL_FILE:
        task_config = create_sql_file_task_config(task_key, task_config_json, default_warehouse_id)
    elif task_type == TASK_TYPE_PYTHON_WHEEL:
        task_config = create_python_wheel_task_config(task_key, task_config_json)
    elif task_type == TASK_TYPE_SPARK_JAR:
        task_config = create_spark_jar_task_config(task_key, task_config_json)
    elif task_type == TASK_TYPE_PIPELINE:
        task_config = create_pipeline_task_config(task_key, task_config_json)
    elif task_type == TASK_TYPE_DBT:
        task_config = create_dbt_task_config(task_key, task_config_json)
    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for task_key '{task_key}'")

    if depends_on_task_keys:
        task_config["depends_on"] = [{"task_key": task_key} for task_key in depends_on_task_keys]

    # Add task-level timeout_seconds if specified in task_config
    if "timeout_seconds" in task_config_json:
        task_config["timeout_seconds"] = task_config_json["timeout_seconds"]

    # Extract and add new task-level fields (optional, only set if specified)
    if "run_if" in task_config_json:
        task_config["run_if"] = task_config_json["run_if"]
    if "environment_key" in task_config_json:
        task_config["environment_key"] = task_config_json["environment_key"]
    if "job_cluster_key" in task_config_json:
        task_config["job_cluster_key"] = task_config_json["job_cluster_key"]
    if "existing_cluster_id" in task_config_json:
        task_config["existing_cluster_id"] = task_config_json["existing_cluster_id"]
    if "notification_settings" in task_config_json:
        task_config["notification_settings"] = task_config_json["notification_settings"]

    # Set disabled flag from task metadata (defaults to False if not specified)
    task_config["disabled"] = task_data.get("disabled", False)

    return task_config


def create_notebook_task_config(task_key: str, task_config: Dict[str, Any]) -> Dict[str, Any]:
    """Create notebook task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains file_path, task_parameters, etc.)

    Returns:
        Notebook task configuration dictionary

    Note:
        Only user-defined task-level parameters from metadata are passed to the notebook.
        Job-level parameters are set at JobSettings level and automatically pushed down by Databricks.
        If notebooks use widgets (like task_key, control_table), Databricks Jobs UI
        automatically adds them when the notebook is attached to a job.
    """
    file_path = task_config.get("file_path")
    if not file_path:
        raise ValueError(f"Missing file_path in task_config for task_key: {task_key}")

    validate_notebook_path(file_path)

    # Extract task-level parameters from task_config
    task_parameters = task_config.get("parameters", {})

    # Pass only user-defined task-level parameters from metadata
    # Job-level parameters are handled separately at JobSettings level
    base_parameters = dict(task_parameters) if task_parameters else {}

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_NOTEBOOK,
        "notebook_task": {
            "notebook_path": file_path,
            "base_parameters": base_parameters,
        },
    }


def _build_sql_task_parameters(parameters: Dict[str, Any]) -> Dict[str, str]:
    """Build SQL task parameters from parameters dictionary.

    Parameters can contain static values or Databricks dynamic value references.
    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references

    Args:
        parameters: Parameters dictionary

    Returns:
        Dictionary of parameter names to string values
    """
    return {k: str(v) for k, v in parameters.items()} if parameters else {}


def create_sql_query_task_config(
    task_key: str, task_config: Dict[str, Any], default_warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create SQL query task configuration.

    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    If not provided in task_config, will use default_warehouse_id if available.

    SQL queries should use parameter syntax (:parameter_name). Task-level parameters are defined in
    task_config and can use Databricks dynamic value references.
    Job-level parameters are set at JobSettings level and automatically pushed down by Databricks.

    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains warehouse_id, sql_query, query_id, task_parameters, etc.)
        default_warehouse_id: Optional default SQL warehouse ID to use if not specified in config

    Returns:
        SQL query task configuration dictionary

    Raises:
        ValueError: If warehouse_id is missing or neither sql_query nor query_id is provided
    """
    warehouse_id = task_config.get("warehouse_id")
    if warehouse_id and warehouse_id.lower() in ("your-warehouse-id", "your_warehouse_id"):
        warehouse_id = None
    warehouse_id = warehouse_id or default_warehouse_id
    if not warehouse_id:
        raise ValueError(
            f"Missing warehouse_id for task_key: {task_key}. "
            f"Either specify warehouse_id in task_config or provide default_warehouse_id to orchestrator."
        )

    # Extract task-level parameters from task_config
    task_parameters = task_config.get("parameters", {})

    sql_query = task_config.get("sql_query")
    query_id = task_config.get("query_id")

    if not sql_query and not query_id:
        raise ValueError(f"Must provide either sql_query or query_id for task_key: {task_key}")

    # Convert task parameters to strings for SQL API
    task_parameters = _build_sql_task_parameters(task_parameters)

    result = {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_QUERY,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "parameters": task_parameters,
        },
    }

    if query_id:
        result["sql_task"]["query"] = {"query_id": query_id}
    else:
        result["sql_task"]["query"] = {"query": sql_query}

    return result


def create_sql_file_task_config(
    task_key: str, task_config: Dict[str, Any], default_warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create SQL file task configuration.

    SQL file tasks reference SQL files directly. SQL files should use parameter syntax
    (:parameter_name). Task-level parameters are defined in task_config and can use Databricks
    dynamic value references.
    Job-level parameters are set at JobSettings level and automatically pushed down by Databricks.

    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references

    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    If not provided in task_config, will use default_warehouse_id if available.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains warehouse_id, file_path, task_parameters, etc.)
        default_warehouse_id: Optional default SQL warehouse ID to use if not specified in config

    Returns:
        SQL file task configuration dictionary

    Raises:
        ValueError: If warehouse_id or file_path is missing
    """
    warehouse_id = task_config.get("warehouse_id")
    if warehouse_id and warehouse_id.lower() in ("your-warehouse-id", "your_warehouse_id"):
        warehouse_id = None
    warehouse_id = warehouse_id or default_warehouse_id
    if not warehouse_id:
        raise ValueError(
            f"Missing warehouse_id for task_key: {task_key}. "
            f"Either specify warehouse_id in task_config or provide default_warehouse_id to orchestrator."
        )

    file_path = task_config.get("file_path")
    if not file_path:
        raise ValueError(f"Missing file_path for task_key: {task_key}")

    # Extract task-level parameters from task_config
    task_parameters = task_config.get("parameters", {})
    file_source = task_config.get("file_source", "WORKSPACE")

    # Convert task parameters to strings for SQL API
    task_parameters = _build_sql_task_parameters(task_parameters)

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_FILE,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "file": {
                "path": file_path,
                "source": file_source,
            },
            "parameters": task_parameters,
        },
    }


def create_python_wheel_task_config(task_key: str, task_config: Dict[str, Any]) -> Dict[str, Any]:
    """Create Python wheel task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains package_name, entry_point, task_parameters list)

    Returns:
        Python wheel task configuration dictionary

    Raises:
        ValueError: If package_name or entry_point is missing
    """
    package_name = task_config.get("package_name")
    if not package_name:
        raise ValueError(f"Missing package_name for task_key: {task_key}")

    entry_point = task_config.get("entry_point")
    if not entry_point:
        raise ValueError(f"Missing entry_point for task_key: {task_key}")

    # Extract task-level parameters from task_config (typically a list for Python wheel tasks)
    task_parameters = task_config.get("parameters", [])
    if isinstance(task_parameters, dict):
        task_parameters = list(task_parameters.values())
    elif not isinstance(task_parameters, list):
        task_parameters = []

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_PYTHON_WHEEL,
        "python_wheel_task": {
            "package_name": package_name,
            "entry_point": entry_point,
            "parameters": task_parameters,
        },
    }


def create_spark_jar_task_config(task_key: str, task_config: Dict[str, Any]) -> Dict[str, Any]:
    """Create Spark JAR task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains main_class_name, task_parameters list)

    Returns:
        Spark JAR task configuration dictionary

    Raises:
        ValueError: If main_class_name is missing
    """
    main_class_name = task_config.get("main_class_name")
    if not main_class_name:
        raise ValueError(f"Missing main_class_name for task_key: {task_key}")

    # Extract task-level parameters from task_config (typically a list for Spark JAR tasks)
    task_parameters = task_config.get("parameters", [])
    if isinstance(task_parameters, dict):
        task_parameters = list(task_parameters.values())
    elif not isinstance(task_parameters, list):
        task_parameters = []

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_SPARK_JAR,
        "spark_jar_task": {
            "main_class_name": main_class_name,
            "parameters": task_parameters,
        },
    }


def create_pipeline_task_config(task_key: str, task_config: Dict[str, Any]) -> Dict[str, Any]:
    """Create Pipeline task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains pipeline_id)

    Returns:
        Pipeline task configuration dictionary

    Raises:
        ValueError: If pipeline_id is missing
    """
    pipeline_id = task_config.get("pipeline_id")
    if not pipeline_id:
        raise ValueError(f"Missing pipeline_id for task_key: {task_key}")

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_PIPELINE,
        "pipeline_task": {
            "pipeline_id": pipeline_id,
        },
    }


def create_dbt_task_config(
    task_key: str, task_config: Dict[str, Any], default_warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create dbt task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains commands, warehouse_id, etc.)
        default_warehouse_id: Not used for dbt tasks (warehouse_id is optional)

    Returns:
        dbt task configuration dictionary

    Raises:
        ValueError: If commands is missing
    """
    commands = task_config.get("commands")
    if not commands:
        raise ValueError(f"Missing commands for task_key: {task_key}")

    if isinstance(commands, str):
        commands = commands.strip()
        if not commands.startswith(("dbt", "edr")):
            raise ValueError(f"Invalid dbt command, all dbt commands must start with `dbt` or `edr`. Got: {commands}")
    elif isinstance(commands, list):
        for cmd in commands:
            if isinstance(cmd, str):
                cmd = cmd.strip()
                if not cmd.startswith(("dbt", "edr")):
                    raise ValueError(
                        f"Invalid dbt command, all dbt commands must start with `dbt` or `edr`. Got: {cmd}"
                    )

    warehouse_id = None
    task_warehouse_id = task_config.get("warehouse_id")
    if task_warehouse_id:
        if isinstance(task_warehouse_id, str) and task_warehouse_id.lower() not in (
            "your-warehouse-id",
            "your_warehouse_id",
        ):
            warehouse_id = task_warehouse_id

    if isinstance(commands, str):
        commands_list = [commands]
    else:
        commands_list = commands

    dbt_config = {
        "commands": commands_list,
    }

    if warehouse_id:
        dbt_config["warehouse_id"] = warehouse_id

    profiles_dir = task_config.get("profiles_directory")
    logger.debug(
        "DEBUG: profiles_directory for task_key '%s': %s (type: %s)", task_key, profiles_dir, type(profiles_dir)
    )
    if profiles_dir:
        if warehouse_id:
            logger.warning(
                f"profiles_directory specified for task_key '{task_key}' but warehouse_id is also provided. "
                f"warehouse_id takes precedence and profiles_directory will be ignored."
            )
        dbt_config["profiles_directory"] = profiles_dir
    project_dir = task_config.get("project_directory")
    if project_dir:
        dbt_config["project_directory"] = project_dir
    if "catalog" in task_config:
        dbt_config["catalog"] = task_config["catalog"]
    if "schema" in task_config:
        dbt_config["schema"] = task_config["schema"]

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_DBT,
        "dbt_task": dbt_config,
    }


def convert_task_config_to_sdk_task(task_config: Dict[str, Any]) -> Task:
    """Convert task configuration dictionary to Databricks SDK Task object.

    Args:
        task_config: Task configuration dictionary

    Returns:
        Databricks SDK Task object
    """
    task_key = task_config["task_key"]
    task_type = task_config.get("task_type", TASK_TYPE_NOTEBOOK)

    task_dependencies = None
    if "depends_on" in task_config:
        task_dependencies = [TaskDependency(task_key=dep["task_key"]) for dep in task_config["depends_on"]]

    # Get task-level timeout_seconds if specified, otherwise use default
    task_timeout = task_config.get("timeout_seconds", TASK_TIMEOUT_SECONDS)
    task_disabled = task_config.get("disabled", False)

    # Extract new task-level fields
    run_if_str = task_config.get("run_if")
    run_if_enum = None
    if run_if_str:
        try:
            run_if_enum = RunIf(run_if_str)
        except (ValueError, TypeError):
            logger.warning(f"Invalid run_if value '{run_if_str}' for task '{task_key}', ignoring")

    environment_key = task_config.get("environment_key")
    job_cluster_key = task_config.get("job_cluster_key")
    existing_cluster_id = task_config.get("existing_cluster_id")

    # Build notification_settings if provided
    notification_settings_obj = None
    if "notification_settings" in task_config:
        notif_config = task_config["notification_settings"]
        email_notifications_obj = None
        if "email_notifications" in notif_config:
            email_config = notif_config["email_notifications"]
            email_notifications_obj = JobEmailNotifications(
                on_start=email_config.get("on_start", []),
                on_success=email_config.get("on_success", []),
                on_failure=email_config.get("on_failure", []),
                on_duration_warning_threshold_exceeded=email_config.get("on_duration_warning_threshold_exceeded", []),
            )
        notification_settings_obj = TaskNotificationSettings(
            no_alert_for_skipped_runs=notif_config.get("no_alert_for_skipped_runs"),
            no_alert_for_canceled_runs=notif_config.get("no_alert_for_canceled_runs"),
            alert_on_last_attempt=notif_config.get("alert_on_last_attempt"),
        )
        if email_notifications_obj:
            notification_settings_obj.email_notifications = email_notifications_obj

    # Create Task object (disabled is handled in serialization, not in constructor)
    task_obj = None

    if task_type == TASK_TYPE_NOTEBOOK:
        notebook_config = task_config["notebook_task"]
        task_obj = Task(
            task_key=task_key,
            notebook_task=NotebookTask(
                notebook_path=notebook_config["notebook_path"],
                base_parameters=notebook_config.get("base_parameters", {}),
            ),
            depends_on=task_dependencies,
            existing_cluster_id=existing_cluster_id,
            job_cluster_key=job_cluster_key,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_SQL_QUERY:
        sql_config = task_config["sql_task"]
        query_config = sql_config.get("query", {})

        query_id = query_config.get("query_id")
        query_text = query_config.get("query")

        if query_id:
            sql_query = SqlTaskQuery(query_id=query_id)
        elif query_text:
            sql_query = {"query": query_text}
        else:
            raise ValueError(f"SQL query task '{task_key}' must have either query_id or query in query config")

        task_obj = Task(
            task_key=task_key,
            sql_task=SqlTask(
                warehouse_id=sql_config["warehouse_id"], query=sql_query, parameters=sql_config.get("parameters", {})
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_SQL_FILE:
        sql_config = task_config["sql_task"]
        file_config = sql_config.get("file", {})

        if not file_config:
            raise ValueError(f"SQL file task '{task_key}' must have file configuration")

        file_path = file_config.get("path")
        file_source = file_config.get("source", "WORKSPACE")

        if not file_path:
            raise ValueError(f"SQL file task '{task_key}' must have file.path")

        try:
            source_enum = Source(file_source) if isinstance(file_source, str) else file_source
        except (ValueError, TypeError):
            source_enum = Source.WORKSPACE

        sql_file = SqlTaskFile(path=file_path, source=source_enum)

        task_obj = Task(
            task_key=task_key,
            sql_task=SqlTask(
                warehouse_id=sql_config["warehouse_id"],
                file=sql_file,
                parameters=sql_config.get("parameters", {}),
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_PYTHON_WHEEL:
        python_wheel_config = task_config["python_wheel_task"]
        task_obj = Task(
            task_key=task_key,
            python_wheel_task=PythonWheelTask(
                package_name=python_wheel_config["package_name"],
                entry_point=python_wheel_config["entry_point"],
                parameters=python_wheel_config.get("parameters", []),
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_SPARK_JAR:
        spark_jar_config = task_config["spark_jar_task"]
        task_obj = Task(
            task_key=task_key,
            spark_jar_task=SparkJarTask(
                main_class_name=spark_jar_config["main_class_name"],
                parameters=spark_jar_config.get("parameters", []),
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_PIPELINE:
        pipeline_config = task_config["pipeline_task"]
        task_obj = Task(
            task_key=task_key,
            pipeline_task=PipelineTask(
                pipeline_id=pipeline_config["pipeline_id"],
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )

    elif task_type == TASK_TYPE_DBT:
        dbt_config = task_config["dbt_task"]
        dbt_task_kwargs = {
            "commands": dbt_config["commands"],
        }
        warehouse_id_val = dbt_config.get("warehouse_id")
        if warehouse_id_val:
            dbt_task_kwargs["warehouse_id"] = warehouse_id_val
        profiles_dir_val = dbt_config.get("profiles_directory")
        if profiles_dir_val:
            dbt_task_kwargs["profiles_directory"] = profiles_dir_val
        project_dir_val = dbt_config.get("project_directory")
        if project_dir_val:
            dbt_task_kwargs["project_directory"] = project_dir_val
        catalog_val = dbt_config.get("catalog")
        if catalog_val:
            dbt_task_kwargs["catalog"] = catalog_val
        schema_val = dbt_config.get("schema")
        if schema_val:
            dbt_task_kwargs["schema"] = schema_val
        task_obj = Task(
            task_key=task_key,
            dbt_task=DbtTask(**dbt_task_kwargs),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
            run_if=run_if_enum,
            environment_key=environment_key,
            job_cluster_key=job_cluster_key,
            existing_cluster_id=existing_cluster_id,
            notification_settings=notification_settings_obj,
        )
        logger.debug(
            "DEBUG: Created Task object for task_key '%s', dbt_task.project_directory: %s",
            task_key,
            task_obj.dbt_task.project_directory if task_obj.dbt_task else None,
        )

    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for task_key '{task_key}'")

    # Set disabled attribute after creation (SDK Task doesn't accept it in constructor)
    if task_disabled:
        task_obj.disabled = True

    return task_obj


def serialize_task_for_api(task: Task) -> Dict[str, Any]:
    """Serialize Task object to dictionary for API calls.

    Args:
        task: Task object to serialize

    Returns:
        Dictionary representation of the task suitable for jobs.create()/update()
    """
    result: Dict[str, Any] = {
        "task_key": task.task_key,
    }

    if task.depends_on:
        result["depends_on"] = [{"task_key": dep.task_key} for dep in task.depends_on]

    if task.timeout_seconds:
        result["timeout_seconds"] = task.timeout_seconds

    if hasattr(task, "disabled") and task.disabled is not None:
        result["disabled"] = task.disabled

    if task.run_if:
        result["run_if"] = task.run_if.value if hasattr(task.run_if, "value") else str(task.run_if)

    if task.environment_key:
        result["environment_key"] = task.environment_key

    if task.existing_cluster_id:
        result["existing_cluster_id"] = task.existing_cluster_id

    if task.job_cluster_key:
        result["job_cluster_key"] = task.job_cluster_key

    if task.notification_settings:
        notif_dict: Dict[str, Any] = {}
        if task.notification_settings.email_notifications:
            email_notif = task.notification_settings.email_notifications
            notif_dict["email_notifications"] = {
                "on_start": email_notif.on_start or [],
                "on_success": email_notif.on_success or [],
                "on_failure": email_notif.on_failure or [],
                "on_duration_warning_threshold_exceeded": email_notif.on_duration_warning_threshold_exceeded or [],
            }
        if task.notification_settings.no_alert_for_skipped_runs is not None:
            notif_dict["no_alert_for_skipped_runs"] = task.notification_settings.no_alert_for_skipped_runs
        if task.notification_settings.no_alert_for_canceled_runs is not None:
            notif_dict["no_alert_for_canceled_runs"] = task.notification_settings.no_alert_for_canceled_runs
        if task.notification_settings.alert_on_last_attempt is not None:
            notif_dict["alert_on_last_attempt"] = task.notification_settings.alert_on_last_attempt
        if notif_dict:
            result["notification_settings"] = notif_dict

    if task.new_cluster:
        result["new_cluster"] = (
            task.new_cluster.as_dict() if hasattr(task.new_cluster, "as_dict") else task.new_cluster
        )

    if task.notebook_task:
        if hasattr(task.notebook_task, "as_dict"):
            result["notebook_task"] = task.notebook_task.as_dict()
        else:
            result["notebook_task"] = {
                "notebook_path": task.notebook_task.notebook_path,
                "base_parameters": task.notebook_task.base_parameters or {},
            }

    if task.sql_task:
        sql_dict: Dict[str, Any] = {"warehouse_id": task.sql_task.warehouse_id}

        if isinstance(task.sql_task.query, str):
            sql_dict["query"] = {"query": task.sql_task.query}
        elif isinstance(task.sql_task.query, dict):
            sql_dict["query"] = task.sql_task.query
        elif hasattr(task.sql_task.query, "as_dict"):
            query_dict = task.sql_task.query.as_dict()
            if "query_id" in query_dict and query_dict["query_id"]:
                sql_dict["query"] = {"query_id": query_dict["query_id"]}
            else:
                raise ValueError("SqlTaskQuery must have query_id")
        elif task.sql_task.query:
            sql_dict["query"] = {"query_id": task.sql_task.query.query_id}

        if task.sql_task.parameters:
            sql_dict["parameters"] = task.sql_task.parameters

        if task.sql_task.alert:
            if hasattr(task.sql_task.alert, "as_dict"):
                sql_dict["alert"] = task.sql_task.alert.as_dict()
            else:
                sql_dict["alert"] = task.sql_task.alert

        if task.sql_task.dashboard:
            if hasattr(task.sql_task.dashboard, "as_dict"):
                sql_dict["dashboard"] = task.sql_task.dashboard.as_dict()
            else:
                sql_dict["dashboard"] = task.sql_task.dashboard

        if task.sql_task.file:
            if hasattr(task.sql_task.file, "as_dict"):
                sql_dict["file"] = task.sql_task.file.as_dict()
            else:
                sql_dict["file"] = {
                    "path": task.sql_task.file.path,
                    "source": (
                        task.sql_task.file.source.value
                        if hasattr(task.sql_task.file.source, "value")
                        else task.sql_task.file.source
                    ),
                }

        result["sql_task"] = sql_dict

    if task.spark_python_task:
        if hasattr(task.spark_python_task, "as_dict"):
            result["spark_python_task"] = task.spark_python_task.as_dict()
        else:
            result["spark_python_task"] = task.spark_python_task

    if task.spark_submit_task:
        if hasattr(task.spark_submit_task, "as_dict"):
            result["spark_submit_task"] = task.spark_submit_task.as_dict()
        else:
            result["spark_submit_task"] = task.spark_submit_task

    if task.python_wheel_task:
        if hasattr(task.python_wheel_task, "as_dict"):
            result["python_wheel_task"] = task.python_wheel_task.as_dict()
        else:
            result["python_wheel_task"] = {
                "package_name": task.python_wheel_task.package_name,
                "entry_point": task.python_wheel_task.entry_point,
                "parameters": task.python_wheel_task.parameters or [],
            }

    if task.spark_jar_task:
        if hasattr(task.spark_jar_task, "as_dict"):
            result["spark_jar_task"] = task.spark_jar_task.as_dict()
        else:
            result["spark_jar_task"] = {
                "main_class_name": task.spark_jar_task.main_class_name,
                "parameters": task.spark_jar_task.parameters or [],
            }

    if task.pipeline_task:
        if hasattr(task.pipeline_task, "as_dict"):
            result["pipeline_task"] = task.pipeline_task.as_dict()
        else:
            result["pipeline_task"] = {
                "pipeline_id": task.pipeline_task.pipeline_id,
            }

    if task.dbt_task:
        if hasattr(task.dbt_task, "as_dict"):
            result["dbt_task"] = task.dbt_task.as_dict()
        else:
            dbt_dict: Dict[str, Any] = {
                "commands": task.dbt_task.commands,
            }
            if task.dbt_task.warehouse_id:
                dbt_dict["warehouse_id"] = task.dbt_task.warehouse_id
            if task.dbt_task.profiles_directory:
                dbt_dict["profiles_directory"] = task.dbt_task.profiles_directory
            if task.dbt_task.project_directory:
                dbt_dict["project_directory"] = task.dbt_task.project_directory
            if task.dbt_task.catalog:
                dbt_dict["catalog"] = task.dbt_task.catalog
            if task.dbt_task.schema:
                dbt_dict["schema"] = task.dbt_task.schema
            result["dbt_task"] = dbt_dict

    return result
