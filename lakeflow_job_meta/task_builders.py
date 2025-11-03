"""Task builder functions for creating different types of Databricks tasks"""

import json
import logging
from typing import Dict, Any, Optional
from databricks.sdk.service.jobs import (
    Task, NotebookTask, SqlTask, SqlTaskQuery, TaskDependency
)
from lakeflow_job_meta.constants import (
    TASK_TYPE_NOTEBOOK, TASK_TYPE_SQL_QUERY, TASK_TYPE_SQL_FILE,
    TASK_TIMEOUT_SECONDS
)
from lakeflow_job_meta.utils import sanitize_task_key, validate_notebook_path

logger = logging.getLogger(__name__)


def create_task_from_config(
    source: Dict[str, Any],
    control_table: str,
    previous_order_tasks: Optional[list] = None,
    cluster_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create a task configuration from source metadata.
    
    Args:
        source: Source dictionary from control table
        previous_order_tasks: List of task keys from previous execution order
        cluster_id: Optional cluster ID for the task
        
    Returns:
        Task configuration dictionary
        
    Raises:
        ValueError: If task configuration is invalid
    """
    task_key = sanitize_task_key(source['source_id'])
    
    # Parse transformation config
    try:
        trans_config = json.loads(source['transformation_config'])
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Invalid transformation_config JSON for source_id '{source['source_id']}': {str(e)}")
    
    # Determine task type (defaults to notebook if not specified)
    task_type = trans_config.get('task_type', TASK_TYPE_NOTEBOOK)
    
    # Create task config based on type
    if task_type == TASK_TYPE_NOTEBOOK:
        task_config = create_notebook_task_config(source, task_key, trans_config, control_table)
    elif task_type == TASK_TYPE_SQL_QUERY:
        task_config = create_sql_query_task_config(source, task_key, trans_config)
    elif task_type == TASK_TYPE_SQL_FILE:
        task_config = create_sql_file_task_config(source, task_key, trans_config)
    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for source_id '{source['source_id']}'")
    
    # Add dependencies
    if previous_order_tasks:
        task_config["depends_on"] = [{"task_key": task} for task in previous_order_tasks]
    
    return task_config


def create_notebook_task_config(
    source: Dict[str, Any],
    task_key: str,
    trans_config: Dict[str, Any],
    control_table: str
) -> Dict[str, Any]:
    """Create notebook task configuration.
    
    Args:
        source: Source dictionary
        task_key: Sanitized task key
        trans_config: Transformation configuration
        control_table: Name of the control table containing metadata
        
    Returns:
        Notebook task configuration dictionary
    """
    notebook_path = trans_config.get('notebook_path')
    if not notebook_path:
        raise ValueError(f"Missing notebook_path in transformation_config for source_id: {source['source_id']}")
    
    # Optional validation (non-blocking)
    validate_notebook_path(notebook_path)
    
    # Pass source_id and control_table so notebook can read metadata from control table
    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_NOTEBOOK,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "source_id": source['source_id'],
                "control_table": control_table
            }
        }
    }


def create_sql_query_task_config(
    source: Dict[str, Any],
    task_key: str,
    trans_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Create SQL query task configuration.
    
    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    
    Args:
        source: Source dictionary
        task_key: Sanitized task key
        trans_config: Transformation configuration
        
    Returns:
        SQL query task configuration dictionary
        
    Raises:
        ValueError: If warehouse_id is missing or neither sql_query nor query_id is provided
    """
    sql_config = trans_config.get('sql_task', {})
    
    warehouse_id = sql_config.get('warehouse_id')
    if not warehouse_id:
        raise ValueError(f"Missing warehouse_id in sql_task config for source_id: {source['source_id']}")
    
    sql_query = sql_config.get('sql_query')
    query_id = sql_config.get('query_id')
    
    if not sql_query and not query_id:
        raise ValueError(
            f"Must provide either sql_query or query_id in sql_task config for source_id: {source['source_id']}"
        )
    
    task_config = {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_QUERY,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "parameters": sql_config.get('parameters', {})
        }
    }
    
    # Add query (either inline or reference to saved query)
    if query_id:
        task_config["sql_task"]["query"] = {"query_id": query_id}
    else:
        task_config["sql_task"]["query"] = {"query": sql_query}
    
    return task_config


def create_sql_file_task_config(
    source: Dict[str, Any],
    task_key: str,
    trans_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Create SQL file task configuration.
    
    For SQL file tasks, we read the file content and use it as an inline query.
    This is because Databricks SQL tasks expect query text, not file paths.
    
    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    
    Args:
        source: Source dictionary
        task_key: Sanitized task key
        trans_config: Transformation configuration
        
    Returns:
        SQL file task configuration dictionary
        
    Raises:
        ValueError: If warehouse_id or sql_file_path is missing, or if file cannot be read
    """
    sql_config = trans_config.get('sql_task', {})
    
    warehouse_id = sql_config.get('warehouse_id')
    if not warehouse_id:
        raise ValueError(f"Missing warehouse_id in sql_task config for source_id: {source['source_id']}")
    
    sql_file_path = sql_config.get('sql_file_path')
    if not sql_file_path:
        raise ValueError(f"Missing sql_file_path in sql_task config for source_id: {source['source_id']}")
    
    # Read SQL file content
    # Note: Databricks SQL tasks require warehouse_id (per Databricks API).
    # The warehouse_id must be provided in the sql_task configuration.
    sql_query = None
    try:
        # Try using dbutils first (Databricks environment)
        import dbutils
        # Read file from workspace - dbutils.fs.head supports /Workspace/ paths directly
        sql_query = dbutils.fs.head(sql_file_path)
    except (NameError, AttributeError):
        # dbutils not available, try regular file read
        try:
            with open(sql_file_path, 'r') as f:
                sql_query = f.read()
        except FileNotFoundError:
            raise ValueError(f"Could not read SQL file: {sql_file_path}. Ensure file exists in workspace.")
    except Exception as e:
        raise ValueError(f"Error reading SQL file {sql_file_path}: {str(e)}")
    
    if not sql_query:
        raise ValueError(f"SQL file {sql_file_path} appears to be empty")
    
    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_FILE,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "query": {
                "query": sql_query
            },
            "parameters": sql_config.get('parameters', {})
        }
    }


def convert_task_config_to_sdk_task(
    task_config: Dict[str, Any],
    cluster_id: Optional[str] = None
) -> Task:
    """Convert task configuration dictionary to Databricks SDK Task object.
    
    Args:
        task_config: Task configuration dictionary
        cluster_id: Optional cluster ID
        
    Returns:
        Databricks SDK Task object
    """
    task_key = task_config["task_key"]
    task_type = task_config.get("task_type", TASK_TYPE_NOTEBOOK)
    
    # Handle dependencies
    task_dependencies = None
    if "depends_on" in task_config:
        task_dependencies = [
            TaskDependency(task_key=dep["task_key"])
            for dep in task_config["depends_on"]
        ]
    
    # Create task based on type
    if task_type == TASK_TYPE_NOTEBOOK:
        notebook_config = task_config["notebook_task"]
        return Task(
            task_key=task_key,
            notebook_task=NotebookTask(
                notebook_path=notebook_config["notebook_path"],
                base_parameters=notebook_config.get("base_parameters", {})
            ),
            depends_on=task_dependencies,
            existing_cluster_id=cluster_id,
            timeout_seconds=TASK_TIMEOUT_SECONDS
        )
    
    elif task_type in [TASK_TYPE_SQL_QUERY, TASK_TYPE_SQL_FILE]:
        sql_config = task_config["sql_task"]
        query_config = sql_config.get("query", {})
        
        sql_query = SqlTaskQuery(
            query_id=query_config.get("query_id"),
            query=query_config.get("query")
        )
        
        return Task(
            task_key=task_key,
            sql_task=SqlTask(
                warehouse_id=sql_config["warehouse_id"],
                query=sql_query,
                parameters=sql_config.get("parameters", {})
            ),
            depends_on=task_dependencies,
            timeout_seconds=TASK_TIMEOUT_SECONDS
        )
    
    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for task_key '{task_key}'")

