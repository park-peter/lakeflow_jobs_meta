"""
Lakeflow Jobs Meta - Metadata-driven framework for Databricks Lakeflow Jobs

A library for orchestrating Databricks Jobs from metadata stored in Delta tables
or YAML files.
"""

__version__ = "0.1.0"

from typing import Optional, List, Dict, Any
from lakeflow_jobs_meta.orchestrator import JobOrchestrator
from lakeflow_jobs_meta.metadata_manager import MetadataManager
from lakeflow_jobs_meta.monitor import MetadataMonitor

__all__ = [
    "JobOrchestrator",
    "MetadataManager",
    "MetadataMonitor",
    "create_or_update_job",
    "create_or_update_jobs",
    "load_yaml",
    "load_from_folder",
    "sync_from_volume",
]


def create_or_update_job(
    job_name: str,
    control_table: Optional[str] = None,
    cluster_id: Optional[str] = None,
    default_warehouse_id: Optional[str] = None,
    jobs_table: Optional[str] = None,
    workspace_client: Optional[Any] = None,
    default_queries_path: Optional[str] = None,
) -> int:
    """Convenience function to create or update a single job.

    Args:
        job_name: Name of the job to create or update
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        cluster_id: Optional cluster ID to use for tasks
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks
        jobs_table: Optional custom name for the jobs tracking table
        workspace_client: Optional WorkspaceClient instance
        default_queries_path: Optional directory path where inline SQL queries
            will be saved (e.g., "/Workspace/Shared/LakeflowQueriesMeta")

    Returns:
        The job ID (either updated or newly created)

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        job_id = jm.create_or_update_job(
            "my_pipeline",
            control_table="catalog.schema.etl_control",
            default_queries_path="/Workspace/Shared/Queries"
        )
        ```
    """
    orchestrator = JobOrchestrator(
        control_table=control_table,
        jobs_table=jobs_table,
        workspace_client=workspace_client,
        default_warehouse_id=default_warehouse_id,
        default_queries_path=default_queries_path,
    )
    return orchestrator.create_or_update_job(job_name, cluster_id=cluster_id)


def create_or_update_jobs(
    control_table: Optional[str] = None,
    default_pause_status: bool = False,
    yaml_path: Optional[str] = None,
    default_warehouse_id: Optional[str] = None,
    jobs_table: Optional[str] = None,
    workspace_client: Optional[Any] = None,
    default_queries_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Convenience function to create or update jobs.

    Args:
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        default_pause_status: Default pause state for jobs with triggers/schedules. 
            When False (default): Jobs are created active and run immediately.
            When True: Jobs with continuous/schedule/trigger are created paused.
            Can be overridden by explicit pause_status in YAML metadata.
        yaml_path: Optional path to load metadata from before orchestrating.
            Can be:
            - Path to a YAML file (e.g., "/Workspace/path/to/metadata.yaml")
            - Path to a folder (e.g., "/Workspace/path/to/metadata/") - loads all YAML files
            - Path to a Unity Catalog volume (e.g., "/Volumes/catalog/schema/volume")
            If provided: Only jobs from the yaml_path are processed.
            If not provided: All jobs in control table are processed.
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks
        jobs_table: Optional custom name for the jobs tracking table
        workspace_client: Optional WorkspaceClient instance
        default_queries_path: Optional directory path where inline SQL queries
            will be saved (e.g., "/Workspace/Shared/LakeflowQueriesMeta")

    Returns:
        List of dictionaries with job names and job IDs

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        # Load from YAML file and create only those jobs
        jobs = jm.create_or_update_jobs(
            yaml_path="/Workspace/metadata/jobs.yaml",
            default_pause_status=True
        )

        # Load from folder (all YAML files) and create only those jobs
        jobs = jm.create_or_update_jobs(
            yaml_path="/Workspace/metadata/",
            default_warehouse_id="abc123"
        )

        # Or load from Unity Catalog volume
        jobs = jm.create_or_update_jobs(
            yaml_path="/Volumes/catalog/schema/metadata_volume",
            default_warehouse_id="abc123"
        )

        # Or process all jobs in control table (no loading)
        jobs = jm.create_or_update_jobs()
        ```
    """
    orchestrator = JobOrchestrator(
        control_table=control_table,
        jobs_table=jobs_table,
        workspace_client=workspace_client,
        default_warehouse_id=default_warehouse_id,
        default_queries_path=default_queries_path,
    )
    return orchestrator.create_or_update_jobs(
        default_pause_status=default_pause_status, yaml_path=yaml_path
    )


def load_yaml(
    yaml_path: str,
    control_table: Optional[str] = None,
    validate_file_exists: bool = True,
) -> tuple:
    """Convenience function to load YAML metadata file into control table.

    Args:
        yaml_path: Path to YAML file
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        validate_file_exists: Whether to check if file exists before loading

    Returns:
        Tuple of (num_tasks_loaded, job_names_loaded)
        - num_tasks_loaded: Number of tasks loaded
        - job_names_loaded: List of job names that were loaded

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        num_tasks, job_names = jm.load_yaml(
            "./examples/metadata_examples.yaml",
            control_table="catalog.schema.etl_control"
        )
        print(f"Loaded {num_tasks} tasks for jobs: {', '.join(job_names)}")
        ```
    """
    manager = MetadataManager(control_table or "main.default.job_metadata_control_table")
    return manager.load_yaml(yaml_path, validate_file_exists=validate_file_exists)


def load_from_folder(
    folder_path: str,
    control_table: Optional[str] = None,
) -> tuple:
    """Convenience function to load all YAML files from a workspace folder.

    Args:
        folder_path: Path to workspace folder
            (e.g., '/Workspace/Users/user@example.com/metadata/')
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")

    Returns:
        Tuple of (total_tasks_loaded, job_names_loaded)
        - total_tasks_loaded: Total number of tasks loaded across all YAML files
        - job_names_loaded: List of unique job names that were loaded

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        num_tasks, job_names = jm.load_from_folder(
            "/Workspace/Users/user@example.com/metadata/",
            control_table="catalog.schema.etl_control"
        )
        print(f"Loaded {num_tasks} tasks for jobs: {', '.join(job_names)}")
        ```
    """
    manager = MetadataManager(control_table or "main.default.job_metadata_control_table")
    return manager.load_from_folder(folder_path)


def sync_from_volume(
    volume_path: str,
    control_table: Optional[str] = None,
) -> tuple:
    """Convenience function to sync all YAML files from Unity Catalog volume.

    Args:
        volume_path: Path to Unity Catalog volume
            (e.g., '/Volumes/catalog/schema/volume')
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")

    Returns:
        Tuple of (total_tasks_loaded, job_names_loaded)
        - total_tasks_loaded: Total number of tasks loaded across all YAML files
        - job_names_loaded: List of unique job names that were loaded

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        num_tasks, job_names = jm.sync_from_volume(
            "/Volumes/catalog/schema/metadata_volume",
            control_table="catalog.schema.etl_control"
        )
        print(f"Loaded {num_tasks} tasks for jobs: {', '.join(job_names)}")
        ```
    """
    manager = MetadataManager(control_table or "main.default.job_metadata_control_table")
    return manager.sync_from_volume(volume_path)
