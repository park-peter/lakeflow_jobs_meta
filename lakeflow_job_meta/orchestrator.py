"""Main orchestration functions and classes for generating and managing Databricks jobs"""

import json
import logging
from typing import Optional, List, Dict, Any
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, Task, TaskDependency
from delta.tables import DeltaTable

from lakeflow_job_meta.constants import (
    TASK_TIMEOUT_SECONDS, JOB_TIMEOUT_SECONDS, MAX_CONCURRENT_RUNS
)
from lakeflow_job_meta.task_builders import create_task_from_config, convert_task_config_to_sdk_task
from lakeflow_job_meta.metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


def _get_spark():
    """Get active Spark session (always available in Databricks runtime)."""
    return SparkSession.getActiveSession()


class JobOrchestrator:
    """Orchestrates Databricks Jobs based on metadata in control table.
    
    Encapsulates job creation, updates, and management operations.
    Reduces parameter passing by maintaining state for control_table,
    workspace_client, and jobs_table.
    
    Example:
        ```python
        orchestrator = JobOrchestrator("catalog.schema.control_table")
        orchestrator.ensure_setup()  # Create tables if needed
        job_id = orchestrator.create_or_update_job("my_module")
        
        # Or run all modules
        results = orchestrator.run_all_modules(auto_run=True)
        ```
    """
    
    def __init__(self, control_table: str, workspace_client: Optional[WorkspaceClient] = None):
        """Initialize JobOrchestrator.
        
        Args:
            control_table: Name of the control table (e.g., "catalog.schema.table")
            workspace_client: Optional WorkspaceClient instance (creates new if not provided)
        """
        if not control_table or not isinstance(control_table, str):
            raise ValueError("control_table must be a non-empty string")
        
        self.control_table = control_table
        self.jobs_table = f"{control_table}_jobs"
        self.workspace_client = workspace_client or WorkspaceClient()
        self.metadata_manager = MetadataManager(control_table)
    
    def _create_job_tracking_table(self) -> None:
        """Create Delta table to track job IDs for each module (internal)."""
        spark = _get_spark()
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.jobs_table} (
                    module_name STRING,
                    job_id BIGINT,
                    job_name STRING,
                    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
                )
                TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')
            """)
            logger.info(f"Job tracking table {self.jobs_table} created/verified successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to create job tracking table '{self.jobs_table}': {str(e)}")
    
    def ensure_setup(self) -> None:
        """Ensure control table and jobs tracking table exist."""
        self.metadata_manager.ensure_exists()
        self._create_job_tracking_table()
    
    def _get_stored_job_id(self, module_name: str) -> Optional[int]:
        """Get stored job_id for a module from Delta table (internal)."""
        spark = _get_spark()
        try:
            job_id_df = spark.table(self.jobs_table).filter(
                F.col("module_name") == module_name
            ).select("job_id").limit(1)
            
            if job_id_df.count() > 0:
                return job_id_df.collect()[0]['job_id']
            else:
                return None
        except Exception as e:
            logger.warning(f"Could not retrieve job_id for {module_name}: {str(e)}")
            return None
    
    def _store_job_id(self, module_name: str, job_id: int, job_name: str) -> None:
        """Store/update job_id for a module in Delta table (internal)."""
        spark = _get_spark()
        try:
            source_data = [Row(
                module_name=module_name,
                job_id=job_id,
                job_name=job_name
            )]
            source_df = spark.createDataFrame(source_data)
            
            try:
                delta_table = DeltaTable.forName(spark, self.jobs_table)
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    "target.module_name = source.module_name"
                ).whenMatchedUpdate(
                    set={
                        "job_id": "source.job_id",
                        "job_name": "source.job_name",
                        "updated_timestamp": "current_timestamp()"
                    }
                ).whenNotMatchedInsert(
                    values={
                        "module_name": "source.module_name",
                        "job_id": "source.job_id",
                        "job_name": "source.job_name"
                    }
                ).execute()
            except ImportError:
                # Fallback for environments without DeltaTable
                source_df.createOrReplaceTempView("source_data")
                table_parts = self.jobs_table.split('.')
                if len(table_parts) == 3:
                    catalog, schema, table = table_parts
                    safe_table = f"`{catalog}`.`{schema}`.`{table}`"
                else:
                    safe_table = f"`{self.jobs_table}`"
                
                spark.sql(f"""
                    MERGE INTO {safe_table} AS target
                    USING source_data AS source
                    ON target.module_name = source.module_name
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            job_id = source.job_id, 
                            job_name = source.job_name,
                            updated_timestamp = current_timestamp()
                    WHEN NOT MATCHED THEN 
                        INSERT (module_name, job_id, job_name) 
                        VALUES (source.module_name, source.job_id, source.job_name)
                """)
            
            logger.info(f"Stored job_id {job_id} for module {module_name}")
        except Exception as e:
            logger.error(f"Could not store job_id: {str(e)}")
            raise RuntimeError(f"Failed to store job_id for module '{module_name}': {str(e)}") from e
    
    def generate_tasks_for_module(self, module_name: str) -> List[Dict[str, Any]]:
        """Generate task configurations for a module based on metadata.
        
        Args:
            module_name: Name of the module to generate tasks for
            
        Returns:
            List of task configuration dictionaries
            
        Raises:
            ValueError: If module_name is invalid, no active sources found, or config is invalid
            RuntimeError: If control table doesn't exist or is inaccessible
        """
        if not module_name or not isinstance(module_name, str):
            raise ValueError("module_name must be a non-empty string")
        
        spark = _get_spark()
        try:
            module_sources = spark.table(self.control_table).filter(
                (F.col("module_name") == module_name) & 
                (F.col("is_active") == True)
            ).orderBy("execution_order").collect()
        except Exception as e:
            raise RuntimeError(f"Failed to read control table '{self.control_table}': {str(e)}")
        
        if not module_sources:
            raise ValueError(f"No active sources found for module '{module_name}'")
        
        # Group by execution order
        order_groups: Dict[int, List[Any]] = {}
        for source in module_sources:
            order = source['execution_order']
            if order not in order_groups:
                order_groups[order] = []
            order_groups[order].append(source)
        
        # Create task structure with dependencies
        tasks: List[Dict[str, Any]] = []
        previous_order_tasks: List[str] = []
        
        for order in sorted(order_groups.keys()):
            current_order_tasks: List[str] = []
            
            for source in order_groups[order]:
                try:
                    task_config = create_task_from_config(
                        source=source,
                        control_table=self.control_table,
                        previous_order_tasks=previous_order_tasks
                    )
                    tasks.append(task_config)
                    current_order_tasks.append(task_config["task_key"])
                except Exception as e:
                    logger.error(f"Failed to create task for source_id '{source['source_id']}': {str(e)}")
                    raise
            
            previous_order_tasks = current_order_tasks
        
        return tasks
    
    def create_or_update_job(
        self,
        module_name: str,
        cluster_id: Optional[str] = None
    ) -> int:
        """Create new job or update existing job using stored job_id.
        
        Follows Databricks Jobs API requirements:
        - Uses JobSettings for both create and update operations
        - Tasks are required (will raise ValueError if empty)
        - Job name format: "Metadata_Driven_ETL_{module_name}"
        
        Args:
            module_name: Name of the module
            cluster_id: Optional cluster ID to use for tasks (applies to all tasks)
            
        Returns:
            The job ID (either updated or newly created)
            
        Raises:
            ValueError: If inputs are invalid or no tasks found
            RuntimeError: If job creation/update fails
        """
        if not module_name or not isinstance(module_name, str):
            raise ValueError("module_name must be a non-empty string")
        
        job_name = f"Metadata_Driven_ETL_{module_name}"
        
        # Get stored job_id
        stored_job_id = self._get_stored_job_id(module_name)
        
        # Generate task definitions
        task_definitions = self.generate_tasks_for_module(module_name)
        
        if not task_definitions or len(task_definitions) == 0:
            raise ValueError(f"No active tasks found for module '{module_name}'. Cannot create job without tasks.")
        
        # Convert to SDK Task objects
        sdk_tasks = []
        for task_def in task_definitions:
            sdk_task = convert_task_config_to_sdk_task(task_def, cluster_id)
            sdk_tasks.append(sdk_task)
        
        # Try to update existing job first
        if stored_job_id:
            try:
                self.workspace_client.jobs.update(
                    job_id=stored_job_id,
                    new_settings=JobSettings(
                        name=job_name,
                        tasks=sdk_tasks,
                        max_concurrent_runs=MAX_CONCURRENT_RUNS,
                        timeout_seconds=JOB_TIMEOUT_SECONDS
                    )
                )
                logger.info(f"Job updated successfully for {module_name} (Job ID: {stored_job_id})")
                return stored_job_id
                
            except Exception as e:
                error_str = str(e).lower()
                if "does not exist" in error_str or "not found" in error_str:
                    logger.warning(f"Stored job ID {stored_job_id} no longer exists. Creating new job...")
                    try:
                        spark_del = _get_spark()
                        delta_table = DeltaTable.forName(spark_del, self.jobs_table)
                        delta_table.delete(F.col("module_name") == module_name)
                    except Exception as delete_error:
                        logger.warning(f"Could not delete invalid job_id record: {str(delete_error)}")
                else:
                    logger.error(f"Error updating job {stored_job_id}: {str(e)}")
                    raise RuntimeError(f"Failed to update job {stored_job_id} for module '{module_name}': {str(e)}") from e
        
        # Create new job
        try:
            created_job = self.workspace_client.jobs.create(
                settings=JobSettings(
                    name=job_name,
                    tasks=sdk_tasks,
                    max_concurrent_runs=MAX_CONCURRENT_RUNS,
                    timeout_seconds=JOB_TIMEOUT_SECONDS
                )
            )
            
            logger.info(f"Job created successfully for {module_name} (Job ID: {created_job.job_id})")
            
            self._store_job_id(module_name, created_job.job_id, job_name)
            
            return created_job.job_id
            
        except Exception as e:
            logger.error(f"Error creating job: {str(e)}")
            raise RuntimeError(f"Failed to create job for module '{module_name}': {str(e)}") from e
    
    def run_all_modules(
        self,
        auto_run: bool = True,
        yaml_path: Optional[str] = None,
        sync_yaml: bool = False
    ) -> List[Dict[str, Any]]:
        """Create and optionally run jobs for all modules in control table.
        
        Args:
            auto_run: Whether to automatically run jobs after creation (default: True)
            yaml_path: Optional path to YAML file to load before orchestrating
            sync_yaml: Whether to load YAML file if provided (default: False)
            
        Returns:
            List of dictionaries with module names and job IDs
        """
        logger.info(f"Starting orchestration with control_table: {self.control_table}")
        
        # Ensure setup
        self.ensure_setup()
        
        # Optionally load YAML if provided
        if yaml_path and sync_yaml:
            try:
                sources_loaded = self.metadata_manager.load_yaml(yaml_path)
                logger.info(f"Loaded {sources_loaded} sources from YAML before orchestrating")
            except FileNotFoundError:
                logger.warning(f"YAML file not found: {yaml_path}. Continuing with existing table data.")
            except Exception as e:
                logger.warning(f"Failed to load YAML file: {str(e)}. Continuing with existing table data.")
        
        modules = self.metadata_manager.get_all_modules()
        
        if not modules:
            logger.warning(f"No modules found in control table '{self.control_table}'")
            return []
        
        created_jobs = []
        failed_modules = []
        
        for module_name in modules:
            try:
                job_id = self.create_or_update_job(module_name)
                created_jobs.append({"module": module_name, "job_id": job_id})
                
                if auto_run:
                    try:
                        run_result = self.workspace_client.jobs.run_now(job_id=job_id)
                        logger.info(f"Started job run for {module_name}: {run_result.run_id}")
                    except Exception as run_error:
                        logger.error(f"Failed to start job run for {module_name}: {str(run_error)}")
                
            except Exception as e:
                logger.error(f"Failed to create/run job for module {module_name}: {str(e)}")
                failed_modules.append({"module": module_name, "error": str(e)})
        
        if failed_modules:
            logger.warning(f"Failed to process {len(failed_modules)} module(s): {failed_modules}")
        
        logger.info(f"Orchestration completed. Managed {len(created_jobs)} job(s) successfully")
        return created_jobs

