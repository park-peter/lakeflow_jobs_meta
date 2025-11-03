"""Continuous monitoring and automatic job update functionality"""

import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from databricks.sdk import WorkspaceClient

from lakeflow_job_meta.orchestrator import JobOrchestrator
from lakeflow_job_meta.metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


class MetadataMonitor:
    """Monitor metadata changes and automatically update jobs.
    
    Monitors:
    1. Delta control table for direct updates
    2. Unity Catalog volume for YAML file changes
    
    Automatically updates Databricks jobs when changes are detected.
    """
    
    def __init__(
        self,
        control_table: str,
        check_interval_seconds: int = 60,
        volume_path: Optional[str] = None,
        auto_update_jobs: bool = True
    ):
        """Initialize metadata monitor.
        
        Args:
            control_table: Name of the control table
            check_interval_seconds: How often to check for changes (default: 60)
            volume_path: Optional Unity Catalog volume path to watch for YAML files
            auto_update_jobs: Whether to automatically update jobs on changes
        """
        self.control_table = control_table
        self.check_interval = check_interval_seconds
        self.volume_path = volume_path
        self.auto_update_jobs = auto_update_jobs
        self.last_check_timestamp = None
        self.last_yaml_file_times = {}  # Track YAML file modification times
        self.workspace_client = WorkspaceClient()
        self.metadata_manager = MetadataManager(control_table)
        self.orchestrator = JobOrchestrator(control_table, workspace_client=self.workspace_client)
        
        # Ensure control table exists
        self.metadata_manager.ensure_exists()
    
    def _check_yaml_files_changed(self) -> bool:
        """Check if any YAML files in volume have changed.
        
        Returns:
            True if any YAML files have been modified since last check
        """
        if not self.volume_path:
            return False
        
        try:
            import dbutils
        except NameError:
            logger.warning("dbutils not available, cannot check YAML file changes")
            return False
        
        try:
            # List files in volume
            files = dbutils.fs.ls(self.volume_path)
            yaml_files = {f.name: f.modificationTime for f in files 
                         if f.name.endswith(('.yaml', '.yml'))}
            
            # Check if any files are new or modified
            for filename, mod_time in yaml_files.items():
                if filename not in self.last_yaml_file_times:
                    # New file
                    self.last_yaml_file_times[filename] = mod_time
                    logger.info(f"New YAML file detected: {filename}")
                    return True
                elif self.last_yaml_file_times[filename] != mod_time:
                    # Modified file
                    logger.info(f"YAML file modified: {filename}")
                    self.last_yaml_file_times[filename] = mod_time
                    return True
            
            # Check for deleted files
            deleted_files = set(self.last_yaml_file_times.keys()) - set(yaml_files.keys())
            if deleted_files:
                for filename in deleted_files:
                    del self.last_yaml_file_times[filename]
                    logger.info(f"YAML file deleted: {filename}")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking YAML file changes: {str(e)}")
            return False
    
    def check_and_update(self) -> Dict[str, Any]:
        """Check for metadata changes from both Delta table and YAML files, and update jobs if needed.
        
        Returns:
            Dictionary with change detection results and update status
        """
        result = {
            'timestamp': datetime.now().isoformat(),
            'changes_detected': False,
            'yaml_changes': False,
            'table_changes': False,
            'jobs_updated': [],
            'errors': []
        }
        
        try:
            # Check for YAML file changes in volume
            yaml_changed = False
            if self.volume_path:
                yaml_changed = self._check_yaml_files_changed()
                if yaml_changed:
                    result['yaml_changes'] = True
                    result['changes_detected'] = True
                    logger.info("YAML files changed in volume, syncing...")
                    try:
                        sources_loaded = self.metadata_manager.sync_from_volume(self.volume_path)
                        logger.info(f"Synced {sources_loaded} sources from YAML files")
                    except Exception as e:
                        logger.warning(f"Failed to sync YAML from volume: {str(e)}")
                        result['errors'].append(f"Volume sync error: {str(e)}")
            
            # Detect changes in Delta table
            changes = self.metadata_manager.detect_changes(self.last_check_timestamp)
            
            table_changed = any([
                changes['new_modules'],
                changes['updated_modules'],
                changes['deactivated_modules']
            ])
            
            if table_changed:
                result['table_changes'] = True
                result['changes_detected'] = True
            
            modules_to_update = set()
            
            # Collect modules that need updates
            if changes['new_modules']:
                modules_to_update.update(changes['new_modules'])
            
            if changes['updated_modules']:
                modules_to_update.update(changes['updated_modules'])
            
            if changes['deactivated_modules']:
                modules_to_update.update(changes['deactivated_modules'])
            
            # If YAML changed, update all modules (since YAML might affect any module)
            if yaml_changed:
                # Get all active modules to update
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.getActiveSession()
                    if spark:
                        all_modules = spark.table(self.control_table).select("module_name").distinct().collect()
                        modules_to_update.update([row['module_name'] for row in all_modules])
                    else:
                        logger.warning("Spark session not available, cannot get all modules")
                except Exception as e:
                    logger.warning(f"Could not get all modules for YAML update: {str(e)}")
            
            # Update jobs if changes detected
            if modules_to_update and self.auto_update_jobs:
                logger.info(f"Updating {len(modules_to_update)} module(s) due to metadata changes: {list(modules_to_update)}")
                
                try:
                    # Use orchestrator to update changed modules
                    updated_module_jobs = []
                    for module_name in modules_to_update:
                        try:
                            job_id = self.orchestrator.create_or_update_job(module_name)
                            updated_module_jobs.append({"module": module_name, "job_id": job_id})
                        except Exception as module_error:
                            logger.error(f"Failed to update job for module '{module_name}': {str(module_error)}")
                            result['errors'].append(f"Module '{module_name}' update error: {str(module_error)}")
                    
                    result['jobs_updated'] = updated_module_jobs
                    
                except Exception as e:
                    error_msg = f"Failed to update jobs: {str(e)}"
                    logger.error(error_msg)
                    result['errors'].append(error_msg)
            
            # Update last check timestamp
            self.last_check_timestamp = datetime.now().isoformat()
            result['last_check_timestamp'] = self.last_check_timestamp
            
        except Exception as e:
            error_msg = f"Error in check_and_update: {str(e)}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
        
        return result
    
    def run_continuous(self, max_iterations: Optional[int] = None):
        """Run continuous monitoring loop.
        
        Args:
            max_iterations: Maximum number of iterations (None for infinite)
        """
        logger.info(f"Starting continuous monitoring (interval: {self.check_interval}s)")
        
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                result = self.check_and_update()
                
                if result['changes_detected']:
                    logger.info(f"Changes detected: {len(result['jobs_updated'])} job(s) updated")
                else:
                    logger.debug("No changes detected")
                
                iteration += 1
                
                # Sleep until next check
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(self.check_interval)  # Continue despite errors



