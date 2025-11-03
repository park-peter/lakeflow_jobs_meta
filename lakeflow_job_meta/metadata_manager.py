"""Metadata management functions and classes for loading and syncing metadata"""

import json
import logging
import os
from typing import Optional, Dict, Any, List
import yaml
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

logger = logging.getLogger(__name__)


def _get_spark():
    """Get active Spark session (always available in Databricks runtime)."""
    return SparkSession.getActiveSession()


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
                    source_id STRING,
                    module_name STRING,
                    source_type STRING,
                    execution_order INT,
                    source_config STRING,
                    target_config STRING,
                    transformation_config STRING,
                    is_active BOOLEAN DEFAULT true,
                    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
                )
                TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')
            """
            )
            logger.info(f"Control table {self.control_table} verified/created")
        except Exception as e:
            raise RuntimeError(f"Failed to create control table '{self.control_table}': {str(e)}")

    def load_yaml(self, yaml_path: str, validate_file_exists: bool = True) -> int:
        """Load YAML metadata file into control table.

        Args:
            yaml_path: Path to YAML file
            validate_file_exists: Whether to check if file exists before loading

        Returns:
            Number of sources loaded

        Raises:
            FileNotFoundError: If YAML file doesn't exist and validate_file_exists=True
            ValueError: If YAML is invalid
        """
        if validate_file_exists and not os.path.exists(yaml_path):
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        # Ensure table exists
        self.ensure_exists()

        try:
            with open(yaml_path, "r") as file:
                config = yaml.safe_load(file)
        except Exception as e:
            raise ValueError(f"Failed to parse YAML file '{yaml_path}': {str(e)}")

        if not config or "modules" not in config:
            raise ValueError("YAML file must contain 'modules' key")

        # Flatten YAML structure into DataFrame
        rows = []
        for module in config["modules"]:
            for source in module["sources"]:
                # Handle transformation_config - ensure task_type is included
                trans_config = source.get("transformation_config", {})

                # If notebook_path exists but no task_type, default to notebook
                if "notebook_path" in trans_config and "task_type" not in trans_config:
                    trans_config["task_type"] = "notebook"

                rows.append(
                    {
                        "source_id": source["source_id"],
                        "module_name": module["module_name"],
                        "source_type": source.get("source_type", "unknown"),
                        "execution_order": source["execution_order"],
                        "source_config": json.dumps(source.get("source_config", {})),
                        "target_config": json.dumps(source.get("target_config", {})),
                        "transformation_config": json.dumps(trans_config),
                        "is_active": True,
                    }
                )

        if not rows:
            logger.warning(f"No sources found in YAML file '{yaml_path}'")
            return 0

        spark = _get_spark()
        schema = StructType(
            [
                StructField("source_id", StringType(), True),
                StructField("module_name", StringType(), True),
                StructField("source_type", StringType(), True),
                StructField("execution_order", IntegerType(), True),
                StructField("source_config", StringType(), True),
                StructField("target_config", StringType(), True),
                StructField("transformation_config", StringType(), True),
                StructField("is_active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(rows, schema)

        # Use merge to update existing records or insert new ones
        df.createOrReplaceTempView("yaml_data")

        spark.sql(
            f"""
            MERGE INTO {self.control_table} AS target
            USING yaml_data AS source
            ON target.source_id = source.source_id AND target.module_name = source.module_name
            WHEN MATCHED THEN
                UPDATE SET
                    source_type = source.source_type,
                    execution_order = source.execution_order,
                    source_config = source.source_config,
                    target_config = source.target_config,
                    transformation_config = source.transformation_config,
                    is_active = source.is_active,
                    updated_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (source_id, module_name, source_type, execution_order, 
                        source_config, target_config, transformation_config, is_active)
                VALUES (source.source_id, source.module_name, source.source_type, 
                        source.execution_order, source.source_config, source.target_config, 
                        source.transformation_config, source.is_active)
        """
        )

        logger.info(f"Successfully loaded {len(rows)} sources from '{yaml_path}' into {self.control_table}")
        return len(rows)

    def sync_from_volume(self, volume_path: str, file_pattern: str = "*.yaml") -> int:  # noqa: ARG002
        """Load YAML files from Unity Catalog volume into control table.

        Args:
            volume_path: Path to Unity Catalog volume (e.g., '/Volumes/catalog/schema/volume')
            file_pattern: File pattern to match (default: "*.yaml")

        Returns:
            Total number of sources loaded

        Raises:
            RuntimeError: If dbutils is not available (requires Databricks environment)
        """
        try:
            # Try to import dbutils (available in Databricks environment)
            try:
                import dbutils
            except NameError:
                raise RuntimeError("dbutils not available. This function requires Databricks environment.")

            # List files in volume
            files = dbutils.fs.ls(volume_path)
            yaml_files = [f.path for f in files if f.name.endswith((".yaml", ".yml"))]

            if not yaml_files:
                logger.warning(f"No YAML files found in {volume_path}")
                return 0

            total_loaded = 0
            for yaml_file in yaml_files:
                try:
                    # Read file from volume
                    file_content = dbutils.fs.head(yaml_file)

                    # Write to temp file for parsing (yaml.safe_load requires file-like object)
                    import tempfile

                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
                            tmp.write(file_content)
                            tmp_path = tmp.name

                        loaded = self.load_yaml(tmp_path, validate_file_exists=False)
                        total_loaded += loaded
                    finally:
                        # Clean up temp file
                        if tmp_path and os.path.exists(tmp_path):
                            try:
                                os.unlink(tmp_path)
                            except Exception as cleanup_error:
                                logger.warning(f"Could not clean up temp file '{tmp_path}': {str(cleanup_error)}")

                except Exception as e:
                    logger.error(f"Failed to load YAML file '{yaml_file}': {str(e)}")
                    continue

            logger.info(f"Loaded {total_loaded} total sources from {len(yaml_files)} YAML files in {volume_path}")
            return total_loaded

        except Exception as e:
            logger.error(f"Error syncing YAML from volume '{volume_path}': {str(e)}")
            raise

    def detect_changes(self, last_check_timestamp: Optional[str] = None) -> Dict[str, Any]:
        """Detect changes in control table since last check.

        Args:
            last_check_timestamp: ISO timestamp of last check (optional)

        Returns:
            Dictionary with change information:
            {
                'new_modules': List[str],
                'updated_modules': List[str],
                'deactivated_modules': List[str],
                'changed_sources': List[Dict]
            }
        """
        spark = _get_spark()
        try:
            table_df = spark.table(self.control_table)

            changes = {"new_modules": [], "updated_modules": [], "deactivated_modules": [], "changed_sources": []}

            if last_check_timestamp:
                # Check for updates since last check
                changed_df = table_df.filter(F.col("updated_timestamp") > F.lit(last_check_timestamp))

                if changed_df.count() > 0:
                    # Get unique modules with changes
                    updated_modules = changed_df.select("module_name").distinct().collect()
                    changes["updated_modules"] = [row["module_name"] for row in updated_modules]

                    # Get new sources (created since last check)
                    new_sources = changed_df.filter(F.col("created_timestamp") == F.col("updated_timestamp")).collect()
                    changes["changed_sources"] = [
                        {"source_id": row["source_id"], "module_name": row["module_name"], "action": "new"}
                        for row in new_sources
                    ]

                    # Check for deactivated modules (sources that became inactive since last check)
                    deactivated_df = changed_df.filter(F.col("is_active") == False)
                    if deactivated_df.count() > 0:
                        deactivated_modules = deactivated_df.select("module_name").distinct().collect()
                        changes["deactivated_modules"] = [row["module_name"] for row in deactivated_modules]
            else:
                # First run: treat all active modules as new
                active_modules = table_df.filter(F.col("is_active") == True).select("module_name").distinct().collect()
                changes["new_modules"] = [row["module_name"] for row in active_modules]

            return changes

        except Exception as e:
            logger.error(f"Error detecting metadata changes: {str(e)}")
            return {"new_modules": [], "updated_modules": [], "deactivated_modules": [], "changed_sources": []}

    def get_all_modules(self) -> List[str]:
        """Get list of all module names in control table.

        Returns:
            List of module names
        """
        spark = _get_spark()
        try:
            modules = spark.table(self.control_table).select("module_name").distinct().collect()
            return [row["module_name"] for row in modules]
        except Exception as e:
            logger.error(f"Failed to get modules from control table '{self.control_table}': {str(e)}")
            raise RuntimeError(f"Failed to get modules from control table '{self.control_table}': {str(e)}") from e

    def get_module_sources(self, module_name: str) -> List[Dict[str, Any]]:
        """Get all sources for a specific module.

        Args:
            module_name: Name of the module

        Returns:
            List of source dictionaries
        """
        spark = _get_spark()
        try:
            sources = (
                spark.table(self.control_table)
                .filter((F.col("module_name") == module_name) & (F.col("is_active") == True))
                .orderBy("execution_order")
                .collect()
            )
            return [dict(row) for row in sources]
        except Exception as e:
            logger.error(f"Failed to get sources for module '{module_name}': {str(e)}")
            raise RuntimeError(f"Failed to get sources for module '{module_name}': {str(e)}") from e
