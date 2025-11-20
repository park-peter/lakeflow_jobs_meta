# Changelog

## [0.2.0] - 2025-11-20

### 🚨 BREAKING CHANGES

**YAML Format Change**: Jobs are now defined as dictionary keys instead of list items

**Before (v0.1.x)**:
```yaml
jobs:
  - job_name: "my_job"
    description: "My job"
    tasks: [...]
```

**After (v0.2.0)**:
```yaml
jobs:
  my_job:  # resource_id (YAML dict key)
    name: "My Job Display Name"  # Optional: Databricks job name
    description: "My job"
    tasks: [...]
```

### Added

- **Variable Substitution in YAML Templates** (`${var.name}` syntax)
  - Use `${var.variable_name}` anywhere in YAML for dynamic values
  - Compatible with Databricks Asset Bundles (DABs) syntax
  - Pass variables via `var` parameter: `jm.create_or_update_jobs(var={'env': 'prod'})`
  - Strict validation: raises error if variable is undefined
  - Works everywhere: job names, task configs, SQL queries, file paths
  - Example:
    ```yaml
    jobs:
      etl_${var.env}_customers:
        tasks:
          - task_key: "extract"
            sql_query: "SELECT * FROM ${var.catalog}.${var.schema}.customers"
    ```

- **Resource ID and Job Name Separation**
  - `resource_id`: YAML dict key (stable identifier for tracking)
  - `job_name`: Optional `name` field or defaults to `resource_id`
  - Allows job name changes without losing job history
  - Example:
    ```yaml
    jobs:
      customer_etl:  # resource_id
        name: "Customer ETL Pipeline - Production"  # job_name in Databricks
    ```

### Changed

- **YAML Structure**: Jobs are now dict entries instead of list items (BREAKING)
- **Control Table Schema**: Added `resource_id` column as first column
- **Jobs Tracking Table Schema**: Added `resource_id` column
- **API Functions**: All load functions now return `resource_ids` instead of `job_names`
- **Orchestrator**: Uses `resource_id` for job tracking, `job_name` for Databricks API

### Technical Details

- Added `substitute_variables()` function in `utils.py` with regex pattern `\$\{var\.([a-zA-Z_][a-zA-Z0-9_]*)\}`
- Updated `MetadataManager.load_yaml()` to accept `variables` parameter
- Updated all public API functions to accept `var` parameter
- Updated YAML parser to iterate over dict: `for resource_id, job in config["jobs"].items()`
- Updated MERGE statements to use `resource_id` as key
- Updated `create_or_update_job()` signature to accept `resource_id` instead of `job_name`
- Updated `get_all_jobs()` to return list of `resource_ids`

### Migration Guide

**For existing YAML files**:
1. Convert from list format to dict format
2. Change `- job_name: "xxx"` to `xxx:` (dict key)
3. Optionally add `name` field if you want different display name

**For programmatic usage**:
- Functions now return `resource_ids` instead of `job_names`
- When calling `create_or_update_job()`, pass `resource_id` not `job_name`

**No database migration needed**: New columns will be auto-added on first run

## [0.1.1] - 2025-11-14

### Added
- **Multi-cloud support for job clusters**: Added support for `azure_attributes` and `gcp_attributes` in `job_clusters.new_cluster` configuration
  - Azure: Supports `availability`, `first_on_demand`, `log_analytics_info`, `spot_bid_max_price`
  - GCP: Supports `availability`, `boot_disk_size`, `first_on_demand`, `google_service_account`, `local_ssd_count`, `use_preemptible_executors`, `zone_id`
  - Automatic enum conversion for `availability` fields using `AzureAvailability` and `GcpAvailability`
  - Maintains backward compatibility with existing `aws_attributes` configurations
- **Enhanced AWS attributes support**: Added support for `ebs_volume_type` with automatic `EbsVolumeType` enum conversion
  - Now supports all AWS attributes: `availability`, `ebs_volume_count`, `ebs_volume_iops`, `ebs_volume_size`, `ebs_volume_throughput`, `ebs_volume_type`, `first_on_demand`, `instance_profile_arn`, `spot_bid_price_percent`, `zone_id`

### Improved
- **Removed unnecessary monkey-patching**: Simplified cluster configuration serialization by relying on SDK's native `as_dict()` method
  - Removed custom `patched_cluster_spec_as_dict` and `patched_job_cluster_as_dict` functions
  - Removed try-finally blocks that were restoring patched methods
  - Cleaner, more maintainable code with better performance
  - SDK's native serialization properly handles all nested objects and enums
- **Better error handling for invalid jobs**: Jobs with validation errors no longer block other valid jobs from being processed
  - Invalid jobs are logged as warnings and skipped
  - Valid jobs continue to be loaded into the control table
  - Failed jobs are reported in the warning message with specific error details
  - Example: If job 'A' has a dependency error, jobs 'B' and 'C' will still be processed successfully

### Technical Details
- Imported `AzureAttributes`, `AzureAvailability`, `GcpAttributes`, `GcpAvailability`, and `EbsVolumeType` from `databricks.sdk.service.compute`
- Updated cluster configuration logic in both job creation and update paths
- Graceful fallback if cloud-specific attributes fail to parse
- Verified SDK's native `as_dict()` handles all serialization correctly, eliminating need for custom monkey-patching
- Per-job exception handling in `load_yaml()` to isolate failures and continue processing valid jobs

## [0.1.0] - Initial Release

### Overview

Lakeflow Jobs Meta is a metadata-driven framework for orchestrating Databricks Lakeflow Jobs. This initial release provides comprehensive job management capabilities with support for multiple task types, flexible metadata ingestion, and production-ready features.

### Core Features

#### Task Types
- **Notebook tasks**: Execute Databricks notebooks with parameter support
- **SQL query tasks**: Execute inline SQL queries or saved queries
- **SQL file tasks**: Execute SQL from workspace files
- **Python Wheel tasks**: Run Python wheel packages
- **Spark JAR tasks**: Execute Spark JARs with main class
- **Pipeline tasks**: Trigger Lakeflow Declarative Pipelines
- **dbt tasks**: Run dbt transformations

#### Metadata Management
- **Delta table as source of truth**: Store metadata in Delta tables for durability and ACID guarantees
- **YAML ingestion**: Load metadata from YAML files, folders, or Unity Catalog volumes
- **Flexible path types**: Support for single files, folders (recursive), and UC volumes
- **Change detection**: Automatic detection of metadata changes
- **Task deletion**: Automatically removes tasks deleted from metadata

#### Job Management
- **Dynamic job generation**: Automatically create/update Databricks jobs from metadata
- **Update-in-place**: Updates existing jobs while preserving job IDs and history
- **Job lifecycle tracking**: Track job IDs in dedicated jobs table
- **Dependency management**: Topological sorting and dependency resolution using `depends_on`
- **Task disabling**: Temporarily disable tasks without removing them

#### Advanced Features
- **Job-level settings**: timeout, max_concurrent_runs, parameters, tags, queue, environments, job_clusters
- **Task-level settings**: timeout, run_if conditions, notifications, cluster selection
- **Pause status management**: Control job pause state with `default_pause_status` and explicit overrides
- **File arrival triggers**: Automatic processing with Databricks file arrival triggers
- **Audit fields**: Automatic tracking of created_by, updated_by, and timestamps

#### Production Ready
- **Comprehensive testing**: Full test suite with mocking for external dependencies
- **Error handling**: Graceful error handling and informative error messages
- **Logging**: Structured logging throughout the framework
- **Type safety**: Type hints throughout codebase
- **Documentation**: Comprehensive README, guides, and examples

### Package Structure

```
lakeflow-jobs-meta/
├── lakeflow_jobs_meta/         # Main package
│   ├── __init__.py            # Public API
│   ├── main.py                # Entry point for monitoring
│   ├── constants.py           # Constants and validation
│   ├── utils.py               # Utility functions
│   ├── task_builders.py       # Task configuration builders
│   ├── orchestrator.py        # Job orchestration
│   ├── metadata_manager.py    # Metadata management
│   └── monitor.py             # Continuous monitoring
├── examples/                   # Example files
│   ├── orchestrator_example.ipynb
│   ├── metadata_examples.yaml
│   ├── notebook_task/
│   └── sql_file_task/
├── docs/                       # Documentation
│   ├── METADATA_MANAGEMENT.md
│   └── PACKAGING_AND_DEPLOYMENT.md
├── tests/                      # Test suite
├── setup.py                    # Package setup
└── pyproject.toml             # Modern Python packaging
```

### Installation

```bash
pip install lakeflow-jobs-meta
```

### Quick Start

```python
import lakeflow_jobs_meta as jm

# Load and process jobs from YAML
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata.yaml",
    control_table="catalog.schema.etl_control"
)
```

### Requirements

- Python 3.8+
- Databricks SDK 0.65.0+
- PySpark (available in Databricks Runtime)

### Documentation

- **README.md**: Quick start guide and comprehensive documentation
- **docs/METADATA_MANAGEMENT.md**: Detailed guide on metadata workflows
- **docs/PACKAGING_AND_DEPLOYMENT.md**: Deployment and packaging instructions
- **examples/**: Working examples for all supported task types