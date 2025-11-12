# Changelog

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