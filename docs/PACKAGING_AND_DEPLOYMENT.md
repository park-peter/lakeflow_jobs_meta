# Packaging and Deployment Guide

## Overview

Lakeflow Jobs Meta is packaged as a Python library that can be installed and run as a single task in a Databricks Lakeflow Jobs.

## Package Structure

```
lakeflow-jobs-meta/
├── lakeflow_jobs_meta/          # Main package
│   ├── __init__.py
│   ├── main.py                 # Entry point for monitoring task
│   ├── constants.py
│   ├── utils.py
│   ├── task_builders.py
│   ├── orchestrator.py
│   ├── metadata_manager.py
│   └── monitor.py
├── setup.py                    # Package setup
├── pyproject.toml             # Modern Python packaging
└── README.md
```

## Installation

### Option 1: Install from Source (Development)

```bash
# Clone repository
git clone https://github.com/yourusername/lakeflow-jobs-meta.git
cd lakeflow-jobs-meta

# Install in development mode
pip install -e .
```

### Option 2: Install from Wheel (Production)

```bash
# Build wheel
python setup.py bdist_wheel

# Install wheel
pip install dist/lakeflow_jobs_meta-0.1.0-py3-none-any.whl
```

### Option 3: Install in Databricks Workspace

```python
# In Databricks notebook or job
%pip install /Workspace/Repos/your-repo/lakeflow-jobs-meta

# Or from wheel uploaded to workspace
%pip install /dbfs/FileStore/lakeflow_jobs_meta-0.1.0-py3-none-any.whl

# Or install from PyPI (once published)
%pip install lakeflow-jobs-meta
```

## Usage as a Lakeflow Jobs Task

### Step 1: Create Orchestration Job

Create a Databricks Job with a single notebook task:

**Task Configuration:**
- **Task Type:** Notebook
- **Notebook:** Use `examples/orchestrator_example.ipynb` or create your own
- **Cluster:** Any cluster (or use job clusters)

### Step 2: Configure Parameters

Configure parameters via Databricks notebook widgets or base_parameters:

```python
# Widget names:
# - control_table: Delta table containing metadata (required)
# - yaml_path: Path to YAML file, folder, or UC volume (optional)
# - default_warehouse_id: Default SQL warehouse ID (optional)
# - default_queries_path: Path for inline SQL queries (optional)
```

### Step 3: Run the Job

**For On-Demand Execution:**
- Run the job manually when you want to create/update jobs
- The job loads metadata from `yaml_path` (if provided) and creates/updates jobs

**For Continuous Monitoring:**
- Uncomment the monitoring cell in `orchestrator_example.ipynb`
- The job will monitor for changes and auto-update jobs continuously

**For File Arrival Triggers (Recommended for Production):**
- Configure a file arrival trigger on your job to monitor a Unity Catalog volume
- When YAML files are uploaded, the job automatically triggers
- Set `yaml_path` to the volume path to process all YAML files

## Monitoring Behavior

### What It Monitors

1. **Delta Control Table:**
   - Detects new tasks (`created_timestamp == updated_timestamp`)
   - Detects updated tasks (`updated_timestamp > last_check`)
   - Detects disabled tasks (`disabled` flag changes)
   - Detects deleted tasks (removed from metadata)

2. **Unity Catalog Volume (if configured):**
   - Detects new YAML files
   - Detects modified YAML files (by modification time)
   - Automatically syncs changed YAML files to control table
   - Loads all YAML files from volume when `yaml_path` points to volume

### Change Detection Logic

```python
# Every configured check interval (for continuous monitoring):
1. Check YAML files in volume (if configured)
   - Compare modification times
   - If changed: sync to control table
   
2. Check Delta table for changes
   - Compare updated_timestamp with last_check
   - Identify changed jobs
   
3. If changes detected:
   - Update affected Databricks jobs
   - Log results
   
4. Update last_check_timestamp
5. Sleep until next check
```

### On-Demand vs Continuous Monitoring

**On-Demand (Default):**
- Use `create_or_update_jobs(yaml_path="/path/to/metadata.yaml")`
- Loads metadata once and creates/updates jobs
- Job completes after processing

**Continuous Monitoring (Optional):**
- Use `MetadataMonitor` class (see `orchestrator_example.ipynb`)
- Monitors for changes every `check_interval_seconds`
- Runs indefinitely until `max_iterations` reached

**File Arrival Triggers (Recommended for Production):**
- Configure Databricks file arrival trigger
- Job triggers automatically when files are uploaded to volume
- No continuous monitoring needed - only runs when files change

## Example Job Configuration

### On-Demand Execution (Recommended)

```python
# Job task configuration
{
    "task_key": "orchestrate_jobs",
    "notebook_task": {
        "notebook_path": "/Workspace/Repos/repo/lakeflow-jobs-meta/examples/orchestrator_example",
        "base_parameters": {
            "control_table": "your_catalog.schema.etl_control",
            "yaml_path": "/Workspace/path/to/metadata.yaml",
            "default_warehouse_id": "your-warehouse-id"
        }
    },
    "existing_cluster_id": "your-cluster-id",
    "timeout_seconds": 3600  # 1 hour
}
```

### With File Arrival Trigger (Production)

```python
# Job configuration with file arrival trigger
{
    "name": "Metadata Sync Job",
    "trigger": {
        "file_arrival": {
            "url": "/Volumes/catalog/schema/metadata_volume/"
        }
    },
    "tasks": [
        {
            "task_key": "sync_and_orchestrate",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/repo/lakeflow-jobs-meta/examples/orchestrator_example",
                "base_parameters": {
                    "control_table": "your_catalog.schema.etl_control",
                    "yaml_path": "/Volumes/catalog/schema/metadata_volume",
                    "default_warehouse_id": "your-warehouse-id"
                }
            },
            "existing_cluster_id": "your-cluster-id",
            "timeout_seconds": 3600
        }
    ]
}
```

**Note:** When YAML files are uploaded to the volume, the job automatically triggers, loads all YAML files, and updates jobs.

### Continuous Monitoring (Optional)

```python
# Job task configuration for continuous monitoring
{
    "task_key": "monitor_metadata",
    "notebook_task": {
        "notebook_path": "/Workspace/Repos/repo/lakeflow-jobs-meta/examples/orchestrator_example",
        "base_parameters": {
            "control_table": "your_catalog.schema.etl_control",
            "yaml_path": "/Volumes/catalog/schema/metadata_volume",
            "check_interval_seconds": "60"
        }
    },
    "existing_cluster_id": "your-cluster-id",
    "timeout_seconds": 0  # No timeout - runs indefinitely
}
```

**Note:** Uncomment the monitoring cell in `orchestrator_example.ipynb` for continuous monitoring.

## Configuration Parameters Reference

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `control_table` | ✅ Yes | - | Delta table containing metadata |
| `yaml_path` | ❌ No | None | Path to YAML file, folder, or UC volume |
| `default_warehouse_id` | ❌ No | None | Default SQL warehouse ID for SQL tasks |
| `default_queries_path` | ❌ No | None | Directory path for inline SQL queries |
| `default_pause_status` | ❌ No | False | Default pause state for jobs with triggers/schedules |
| `jobs_table` | ❌ No | `{control_table}_jobs` | Custom jobs tracking table name |
| `check_interval_seconds` | ❌ No | 60 | How often to check for changes (continuous monitoring only) |
| `max_iterations` | ❌ No | None | Max iterations before stopping (continuous monitoring only) |

**Path Types for `yaml_path`:**
- **Single file**: `/Workspace/path/to/metadata.yaml` - Loads one YAML file
- **Folder**: `/Workspace/path/to/metadata/` - Loads all YAML files recursively
- **Volume**: `/Volumes/catalog/schema/volume` - Loads all YAML files from UC volume

**Note:** When `yaml_path` is provided, only jobs from that path are processed. When not provided, all jobs in the control table are processed.

## Logging

The monitor logs all activities:

```
INFO - Starting continuous monitoring (interval: 60s)
INFO - Control Table: fe_ppark_demo.job_demo.etl_control
INFO - Volume Path: /Volumes/catalog/schema/metadata
INFO - YAML files changed in volume, syncing...
INFO - Synced 5 sources from YAML files
INFO - Changes detected: 2 job(s) updated
INFO - Updating 2 job(s) due to metadata changes: ['job1', 'job2']
INFO - Job updated successfully for job1 (Job ID: 12345)
```

## Error Handling

- **Volume Sync Errors:** Logged as warnings, monitoring continues
- **Job Update Errors:** Logged as errors, other jobs still processed
- **Fatal Errors:** Logged and monitoring stops (can be restarted)

## Best Practices

1. **Use Job Clusters:** Create job clusters to avoid cluster costs when idle
2. **Set Appropriate Timeout:** Use `timeout_seconds: 0` for infinite runs
3. **Monitor Logs:** Set up alerts on ERROR level logs
4. **Version Control:** Store YAML files in Git, sync to volume via CI/CD
5. **Permissions:** Ensure job has permissions to:
   - Read/write control table
   - Read Unity Catalog volume (if used)
   - Create/update Databricks jobs

## Troubleshooting

### Package Not Found
```python
# Make sure package is installed
%pip install -e /Workspace/path/to/lakeflow-jobs-meta

# Or add to Python path
import sys
sys.path.insert(0, '/Workspace/path/to/lakeflow-jobs-meta')

# Or install from PyPI
%pip install lakeflow-jobs-meta
```

### dbutils Not Available
- Ensure running in Databricks environment
- Volume monitoring requires dbutils

### No Changes Detected
- Check `updated_timestamp` in control table
- Verify YAML files are in correct volume path
- Check logs for errors


---

## Quick Start

1. **Install package:**
   ```bash
   pip install -e .
   ```

2. **Use the orchestrator example:**
   - Use `examples/orchestrator_example.ipynb` 
   - Configure parameters via widgets or base_parameters
   - For continuous monitoring, uncomment the monitoring cell in the notebook

3. **Update metadata:**
   - Update Delta table directly, OR
   - Add/update YAML files in Unity Catalog volume

4. **Monitor automatically updates jobs!** ✨

