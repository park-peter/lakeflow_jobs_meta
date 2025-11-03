# Packaging and Deployment Guide

## Overview

Lakeflow Job Meta is packaged as a Python library that can be installed and run as a single task in a Databricks Lakeflow Job.

## Package Structure

```
lakeflow-job-meta/
├── lakeflow_job_meta/          # Main package
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
git clone https://github.com/yourusername/lakeflow-job-meta.git
cd lakeflow-job-meta

# Install in development mode
pip install -e .
```

### Option 2: Install from Wheel (Production)

```bash
# Build wheel
python setup.py bdist_wheel

# Install wheel
pip install dist/lakeflow_job_meta-0.1.0-py3-none-any.whl
```

### Option 3: Install in Databricks Workspace

```python
# In Databricks notebook or job
%pip install /Workspace/Repos/your-repo/lakeflow-job-meta

# Or from wheel uploaded to workspace
%pip install /dbfs/FileStore/lakeflow_job_meta-0.1.0-py3-none-any.whl
```

## Usage as a Lakeflow Job Task

### Step 1: Create Monitoring Job

Create a Databricks Job with a single Python task:

**Task Configuration:**
- **Task Type:** Python script or Notebook
- **Script/Notebook:** Use `examples/orchestrator_example.ipynb` or create your own
- **Cluster:** Any cluster (or use job clusters)

### Step 2: Configure Parameters

Configure parameters directly in the notebook or via Databricks notebook widgets:

```python
# Parameters are set via Databricks widgets or base_parameters
# Widget names: control_table, volume_path, check_interval_seconds, max_iterations
```

### Step 3: Run the Job

The job will:
1. Install/import the package
2. Start monitoring loop
3. Check for changes every configured interval
4. Automatically update Databricks jobs when changes detected

## Monitoring Behavior

### What It Monitors

1. **Delta Control Table:**
   - Detects new sources (`created_timestamp == updated_timestamp`)
   - Detects updated sources (`updated_timestamp > last_check`)
   - Detects deactivated sources (`is_active` changes)

2. **Unity Catalog Volume (if configured):**
   - Detects new YAML files
   - Detects modified YAML files (by modification time)
   - Detects deleted YAML files
   - Automatically syncs changed YAML files to control table

### Change Detection Logic

```python
# Every configured check interval:
1. Check YAML files in volume (if configured)
   - Compare modification times
   - If changed: sync to control table
   
2. Check Delta table for changes
   - Compare updated_timestamp with last_check
   - Identify changed modules
   
3. If changes detected:
   - Update affected Databricks jobs
   - Log results
   
4. Update last_check_timestamp
5. Sleep until next check
```

## Example Job Configuration

### Using Notebook Task (Recommended)

```python
# Job task configuration
{
    "task_key": "monitor_metadata",
    "notebook_task": {
        "notebook_path": "/Workspace/Repos/repo/lakeflow-job-meta/examples/orchestrator_example",
        "base_parameters": {
            "control_table": "your_catalog.schema.etl_control",
            "volume_path": "/Volumes/catalog/schema/metadata",
            "check_interval_seconds": "60"
        }
    },
    "existing_cluster_id": "your-cluster-id",
    "timeout_seconds": 0  # No timeout - runs indefinitely
}
```

**Note:** Configure parameters via widgets or base_parameters (see `examples/orchestrator_example.ipynb`). For continuous monitoring, uncomment the monitoring cell in the notebook.

### Using Python Script Task

```python
# Create a simple Python script that calls main() with command-line arguments
# File: run_monitor.py
# Usage: python run_monitor.py --control-table your_table --volume-path /Volumes/...

# Job task configuration
{
    "task_key": "monitor_metadata",
    "spark_python_task": {
        "python_file": "/Workspace/path/to/run_monitor.py",
        "parameters": [
            "--control-table", "your_catalog.schema.etl_control",
            "--volume-path", "/Volumes/catalog/schema/metadata",
            "--check-interval-seconds", "60"
        ]
    },
    "existing_cluster_id": "your-cluster-id",
    "timeout_seconds": 0
}
```

## Configuration Parameters Reference

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `control_table` | ✅ Yes | - | Delta table containing metadata |
| `volume_path` | ❌ No | None | Unity Catalog volume path for YAML files |
| `check_interval_seconds` | ❌ No | 60 | How often to check for changes (seconds) |
| `max_iterations` | ❌ No | None | Max iterations before stopping (None = infinite) |

**Note:** All parameters must be specified explicitly when calling functions. No environment variables are used.

## Logging

The monitor logs all activities:

```
INFO - Starting continuous monitoring (interval: 60s)
INFO - Control Table: fe_ppark_demo.job_demo.etl_control
INFO - Volume Path: /Volumes/catalog/schema/metadata
INFO - YAML files changed in volume, syncing...
INFO - Synced 5 sources from YAML files
INFO - Changes detected: 2 job(s) updated
INFO - Updating 2 module(s) due to metadata changes: ['module1', 'module2']
INFO - Job updated successfully for module1 (Job ID: 12345)
```

## Error Handling

- **Volume Sync Errors:** Logged as warnings, monitoring continues
- **Job Update Errors:** Logged as errors, other modules still processed
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
%pip install -e /Workspace/path/to/lakeflow-job-meta

# Or add to Python path
import sys
sys.path.insert(0, '/Workspace/path/to/lakeflow-job-meta')
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

