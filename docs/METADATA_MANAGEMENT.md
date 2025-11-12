# Metadata Management Guide

## Overview

This framework uses **Delta tables as the source of truth** for metadata, with YAML files as the recommended ingestion mechanism. YAML files provide version control, easy editing, and can be automatically processed using Databricks file arrival triggers.

---

## Workflow Options

### Workflow 1: YAML File Ingestion (Recommended) ⭐

**Best for:** Most use cases - version control, easy editing, CI/CD integration

The `yaml_path` parameter accepts three types of paths:

**Option 1a: Single YAML File**

```python
import lakeflow_jobs_meta as jm

# Load and process a single YAML file
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata.yaml",
    control_table=CONTROL_TABLE
)
```

**Option 1b: Folder Path (All YAML Files)**

```python
import lakeflow_jobs_meta as jm

# Load and process all YAML files in folder (recursive)
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata/",
    control_table=CONTROL_TABLE
)
```

**Option 1c: Unity Catalog Volume with File Arrival Trigger (Recommended for Production)**

For production environments, use Databricks file arrival triggers to automatically process YAML files when they're uploaded to a Unity Catalog volume:

1. **Configure File Arrival Trigger**: Set up a file arrival trigger on your job to monitor the Unity Catalog volume. See [Databricks File Arrival Triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers).

2. **Upload YAML Files**: When YAML files are uploaded, the job automatically triggers.

3. **Process YAML Files**: The job loads and processes all YAML files:

```python
import lakeflow_jobs_meta as jm

# Automatically runs when file arrival trigger fires
jobs = jm.create_or_update_jobs(
    yaml_path="/Volumes/catalog/schema/metadata_volume",
    control_table=CONTROL_TABLE
)
```

**Benefits of File Arrival Triggers:**
- Automatic processing when files arrive (no polling needed)
- Efficient: Only triggers when files actually change
- Scalable: Handles large numbers of files efficiently
- No need for continuous monitoring jobs

**Advanced Usage:**

Load YAML separately from orchestration:

```python
import lakeflow_jobs_meta as jm

# Load from file, folder, or volume
num_tasks, job_names = jm.load_yaml("/Workspace/path/to/metadata.yaml")
num_tasks, job_names = jm.load_from_folder("/Workspace/path/to/metadata/")
num_tasks, job_names = jm.sync_from_volume("/Volumes/catalog/schema/volume")

# Then process all jobs in control table
jobs = jm.create_or_update_jobs(control_table=CONTROL_TABLE)
```

---

### Workflow 2: Direct Delta Table Updates

**Best for:** Advanced use cases, programmatic updates

```sql
-- Add a new task
INSERT INTO fe_ppark_demo.job_demo.etl_control VALUES (
  'my_job',
  'new_sql_task',
  '[]',  -- depends_on (JSON array, empty for no dependencies)
  'sql_query',
  '{"catalog": "bronze", "schema": "data"}',
  '{"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers"}',
  false
);

-- Update existing task dependencies
UPDATE fe_ppark_demo.job_demo.etl_control
SET depends_on = '["task_a", "task_b"]',  -- depends_on as JSON array
    task_config = '{"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers WHERE date > CURRENT_DATE()"}',
    updated_timestamp = current_timestamp()
WHERE job_name = 'my_job' AND task_key = 'existing_task';

-- Disable task
UPDATE fe_ppark_demo.job_demo.etl_control
SET disabled = true,
    updated_timestamp = current_timestamp()
WHERE job_name = 'my_job' AND task_key = 'old_task';
```

**Then run orchestrator:**
```python
from lakeflow_jobs_meta import JobOrchestrator

orchestrator = JobOrchestrator(control_table=CONTROL_TABLE)
jobs = orchestrator.create_or_update_jobs()
```

---

## Change Detection

The framework automatically detects changes using:

1. **Timestamp Comparison**: `updated_timestamp > last_check_timestamp`
2. **New Sources**: `created_timestamp == updated_timestamp`
3. **Status Changes**: `is_active` flag changes

### Manual Change Detection

```python
from lakeflow_jobs_meta import MetadataManager

manager = MetadataManager(CONTROL_TABLE)
changes = manager.detect_changes(last_check_timestamp=None)
print(changes)
# {
#   'new_jobs': ['job1'],
#   'updated_jobs': ['job2'],
#   'disabled_jobs': [],
#   'changed_tasks': [...]
# }
```

---

## Update Strategy: Update-in-Place

We **update existing jobs** rather than creating new ones:

✅ **Benefits:**
- Stable job IDs (external systems can reference them)
- Preserves run history
- Maintains permissions and settings
- Keeps existing schedules

**How it works:**
1. Framework stores job_id in `{control_table}_jobs` table
2. When metadata changes, updates the existing job using stored job_id
3. If job doesn't exist (deleted), creates new job and stores new job_id

---

## Best Practices

### For End Users

1. **Use YAML Files**: Store metadata in YAML files for version control and easy editing
2. **Use File Arrival Triggers**: Set up file arrival triggers for automatic processing in production
3. **Version Control**: Store YAML files in Git for change tracking
4. **Direct Table Updates**: Use for programmatic updates or advanced use cases

### For Administrators

1. **Set Up File Arrival Triggers**: Configure file arrival triggers on jobs to monitor Unity Catalog volumes
2. **Use Unity Catalog Volumes**: Store YAML files in volumes for automatic processing
3. **Control Permissions**: Use Unity Catalog to control who can update control table
4. **Monitor Changes**: Query `updated_timestamp` to track metadata changes

---

## Task Dependencies

Tasks use the `depends_on` field to specify dependencies. Tasks without dependencies (or with empty `depends_on`) run first in parallel. Tasks with dependencies wait for all their dependencies to complete before running.

Example:
```yaml
tasks:
  - task_key: "task_a"
    depends_on: []  # No dependencies, runs first
  - task_key: "task_b"
    depends_on: []  # No dependencies, runs in parallel with task_a
  - task_key: "task_c"
    depends_on: ["task_a", "task_b"]  # Waits for both task_a and task_b to complete
```

The framework automatically resolves dependencies and creates the correct execution order. Circular dependencies are detected and will cause an error.

---

## FAQ

**Q: Can I still use YAML files?**  
A: Yes! YAML files are fully supported as an ingestion mechanism.

**Q: What happens if I update the table directly?**  
A: Changes are immediately available. Run the orchestrator to update jobs.

**Q: Can I use both YAML and direct table updates?**  
A: Yes, but be careful - YAML ingestion will overwrite table data. Use one approach consistently, or sync carefully.

**Q: How do I know what changed?**  
A: Use `MetadataManager.detect_changes()` or query `updated_timestamp` column.

**Q: What if I delete a job manually?**  
A: Framework will detect it doesn't exist and create a new one, updating the job_id in the tracking table.

