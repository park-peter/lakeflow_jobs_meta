# Metadata Management Guide

## Overview

This framework uses **Delta tables as the primary source of truth** for metadata, with YAML files as an optional ingestion mechanism.

## Architecture Decision

### Why Delta Tables?

1. **Direct SQL Updates**: Update metadata without file management
2. **Version Control**: Delta time travel provides built-in versioning
3. **Real-time Updates**: Changes are immediately available
4. **Collaboration**: Multiple users can update different modules simultaneously
5. **Audit Trail**: Automatic `created_timestamp` and `updated_timestamp` tracking
6. **Unity Catalog Integration**: Proper permissions and governance

### YAML Files: Optional, Not Required

YAML files are supported for:
- **Initial Bootstrap**: Loading initial metadata
- **Bulk Updates**: Updating many sources at once
- **Version Control**: Storing metadata in Git
- **CI/CD**: Deploying metadata via pipelines

But you can also manage everything directly in Delta tables!

---

## Workflow Options

### Workflow 1: Direct Table Updates (Recommended) ⭐

**Best for:** Day-to-day operations, real-time updates

```sql
-- Add a new source
INSERT INTO fe_ppark_demo.job_demo.etl_control VALUES (
  'new_sql_task',
  'my_module',
  'sql',
  1,
  '{}',
  '{"catalog": "bronze", "schema": "data", "table": "customers"}',
  '{"task_type": "sql_query", "sql_task": {"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers"}}',
  true
);

-- Update existing source
UPDATE fe_ppark_demo.job_demo.etl_control
SET execution_order = 2,
    transformation_config = '{"task_type": "sql_query", "sql_task": {...}}',
    updated_timestamp = current_timestamp()
WHERE source_id = 'existing_source' AND module_name = 'my_module';

-- Deactivate source (soft delete)
UPDATE fe_ppark_demo.job_demo.etl_control
SET is_active = false,
    updated_timestamp = current_timestamp()
WHERE source_id = 'old_source';
```

**Then run orchestrator:**
```python
from lakeflow_job_meta import JobOrchestrator

orchestrator = JobOrchestrator(CONTROL_TABLE)
jobs = orchestrator.run_all_modules()
```

---

### Workflow 2: YAML File Ingestion

**Best for:** Initial setup, bulk migrations, Git-based workflows

```python
# Load YAML into table
from lakeflow_job_meta import MetadataManager, JobOrchestrator

manager = MetadataManager(CONTROL_TABLE)
manager.load_yaml('./examples/metadata_examples.yaml')

# Then orchestrate
orchestrator = JobOrchestrator(CONTROL_TABLE)
jobs = orchestrator.run_all_modules()
```

**Or pass YAML path directly to orchestrator:**
```python
from lakeflow_job_meta import JobOrchestrator

orchestrator = JobOrchestrator(CONTROL_TABLE)
jobs = orchestrator.run_all_modules(
    yaml_path='./examples/metadata_examples.yaml',
    sync_yaml=True
)
```

---

### Workflow 3: Unity Catalog Volume Monitoring

**Best for:** Production environments with automated deployments

```python
from lakeflow_job_meta import MetadataMonitor

# Continuously monitor volume for YAML files
monitor = MetadataMonitor(
    control_table=CONTROL_TABLE,
    volume_path='/Volumes/catalog/schema/metadata_volume',
    check_interval_seconds=60
)
monitor.run_continuous()
```

**Setup:**
1. Store YAML files in Unity Catalog volume
2. Run monitoring job continuously
3. When YAML files are added/updated, they're automatically synced and jobs are updated

---

## Change Detection

The framework automatically detects changes using:

1. **Timestamp Comparison**: `updated_timestamp > last_check_timestamp`
2. **New Sources**: `created_timestamp == updated_timestamp`
3. **Status Changes**: `is_active` flag changes

### Manual Change Detection

```python
from lakeflow_job_meta import MetadataManager

manager = MetadataManager(CONTROL_TABLE)
changes = manager.detect_changes(last_check_timestamp=None)
print(changes)
# {
#   'new_modules': ['module1'],
#   'updated_modules': ['module2'],
#   'deactivated_modules': [],
#   'changed_sources': [...]
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

1. **Prefer Direct Table Updates**: Most flexible and immediate
2. **Use YAML for Bulk Changes**: Great for initial setup or major migrations
3. **Enable Monitoring in Production**: Use continuous monitoring for automated updates
4. **Version Control YAML**: Store YAML files in Git for change tracking

### For Administrators

1. **Set Up Monitoring Job**: Create scheduled job running `MetadataMonitor.run_continuous()` continuously
2. **Use Unity Catalog Volumes**: Store YAML files in volumes for automatic sync
3. **Control Permissions**: Use Unity Catalog to control who can update control table
4. **Monitor Changes**: Query `updated_timestamp` to track metadata changes

---

## Migration from YAML-Only to Delta-First

If you're currently using YAML files:

1. **Initial Load**: Run `MetadataManager.load_yaml()` once to bootstrap
2. **Switch to Table Updates**: Start updating Delta table directly
3. **Keep YAML for Backup**: Periodically export table to YAML for version control
4. **Enable Monitoring**: Set up continuous monitoring if desired

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

