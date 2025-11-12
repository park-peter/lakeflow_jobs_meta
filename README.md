# Lakeflow Jobs Meta

A metadata-driven framework for orchestrating Databricks Lakeflow Jobs. Package as a library and run as a single task in a Lakeflow Jobs to continuously monitor for metadata changes and automatically update jobs.

## Features

- ✅ **Dynamic Job Generation**: Automatically creates/updates Databricks jobs from metadata
- ✅ **Continuous Monitoring**: Automatically detects metadata changes and updates jobs
- ✅ **Multiple Task Types**: Support for Notebook, SQL Query, SQL File, Python Wheel, Spark JAR, Pipeline, and dbt tasks
- ✅ **Advanced Task Features**: Support for run_if conditions, job clusters, environments, and notifications
- ✅ **Delta Table as Source of Truth**: Manage metadata directly in Delta tables
- ✅ **YAML Support**: YAML file ingestion for bulk updates
- ✅ **Dependency Management**: Handles execution order and task dependencies
- ✅ **Job Lifecycle**: Tracks and manages job IDs in Delta tables

## Architecture

```
┌─────────────────────────────────────────┐
│ Monitoring Job                          │
│  - Watches Delta Table                  │
│  - Watches Unity Catalog Volume (YAML)  │
│  - Auto-updates Jobs on Changes         │
└─────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    Delta Control Table    YAML Files (UC Volume)
           │                    │
           └──────────┬─────────┘
                      ▼
              Job Generator
                      ▼
            Databricks Jobs
```

## Package Structure

```
.
├── lakeflow_jobs_meta/         # Main package
│   ├── __init__.py
│   ├── main.py                 # Entry point for monitoring task
│   ├── constants.py
│   ├── utils.py
│   ├── task_builders.py
│   ├── orchestrator.py
│   ├── metadata_manager.py
│   └── monitor.py
├── examples/                     # Example files
│   ├── orchestrator_example.ipynb  # Orchestrator example notebook
│   ├── notebook_task/          # Example notebook tasks
│   │   └── sample_ingestion_notebook.ipynb # Example ingestion notebook task
│   ├── sql_file_task/          # SQL file task examples
│   │   ├── 01_create_sample_data.sql
│   │   ├── 02_daily_aggregations.sql
│   │   ├── 03_bronze_to_silver_transformation.sql
│   │   ├── 04_data_freshness_check.sql
│   │   └── 05_incremental_load.sql
│   └── metadata_examples.yaml   # Example metadata configurations
├── docs/                        # Documentation
│   ├── PACKAGING_AND_DEPLOYMENT.md
│   └── METADATA_MANAGEMENT.md
├── tests/                       # Test suite
│   ├── test_utils.py
│   ├── test_task_builders.py
│   ├── test_metadata_manager.py
│   ├── test_orchestrator.py
│   ├── test_monitor.py
│   └── test_constants.py
├── setup.py                     # Package setup
├── pyproject.toml              # Modern Python packaging
```

## Quick Start

### Installation

```bash
# Install from PyPI
pip install lakeflow-jobs-meta

# Or install from source (development)
pip install -e .

# Or install from wheel
pip install dist/lakeflow_jobs_meta-0.1.0-py3-none-any.whl
```

### Quick Example

```python
import lakeflow_jobs_meta as jm

# Load metadata from YAML file and create/update those jobs
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata.yaml",
    control_table="catalog.schema.etl_control"
)

# Or load from a folder (all YAML files)
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata/",
    control_table="catalog.schema.etl_control"
)

# Or create/update all jobs in control table
jobs = jm.create_or_update_jobs(
    control_table="catalog.schema.etl_control"
)
```

### Usage as a Lakeflow Jobs Task (Recommended)

1. **Use the orchestrator example** from `examples/orchestrator_example.ipynb` to create/update jobs on-demand, OR create a continuous monitoring job as shown below

2. **Configure Parameters** via Databricks widgets or base_parameters:
   ```python
   {
       "control_table": "your_catalog.schema.etl_control",  # Required
       "volume_path": "/Volumes/catalog/schema/metadata",  # Optional
       "check_interval_seconds": "60",  # Optional, default: 60
       "max_iterations": ""  # Optional, empty = infinite
   }
   ```

3. **Run the Job** - It will continuously monitor for changes and auto-update jobs

### Option 1: YAML File Ingestion (Recommended)

The `yaml_path` parameter accepts three types of paths:

**1. Single YAML File**
```python
import lakeflow_jobs_meta as jm

# Load and process jobs from a single YAML file
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata.yaml",
    control_table="your_catalog.schema.etl_control"
)
```

**2. Folder Path (All YAML Files)**
```python
import lakeflow_jobs_meta as jm

# Load and process all YAML files in folder (recursive)
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata/",
    control_table="your_catalog.schema.etl_control"
)
```

**3. Unity Catalog Volume with File Arrival Trigger (Recommended for Production)**

For production environments, use Databricks file arrival triggers to automatically process YAML files when they're uploaded to a Unity Catalog volume:

1. **Configure File Arrival Trigger**: Set up a file arrival trigger on your job to monitor the Unity Catalog volume. See [Databricks File Arrival Triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers).

2. **Upload YAML Files**: When YAML files are uploaded, the job automatically triggers.

3. **Process YAML Files**: The job loads and processes all YAML files from the volume:

```python
import lakeflow_jobs_meta as jm

# Automatically runs when file arrival trigger fires
jobs = jm.create_or_update_jobs(
    yaml_path="/Volumes/catalog/schema/metadata_volume",
    control_table="your_catalog.schema.etl_control"
)
```

**Benefits of File Arrival Triggers:**
- Automatic processing when files arrive (no polling needed)
- Efficient: Only triggers when files actually change
- Scalable: Handles large numbers of files efficiently
- No need for continuous monitoring jobs

**Advanced Usage: Load Separately**

You can also load YAML separately from orchestration:

```python
import lakeflow_jobs_meta as jm

# Load from file
num_tasks, job_names = jm.load_yaml("/Workspace/path/to/metadata.yaml")

# Load from folder
num_tasks, job_names = jm.load_from_folder("/Workspace/path/to/metadata/")

# Load from volume
num_tasks, job_names = jm.sync_from_volume("/Volumes/catalog/schema/volume")

# Then process all jobs in control table
jobs = jm.create_or_update_jobs(control_table="your_catalog.schema.etl_control")
```

### Option 2: Direct Delta Table Updates

For advanced use cases, you can update metadata directly in the Delta table:

```sql
-- Insert new task
INSERT INTO your_catalog.schema.etl_control VALUES (
  'my_pipeline',           -- job_name
  'sql_task_1',            -- task_key
  '[]',                     -- depends_on (JSON array of task_keys, empty for no dependencies)
  'sql_query',             -- task_type
  '{"timeout_seconds": 7200, "tags": {"department": "engineering"}}', -- job_config (JSON string)
  '{"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers", "parameters": {"catalog": "my_cat"}}', -- task_config (JSON string)
  false                    -- disabled
);
```

The monitoring job will automatically detect the change and update the job.

### Option 3: On-Demand Orchestration

Run the orchestrator manually (for development/testing):

```python
import lakeflow_jobs_meta as jm

# Process all jobs in control table
jobs = jm.create_or_update_jobs(
    control_table="your_catalog.schema.etl_control"
)

# Or load from YAML and process those jobs
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata.yaml",
    control_table="your_catalog.schema.etl_control"
)

# With custom configuration
jobs = jm.create_or_update_jobs(
    yaml_path="/Workspace/path/to/metadata/",
    control_table="your_catalog.schema.control_table",
    jobs_table="your_catalog.schema.custom_jobs_table",
    default_warehouse_id="your-warehouse-id",
    default_pause_status=False
)
```

**Note:** When `yaml_path` is provided, only jobs from that path are processed. When not provided, all jobs in the control table are processed.

### Pause Status Management

The `default_pause_status` parameter controls the initial pause state of jobs with triggers or schedules:

**`default_pause_status=False` (default behavior):**
- Jobs with continuous/schedule/trigger are created in **active** (UNPAUSED) state
- Jobs will execute according to their defined triggers/schedules
- Manual (on-demand) jobs are not affected and must be triggered manually

**`default_pause_status=True`:**
- Jobs with continuous/schedule/trigger are created in **paused** (PAUSED) state
- Jobs will NOT execute automatically until manually unpaused
- Manual (on-demand) jobs are not affected and must be triggered manually

**Explicit `pause_status` in YAML always overrides the default:**
```yaml
jobs:
  - job_name: "scheduled_job"
    continuous:
      pause_status: UNPAUSED  # Explicit - overrides default_pause_status
    tasks:
      - task_key: "my_task"
        # ...
```

**Pause Status for Updates:**
- For job updates, `default_pause_status` does NOT affect running jobs
- Pause status only changes if explicitly set in YAML metadata
- This prevents accidentally pausing production jobs during updates

**Example:**
```python
# Create jobs in paused state (for testing/staging)
jobs = jm.create_or_update_jobs(
    control_table="catalog.schema.etl_control",
    default_pause_status=True  # Jobs created paused
)

# Create jobs active (for production)
jobs = jm.create_or_update_jobs(
    control_table="catalog.schema.etl_control",
    default_pause_status=False  # Jobs created active (default)
)
```

### Job Update Behavior (Important!)

When updating existing jobs, the framework **completely replaces** the job definition with what's in your YAML metadata:

**What this means:**
- ✅ Job definition is fully synchronized with metadata
- ✅ Manual changes in Databricks UI are **overwritten**
- ✅ Parameters, tags, or settings not in metadata are **removed**
- ✅ Ensures consistency between metadata and deployed jobs

**Example:**
```yaml
# metadata.yaml
jobs:
  - job_name: "my_job"
    parameters:
      env: "prod"
      region: "us-west"
```

If you manually added a `debug: "true"` parameter in the Databricks UI, running `create_or_update_jobs` will:
1. Remove the `debug` parameter (not in metadata)
2. Keep only `env` and `region` (defined in metadata)

**Best Practice:**
- Always define all job settings in YAML metadata
- Avoid manual changes in Databricks UI for managed jobs
- Use metadata as the single source of truth

## Testing

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=lakeflow_jobs_meta --cov-report=html

# Run specific test file
pytest tests/test_utils.py

# Run specific test
pytest tests/test_utils.py::TestSanitizeTaskKey::test_basic_sanitization
```

### Test Structure

Tests are organized in the `tests/` directory:
- `test_utils.py` - Utility function tests
- `test_task_builders.py` - Task creation tests
- `test_metadata_manager.py` - Metadata management tests
- `test_orchestrator.py` - Orchestration tests
- `test_monitor.py` - Monitoring tests
- `test_constants.py` - Constants validation

All tests use pytest with mocking for external dependencies (Databricks SDK, Spark, dbutils).

## Metadata Schema

Each job in your YAML must have:

```yaml
jobs:
  - job_name: "my_pipeline"      # Required: Job name (becomes job_name in control table)
    description: "Pipeline description"  # Optional: Description
    timeout_seconds: 7200          # Optional: Job timeout (default: 7200)
    max_concurrent_runs: 2         # Optional: Max concurrent runs (default: 1)
    parameters:                    # Optional: Job-level parameters (pushed down to tasks automatically)
      default_catalog: "my_catalog"
    tags:                          # Optional: Job tags
      department: "engineering"
    queue:                         # Optional: Job queue settings
      enabled: true
    continuous:                    # Optional: Continuous job settings
      pause_status: UNPAUSED
      task_retry_mode: ON_FAILURE
    trigger:                       # Optional: Job trigger (file_arrival, table_update, or periodic)
      pause_status: UNPAUSED
      file_arrival:
        url: /Volumes/catalog/schema/folder/
    # OR use schedule instead of trigger:
    # schedule:                    # Optional: Scheduled job (cron)
    #   quartz_cron_expression: "13 2 15 * * ?"
    #   timezone_id: UTC
    #   pause_status: UNPAUSED
    tasks:
      - task_key: "unique_id"        # Required: Unique task identifier
        task_type: "sql_query"       # Required: Type of task (see "Task Types and Parameters" section below)
        depends_on: []               # Optional: List of task_key strings this task depends on (default: [])
        disabled: false              # Optional: Whether task is disabled (default: false)
        timeout_seconds: 3600         # Optional: Task-level timeout (default: 3600)
        run_if: "ALL_SUCCESS"        # Optional: Run condition (see common parameters below)
        job_cluster_key: "Job_cluster"  # Optional: Reference to job-level cluster definition
        # OR existing_cluster_id: "1106-160244-2ko4u9ke"  # Optional: Reference to existing all-purpose cluster
        environment_key: "default_python"  # Optional: Reference to job-level environment
        warehouse_id: "abc123"       # Required for SQL/dbt tasks: Warehouse ID
        sql_query: "SELECT * FROM ..."  # For sql_query: Inline SQL query
        # OR query_id: "abc123"      # For sql_query: Use saved query ID
        notification_settings:       # Optional: Task-level notification settings
          email_notifications:
            on_start: []
            on_success: []
            on_failure: ["alerts@example.com"]
            on_duration_warning_threshold_exceeded: []
          alert_on_last_attempt: true
        parameters:                  # Optional: Task parameters
          catalog: "my_catalog"
          schema: "my_schema"
          start_date: "{{job.start_time.iso_date}}"
```

### Control Table Schema

The control table has the following schema:

```sql
CREATE TABLE control_table (
    job_name STRING,              -- Job name (from job_name in YAML)
    task_key STRING,              -- Unique task identifier
    depends_on STRING,            -- JSON array of task_key strings this task depends on
    task_type STRING,             -- Task type: notebook, sql_query, sql_file, python_wheel, spark_jar, pipeline, or dbt
    job_config STRING,            -- JSON string with job-level settings (tags, parameters, timeout_seconds, etc.)
    task_config STRING,           -- JSON string with task-specific config including parameters
    disabled BOOLEAN DEFAULT false, -- Whether task is disabled
    created_by STRING,            -- Username who created the record
    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
    updated_by STRING,            -- Username who last updated the record
    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
)
```

### Jobs Table Schema

The jobs tracking table has the following schema:

```sql
CREATE TABLE jobs_table (
    job_name STRING,              -- Job name (same as job_name in control table)
    job_id BIGINT,                -- Databricks job ID
    created_by STRING,            -- Username who created the record
    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
    updated_by STRING,            -- Username who last updated the record
    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
)
```

**Note:** `job_name` in the jobs table is the same as `job_name` in the control table. Both refer to the same job name from the YAML `job_name` field.

### Job-Level Settings

Job-level settings control the behavior of the entire Databricks job. These settings are defined directly at the job level (not nested under `job_config`):

- `timeout_seconds`: Maximum time the job can run (default: 7200 seconds)
- `max_concurrent_runs`: Maximum number of concurrent runs (default: 1)
- `parameters`: Job-level parameters (key-value pairs) that are automatically pushed down to tasks that support key-value parameters
- `tags`: Job tags (key-value pairs)
- `job_clusters`: Job-level cluster definitions (referenced by tasks via `job_cluster_key`)
- `environments`: Job-level environment definitions (referenced by tasks via `environment_key`)
- `notification_settings`: Job-level notification settings
- `queue`: Job queue settings (`enabled: true/false`)
- `continuous`: Continuous job settings
  - `pause_status`: `UNPAUSED` or `PAUSED`
  - `task_retry_mode`: `ON_FAILURE` or `NEVER`
- `trigger`: Job trigger settings (file arrival, table update, or periodic)
  - `pause_status`: `UNPAUSED` or `PAUSED`
  - `file_arrival`: Trigger on file arrival
    - `url`: Path to monitor (e.g., `/Volumes/catalog/schema/folder/`)
  - `table_update`: Trigger on table updates
    - `table_names`: List of table names to monitor (e.g., `["catalog.schema.table"]`)
  - `periodic`: Periodic trigger
    - `interval`: Interval number
    - `unit`: Time unit (`DAYS`, `HOURS`, `MINUTES`, etc.)
- `schedule`: Scheduled job using cron expression
  - `quartz_cron_expression`: Cron expression (e.g., `"13 2 15 * * ?"`)
  - `timezone_id`: Timezone (e.g., `"UTC"`)
  - `pause_status`: `UNPAUSED` or `PAUSED`
- `tags`: Job-level tags as key-value pairs (e.g., `{department: "engineering", project: "data_pipeline"}`)
- `job_clusters`: List of job cluster definitions for shared cluster configurations
  - Each cluster definition has:
    - `job_cluster_key`: Key name for the cluster (e.g., `"Job_cluster"`)
    - `new_cluster`: Full cluster specification (spark_version, node_type_id, num_workers, aws_attributes, custom_tags, etc.)
- `environments`: List of environment definitions for Python/Scala dependencies
  - Each environment has:
    - `environment_key`: Key name for the environment
    - `spec`: Environment specification (dependencies, java_dependencies, environment_version)
- `notification_settings`: Job-level notification settings
  - `email_notifications`: Email notification configuration
    - `on_start`: List of email addresses to notify when job starts
    - `on_success`: List of email addresses to notify on success
    - `on_failure`: List of email addresses to notify on failure
    - `on_duration_warning_threshold_exceeded`: List of email addresses for duration warnings
  - `no_alert_for_skipped_runs`: Whether to skip alerts for skipped runs
  - `no_alert_for_canceled_runs`: Whether to skip alerts for canceled runs
  - `alert_on_last_attempt`: Whether to alert only on last retry attempt

## Task Types and Parameters

All task types support these common parameters:

- `task_key`: Required - Unique task identifier
- `task_type`: Required - Type of task (see supported types below)
- `depends_on`: Optional - List of task_key strings this task depends on (default: `[]`)
- `disabled`: Optional - Whether task is disabled (default: `false`)
- `timeout_seconds`: Optional - Maximum time the task can run (default: 3600 seconds)
- `run_if`: Optional - Run condition for the task
  - `ALL_SUCCESS`: Run only if all dependencies succeed
  - `AT_LEAST_ONE_SUCCESS`: Run if at least one dependency succeeds
  - `NONE_FAILED`: Run if no dependencies fail
  - `ALL_DONE`: Run when all dependencies complete (regardless of status)
  - `AT_LEAST_ONE_FAILED`: Run if at least one dependency fails
  - `ALL_FAILED`: Run only if all dependencies fail
- `job_cluster_key`: Optional - Reference to a job-level cluster definition (use with `job_clusters` in `job_config`)
- `existing_cluster_id`: Optional - Reference to an existing all-purpose cluster in the workspace
  - Note: Tasks can use either `job_cluster_key` OR `existing_cluster_id`, but not both
- `environment_key`: Optional - Reference to a job-level environment definition
- `notification_settings`: Optional - Task-level notification settings (same structure as job-level)

#### 1. Notebook Tasks (`task_type: "notebook"`)

**Required Parameters:**
- `file_path`: Path to the notebook file (e.g., `/Workspace/Users/user@example.com/my_notebook`)

**Optional Parameters:**
- `parameters`: Dictionary of parameters passed to the notebook as `base_parameters`
  - Only user-defined parameters from metadata are passed
  - If your notebook uses widgets (e.g., `task_key`, `control_table`), Databricks Jobs UI automatically adds them

**Example:**
```yaml
- task_key: "ingest_data"
  task_type: "notebook"
  file_path: "/Workspace/Users/user@example.com/ingestion_notebook"
  parameters:
    catalog: "my_catalog"
    schema: "my_schema"
    source_table: "source_table"
```

**Note:** If your notebook defines widgets like `dbutils.widgets.text("task_key", "default")`, the Databricks Jobs UI will automatically populate them when the notebook runs as a job task. You don't need to include these in the `parameters` field.

#### 2. SQL Query Tasks (`task_type: "sql_query"`)

**Required Parameters:**
- `warehouse_id`: SQL warehouse ID (can be provided via `default_warehouse_id` in orchestrator)
- Either `sql_query` OR `query_id`:
  - `sql_query`: Inline SQL query string
  - `query_id`: ID of a saved query in Databricks SQL

**Optional Parameters:**
- `parameters`: Dictionary of parameters for SQL parameter substitution (`:parameter_name`)

**Example:**
```yaml
- task_key: "validate_data"
  task_type: "sql_query"
  warehouse_id: "abc123"
  sql_query: "SELECT COUNT(*) as row_count FROM :catalog.:schema.customers"
    parameters:
    catalog: "my_catalog"
    schema: "my_schema"
```

#### 3. SQL File Tasks (`task_type: "sql_file"`)

**Required Parameters:**
- `warehouse_id`: SQL warehouse ID (can be provided via `default_warehouse_id` in orchestrator)
- `file_path`: Path to the SQL file (e.g., `/Workspace/Users/user@example.com/query.sql`)

**Optional Parameters:**
- `parameters`: Dictionary of parameters for SQL parameter substitution (`:parameter_name`)

**Example:**
```yaml
- task_key: "transform_data"
  task_type: "sql_file"
  warehouse_id: "abc123"
  file_path: "/Workspace/Users/user@example.com/transformations.sql"
    parameters:
    catalog: "my_catalog"
    schema: "my_schema"
```

#### 4. Python Wheel Tasks (`task_type: "python_wheel"`)

**Required Parameters:**
- `package_name`: Name of the Python wheel package
- `entry_point`: Entry point function name in the package

**Optional Parameters:**
- `parameters`: List of parameters to pass to the entry point function (can be a list or dict)

**Example:**
```yaml
- task_key: "run_python_wheel"
  task_type: "python_wheel"
  package_name: "my_package"
  entry_point: "main"
  parameters: ["arg1", "arg2", "arg3"]
```

#### 5. Spark JAR Tasks (`task_type: "spark_jar"`)

**Required Parameters:**
- `main_class_name`: Fully qualified name of the main class (e.g., `com.example.MainClass`)

**Optional Parameters:**
- `parameters`: List of parameters to pass to the main class (can be a list or dict)

**Example:**
```yaml
- task_key: "run_spark_jar"
  task_type: "spark_jar"
  main_class_name: "com.example.MainClass"
  parameters: ["param1", "param2"]
```

#### 6. Pipeline Tasks (`task_type: "pipeline"`)

**Required Parameters:**
- `pipeline_id`: ID of the Lakeflow Declarative Pipeline to run

**Example:**
```yaml
- task_key: "run_pipeline"
  task_type: "pipeline"
  pipeline_id: "1165597e-f650-4bf3-9a4f-fc2f2d40d2c3"
```

#### 7. dbt Tasks (`task_type: "dbt"`)

**Required Parameters:**
- `commands`: dbt command to execute (e.g., `"dbt run --models my_model"`)
- `warehouse_id`: SQL warehouse ID (can be provided via `default_warehouse_id` in orchestrator, which is used as default if not specified)

**Optional Parameters:**
- `profiles_directory`: Path to dbt profiles directory
- `project_directory`: Path to dbt project directory
- `catalog`: Catalog name for dbt
- `schema`: Schema name for dbt

**Example:**
```yaml
- task_key: "run_dbt"
  task_type: "dbt"
  commands: "dbt run --models my_model"
  warehouse_id: "abc123"
  profiles_directory: "/Workspace/Users/user@example.com/dbt-profiles"
  project_directory: "/Workspace/Users/user@example.com/dbt-project"
  catalog: "main"
  schema: "analytics"
```

### Task Disabling

The `disabled` field in the control table controls whether a task is disabled in the Databricks job:

- `disabled: false` → Task is enabled (default)
- `disabled: true` → Task is disabled

Disabled tasks are included in the job definition but will not execute when the job runs. This allows you to temporarily disable tasks without removing them from the job. Dependencies on disabled tasks are still satisfied (the task exists, it just doesn't execute).

### Audit Fields

Both the control table and jobs table automatically track user information:

- `created_by`: Username of the user who created the record (set only on INSERT, never updated)
- `updated_by`: Username of the user who last updated the record (set on INSERT and UPDATE)
- `created_timestamp`: Timestamp when the record was created
- `updated_timestamp`: Timestamp when the record was last updated

These fields are automatically populated using Spark SQL's `current_user()` function.

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

## Examples

See `examples/metadata_examples.yaml` for comprehensive examples including:
- Data quality checks
- Bronze to Silver transformations
- Mixed task type pipelines
- End-to-end data pipelines

## Best Practices

1. **Use SQL Files for Reusable Logic**: Store common SQL transformations in files
2. **Use Task Dependencies**: Design your pipelines with clear dependencies using `depends_on`
3. **Parameterize SQL**: Use parameters and Databricks dynamic value references for flexible SQL
4. **Follow Naming Conventions**: Use clear, descriptive `task_key` values
5. **Document Your Pipelines**: Add descriptions to jobs
6. **Use Task Parameters**: Leverage Databricks dynamic value references like `{{job.id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}` for runtime values
7. **Unified Parameters**: Use the `parameters` field for all task parameters instead of separate `source_config`/`target_config`

## Future Enhancements

- [ ] PowerBI refresh Tasks
- [ ] Enhanced error handling and retry logic
- [ ] Execution monitoring dashboard
- [ ] Data quality framework
- [ ] Databricks Apps

## Contributing

This is an internal project. For questions or enhancements, contact Peter Park (peter.park@databricks.com).