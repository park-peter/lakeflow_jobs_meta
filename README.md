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

The `default_pause_status` parameter controls the initial behavior of **newly created jobs**:

**`default_pause_status=False` (default behavior):**
- **Manual jobs** (no schedule/trigger/continuous): **Auto-run immediately** after creation
- **Jobs with schedule/trigger/continuous**: Created **active** (UNPAUSED), run according to schedule

**`default_pause_status=True`:**
- **Manual jobs**: Do **NOT** auto-run after creation
- **Jobs with schedule/trigger/continuous**: Created **paused** (PAUSED), won't run until unpaused

**Explicit `pause_status` in YAML always overrides the default:**

The `pause_status` is set **within** the `continuous`, `schedule`, or `trigger` configuration:

```yaml
jobs:
  - job_name: "scheduled_job"
    continuous:
      pause_status: UNPAUSED  # Explicit - overrides default_pause_status
      task_retry_mode: ON_FAILURE
    tasks:
      - task_key: "my_task"
        # ...
  
  - job_name: "cron_job"
    schedule:
      quartz_cron_expression: "0 0 2 * * ?"
      timezone_id: "America/Los_Angeles"
      pause_status: PAUSED  # Explicit - job won't run until unpaused
    tasks:
      - task_key: "my_task"
        # ...
```

**Behavior:**
- If `pause_status` is **explicitly set**: That value is used (for both create and update)
- If `pause_status` is **NOT set**: 
  - For **NEW jobs**: `default_pause_status` is applied
  - For **UPDATES**: Existing pause status is unchanged

**For Job Updates:**
- `default_pause_status` has **NO effect** on job updates
- Jobs are **never** auto-run when updating existing jobs
- Pause status only changes if explicitly set in YAML metadata
- This prevents accidentally pausing or running production jobs during updates

### UI Lock Protection:

By default, all jobs created by this framework are set with `edit_mode: UI_LOCKED`, which means:
- ✅ Jobs **cannot be edited** in the Databricks UI (default)
- ✅ Prevents accidental manual modifications
- ✅ Enforces metadata as the single source of truth
- ✅ Users can still view job details and run history
- ✅ Changes must be made in YAML metadata

**Override Edit Mode (Optional):**

If you need to allow UI editing for specific jobs, you can set `edit_mode: EDITABLE` in your YAML:

```yaml
jobs:
  - job_name: "experimental_job"
    edit_mode: EDITABLE  # Allow UI editing for this job
    tasks:
      - task_key: "my_task"
        # ...
```

**Note:** It's recommended to keep `edit_mode: UI_LOCKED` (default) for production jobs to maintain consistency.

**Best Practice:**
- Always define all job settings in YAML metadata
- All changes must go through the metadata workflow
- Use version control (Git) to track metadata changes
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

### Job-Level Configuration

Each job in your YAML is defined with the following structure:

#### Required Fields

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `job_name` | String | Unique identifier for the job | `"my_pipeline"` |
| `tasks` | List | List of task definitions (see Task Configuration below) | See Task Configuration |

#### Optional Fields

| Parameter | Type | Default | Description | Possible Values |
|-----------|------|---------|-------------|-----------------|
| `description` | String | None | Human-readable description of the job | Any string |
| `timeout_seconds` | Integer | `7200` | Maximum time the entire job can run (seconds) | Any positive integer |
| `max_concurrent_runs` | Integer | `1` | Maximum number of concurrent runs allowed | Any positive integer |
| `edit_mode` | String | `"UI_LOCKED"` | Whether job can be edited in Databricks UI | `"UI_LOCKED"`, `"EDITABLE"` |
| `parameters` | Object | None | Job-level parameters (automatically passed to tasks) | Key-value pairs |
| `tags` | Object | None | Job tags for organization and filtering | Key-value pairs |
| `queue` | Object | None | Job queue settings | `{enabled: true/false}` |
| `continuous` | Object | None | Continuous job settings (always-on) | See Continuous Settings below |
| `trigger` | Object | None | Job trigger settings (file arrival, table update, or periodic) | See Trigger Settings below |
| `schedule` | Object | None | Scheduled job settings (cron-based) | See Schedule Settings below |
| `job_clusters` | List | None | Shared cluster definitions for tasks | See Job Clusters below |
| `environments` | List | None | Python/Scala environment definitions | See Environments below |
| `notification_settings` | Object | None | Job-level notification settings | See Notification Settings below |

#### Continuous Settings

Used for always-on streaming jobs:

| Parameter | Type | Default | Description | Possible Values |
|-----------|------|---------|-------------|-----------------|
| `pause_status` | String | `"UNPAUSED"` | Initial pause state | `"UNPAUSED"`, `"PAUSED"` |
| `task_retry_mode` | String | `"ON_FAILURE"` | When to retry failed tasks | `"ON_FAILURE"`, `"NEVER"` |

**Example:**
```yaml
continuous:
  pause_status: UNPAUSED
  task_retry_mode: ON_FAILURE
```

#### Schedule Settings

Used for cron-scheduled jobs:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `quartz_cron_expression` | String | Yes | Quartz cron expression | `"0 0 2 * * ?"` (daily at 2am) |
| `timezone_id` | String | Yes | Timezone for the schedule | `"UTC"`, `"America/Los_Angeles"` |
| `pause_status` | String | No | Initial pause state | `"UNPAUSED"`, `"PAUSED"` |

**Example:**
```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Los_Angeles"
  pause_status: UNPAUSED
```

#### Trigger Settings

Used for event-driven jobs (file arrival, table update, or periodic):

| Trigger Type | Parameters | Description | Example |
|--------------|------------|-------------|---------|
| **File Arrival** | `file_arrival.url` | Trigger when files arrive at path | `url: /Volumes/catalog/schema/folder/` |
| **Table Update** | `table_update.table_names` | Trigger when tables are updated | `table_names: ["catalog.schema.table"]` |
| **Periodic** | `periodic.interval`<br>`periodic.unit` | Trigger at regular intervals | `interval: 1`<br>`unit: HOURS` |
| **All Types** | `pause_status` | Initial pause state | `UNPAUSED` or `PAUSED` |

**Example:**
```yaml
trigger:
  pause_status: UNPAUSED
  file_arrival:
    url: /Volumes/catalog/schema/metadata/
```

#### Job Clusters

Define shared cluster configurations that tasks can reference:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `job_cluster_key` | String | Unique identifier for this cluster | `"Job_cluster"` |
| `new_cluster` | Object | Full cluster specification | See Databricks cluster API docs |

**Example:**
```yaml
job_clusters:
  - job_cluster_key: "Job_cluster"
    new_cluster:
      spark_version: "16.4.x-scala2.12"
      node_type_id: "rd-fleet.xlarge"
      num_workers: 8
      aws_attributes:
        availability: SPOT_WITH_FALLBACK
      data_security_mode: SINGLE_USER
      runtime_engine: PHOTON
```

#### Environments

Define Python/Scala environments that tasks can reference:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `environment_key` | String | Unique identifier for this environment | `"default_python"` |
| `spec.dependencies` | List | Python dependencies (pip packages) | `["dbt-databricks>=1.0.0"]` |
| `spec.java_dependencies` | List | Java/Scala dependencies (JARs) | `["/Workspace/.../file.jar"]` |
| `spec.environment_version` | String | Databricks environment version | `"4"`, `"4-scala-preview"` |

**Example:**
```yaml
environments:
  - environment_key: "default_python"
    spec:
      dependencies:
        - "dbt-databricks>=1.0.0"
        - "pandas>=1.5.0"
      environment_version: "4"
```

#### Notification Settings

Configure job-level email notifications:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `email_notifications.on_start` | List | Email addresses to notify on job start | `["team@example.com"]` |
| `email_notifications.on_success` | List | Email addresses to notify on success | `["team@example.com"]` |
| `email_notifications.on_failure` | List | Email addresses to notify on failure | `["alerts@example.com"]` |
| `email_notifications.on_duration_warning_threshold_exceeded` | List | Email addresses for duration warnings | `["alerts@example.com"]` |
| `no_alert_for_skipped_runs` | Boolean | Skip alerts for skipped runs | `true`, `false` |
| `no_alert_for_canceled_runs` | Boolean | Skip alerts for canceled runs | `true`, `false` |
| `alert_on_last_attempt` | Boolean | Alert only on last retry attempt | `true`, `false` |

**Example:**
```yaml
notification_settings:
  email_notifications:
    on_failure: ["alerts@example.com"]
    on_duration_warning_threshold_exceeded: ["alerts@example.com"]
  alert_on_last_attempt: true
```

### Task Configuration

Each task within a job has the following structure:

#### Required Fields

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `task_key` | String | Unique identifier for this task within the job | `"ingest_data"` |
| `task_type` | String | Type of task to execute | See Task Types section below |

#### Common Optional Fields

These fields are available for all task types:

| Parameter | Type | Default | Description | Possible Values |
|-----------|------|---------|-------------|-----------------|
| `depends_on` | List | `[]` | List of task_key values this task depends on | `["task_a", "task_b"]` |
| `disabled` | Boolean | `false` | Whether this task is disabled | `true`, `false` |
| `timeout_seconds` | Integer | `3600` | Maximum time this task can run (seconds) | Any positive integer |
| `run_if` | String | `"ALL_SUCCESS"` | Condition for when this task should run | See Run Conditions below |
| `job_cluster_key` | String | None | Reference to job-level cluster definition | Must match a `job_clusters` key |
| `existing_cluster_id` | String | None | Reference to existing all-purpose cluster | Databricks cluster ID |
| `environment_key` | String | None | Reference to job-level environment | Must match an `environments` key |
| `notification_settings` | Object | None | Task-level notification settings | Same structure as job-level |
| `parameters` | Object | None | Task-specific parameters | Key-value pairs or list (depends on task type) |

**Note:** Use either `job_cluster_key` OR `existing_cluster_id`, not both.

#### Run Conditions

The `run_if` parameter controls when a task executes based on dependency outcomes:

| Value | Description |
|-------|-------------|
| `ALL_SUCCESS` | Run only if all dependencies succeed (default) |
| `AT_LEAST_ONE_SUCCESS` | Run if at least one dependency succeeds |
| `NONE_FAILED` | Run if no dependencies fail (success or skipped) |
| `ALL_DONE` | Run when all dependencies complete (regardless of status) |
| `AT_LEAST_ONE_FAILED` | Run if at least one dependency fails |
| `ALL_FAILED` | Run only if all dependencies fail |

#### Complete Example

```yaml
jobs:
  - job_name: "etl_pipeline"
    description: "Daily ETL pipeline for customer data"
    timeout_seconds: 7200
    max_concurrent_runs: 1
    edit_mode: UI_LOCKED
    parameters:
      default_catalog: "production"
    tags:
      department: "engineering"
      project: "customer_analytics"
    schedule:
      quartz_cron_expression: "0 0 2 * * ?"
      timezone_id: "UTC"
      pause_status: UNPAUSED
    job_clusters:
      - job_cluster_key: "etl_cluster"
        new_cluster:
          spark_version: "16.4.x-scala2.12"
          node_type_id: "rd-fleet.xlarge"
          num_workers: 4
    tasks:
      - task_key: "extract_data"
        task_type: "notebook"
        file_path: "/Workspace/etl/extract_customers"
        job_cluster_key: "etl_cluster"
        parameters:
          source_table: "raw.customers"
      
      - task_key: "transform_data"
        task_type: "sql_query"
        depends_on: ["extract_data"]
        warehouse_id: "abc123"
        sql_query: "SELECT * FROM bronze.customers WHERE updated_date = CURRENT_DATE()"
      
      - task_key: "load_data"
        task_type: "notebook"
        depends_on: ["transform_data"]
        file_path: "/Workspace/etl/load_customers"
        run_if: "ALL_SUCCESS"
        notification_settings:
          email_notifications:
            on_failure: ["data-team@example.com"]
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

## Task Types and Parameters

All task types support the common fields listed in the **Task Configuration** section above (in the Metadata Schema section). Below are the task-type-specific parameters:

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
- `project_directory`: Path to dbt project directory. Optional for Git sourced tasks, in which case if no value is provided, the root of the Git repository is used.

**Optional Parameters:**
- `profiles_directory`: Relative path to the profiles directory. Can only be specified if no warehouse_id is specified. If no warehouse_id is specified and this folder is unset, the root directory is used.
- `warehouse_id`: SQL warehouse ID (can be provided via `default_warehouse_id` in orchestrator, which is used as default if not specified)
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