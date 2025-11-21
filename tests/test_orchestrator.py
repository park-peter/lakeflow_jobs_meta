"""Tests for JobOrchestrator class"""

import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.service.jobs import SqlTaskQuery

from lakeflow_jobs_meta.orchestrator import JobOrchestrator
from lakeflow_jobs_meta.metadata_manager import MetadataManager


def _create_mock_f():
    """Create a mock F module that supports comparison operators."""
    mock_f = MagicMock()
    mock_column = MagicMock()
    mock_column.__gt__ = MagicMock(return_value=mock_column)
    mock_column.__lt__ = MagicMock(return_value=mock_column)
    mock_column.__ge__ = MagicMock(return_value=mock_column)
    mock_column.__le__ = MagicMock(return_value=mock_column)
    mock_column.__eq__ = MagicMock(return_value=mock_column)
    mock_f.col.return_value = mock_column
    mock_f.lit.return_value = mock_column
    mock_f.current_timestamp.return_value = mock_column
    return mock_f


@patch("lakeflow_jobs_meta.orchestrator.F", _create_mock_f())
@patch("lakeflow_jobs_meta.orchestrator.WorkspaceClient")
class TestJobOrchestrator:
    """Tests for JobOrchestrator class."""

    def test_init_success(self, mock_workspace_client_class):
        """Test successful initialization."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        orchestrator = JobOrchestrator(control_table="test_table")

        assert orchestrator.control_table == "test_table"
        assert orchestrator.jobs_table == "test_table_jobs"
        assert orchestrator.workspace_client == mock_client
        assert isinstance(orchestrator.metadata_manager, MetadataManager)

    def test_init_with_custom_jobs_table(self, mock_workspace_client_class):
        """Test initialization with custom jobs table name."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        orchestrator = JobOrchestrator(control_table="test_table", jobs_table="custom_jobs_table")

        assert orchestrator.control_table == "test_table"
        assert orchestrator.jobs_table == "custom_jobs_table"

    def test_init_with_default_control_table(self, mock_workspace_client_class):
        """Test initialization with default control table."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        orchestrator = JobOrchestrator()

        assert orchestrator.control_table == "main.default.job_metadata_control_table"
        assert orchestrator.jobs_table == "main.default.job_metadata_control_table_jobs"

    def test_init_invalid_jobs_table(self, mock_workspace_client_class):
        """Test error with invalid jobs_table name."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        with pytest.raises(ValueError):
            JobOrchestrator(control_table="test_table", jobs_table="")

        with pytest.raises(ValueError):
            JobOrchestrator(control_table="test_table", jobs_table="   ")

    def test_init_custom_workspace_client(self, mock_workspace_client_class, mock_workspace_client):
        """Test initialization with custom WorkspaceClient."""
        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)

        assert orchestrator.workspace_client == mock_workspace_client

    def test_init_invalid_control_table(self, mock_workspace_client_class):
        """Test error with invalid control_table name."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        with pytest.raises(ValueError):
            JobOrchestrator(control_table="")

        with pytest.raises(ValueError):
            JobOrchestrator(control_table="   ")  # Only whitespace

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_job_tracking_table_success(self, mock_get_spark, mock_spark_session):
        """Test successful job tracking table creation."""
        mock_get_spark.return_value = mock_spark_session

        orchestrator = JobOrchestrator(control_table="test_catalog.schema.control_table")
        orchestrator._create_job_tracking_table()

        mock_spark_session.sql.assert_called_once()
        call_args = mock_spark_session.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "control_table_jobs" in call_args

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_job_tracking_table_error(self, mock_get_spark, mock_spark_session):
        """Test handling of table creation errors."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.sql.side_effect = Exception("Database error")

        orchestrator = JobOrchestrator(control_table="test_table")

        with pytest.raises(RuntimeError, match="Failed to create job tracking table"):
            orchestrator._create_job_tracking_table()

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_get_stored_job_id_found(self, mock_get_spark, mock_workspace_client_class):
        """Test retrieving existing job ID."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session

        # Create proper mock chain for DataFrame operations
        mock_row = {"job_id": 12345}
        mock_collect = MagicMock(return_value=[mock_row])
        mock_limit = MagicMock()
        mock_limit.collect = mock_collect
        mock_select = MagicMock()
        mock_select.limit.return_value = mock_limit
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_select
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        orchestrator = JobOrchestrator(control_table="test_table")
        result = orchestrator._get_stored_job_id("test_job")

        assert result == 12345

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_get_stored_job_id_not_found(self, mock_get_spark, mock_workspace_client_class):
        """Test when job ID doesn't exist."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session

        # Create proper mock chain returning empty list
        mock_collect = MagicMock(return_value=[])
        mock_limit = MagicMock()
        mock_limit.collect = mock_collect
        mock_select = MagicMock()
        mock_select.limit.return_value = mock_limit
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_select
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        orchestrator = JobOrchestrator(control_table="test_table")
        result = orchestrator._get_stored_job_id("test_job")

        assert result is None

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_get_stored_job_id_error(self, mock_get_spark, mock_workspace_client_class):
        """Test error handling during retrieval."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session

        # Raise exception when table() is called
        mock_spark_session.table.side_effect = Exception("Table not found")

        orchestrator = JobOrchestrator(control_table="test_table")
        result = orchestrator._get_stored_job_id("test_job")

        # Should return None on error
        assert result is None

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator._get_current_user")
    def test_store_job_id_success(self, mock_get_current_user, mock_get_spark, mock_workspace_client_class):
        """Test successful storage of job ID."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark_session.createDataFrame.return_value = mock_df
        mock_spark_session.sql = MagicMock()

        orchestrator = JobOrchestrator(control_table="test_table")
        orchestrator._store_job_id("test_job_rid", "test_job", 12345)

        assert mock_spark_session.createDataFrame.called
        assert mock_spark_session.sql.called

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator._get_current_user")
    def test_store_job_id_error(self, mock_get_current_user, mock_get_spark, mock_workspace_client_class):
        """Test error handling during storage."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark_session.createDataFrame.return_value = mock_df
        mock_spark_session.sql.side_effect = Exception("Storage error")

        orchestrator = JobOrchestrator(control_table="test_table")

        with pytest.raises(RuntimeError, match="Failed to store job_id"):
            orchestrator._store_job_id("test_job_rid", "test_job", 12345)

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator.create_task_from_config")
    def test_generate_tasks_for_job(self, mock_create_task, mock_get_spark, mock_workspace_client_class):
        """Test generating tasks for a job."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session

        task_data = {
            "task_key": "task1",
            "depends_on": None,
            "job_name": "job1",
            "resource_id": "job1",
            "disabled": False,
            "task_type": "notebook",
            "task_config": '{"file_path": "/test"}',
        }
        mock_task = MagicMock()
        mock_task.asDict.return_value = task_data
        mock_task.__getitem__.side_effect = lambda key: task_data.get(key)

        mock_collect = MagicMock(return_value=[mock_task])
        mock_filter = MagicMock()
        mock_filter.collect = mock_collect
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        mock_create_task.return_value = {"task_key": "task1", "task_type": "notebook"}

        orchestrator = JobOrchestrator(control_table="test_table")
        tasks = orchestrator.generate_tasks_for_job("job1")

        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "task1"

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_generate_tasks_no_tasks(self, mock_get_spark, mock_workspace_client_class):
        """Test error when no tasks found."""
        mock_client = MagicMock()
        mock_workspace_client_class.return_value = mock_client

        mock_spark_session = MagicMock()
        mock_get_spark.return_value = mock_spark_session

        mock_collect = MagicMock(return_value=[])
        mock_filter = MagicMock()
        mock_filter.collect = mock_collect
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        orchestrator = JobOrchestrator(control_table="test_table")

        with pytest.raises(ValueError, match="No tasks found"):
            orchestrator.generate_tasks_for_job("job1")

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator.serialize_task_for_api")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.generate_tasks_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator._get_stored_job_id")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator._store_job_id")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.get_job_settings_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.convert_task_config_to_sdk_task")
    def test_create_or_update_job_create_new(
        self,
        mock_convert,
        mock_get_settings,
        mock_store,
        mock_get_job_id,
        mock_generate_tasks,
        mock_serialize,
        mock_get_spark,
        mock_workspace_client,
        mock_spark_session,
    ):
        """Test creating a new job."""
        mock_get_spark.return_value = mock_spark_session

        task_data = {"resource_id": "test_job", "job_name": "Test Job Name", "task_key": "task1"}
        mock_row = MagicMock()
        mock_row.asDict.return_value = task_data

        mock_collect = MagicMock(return_value=[mock_row])
        mock_limit = MagicMock()
        mock_limit.collect = mock_collect
        mock_select = MagicMock()
        mock_select.limit.return_value = mock_limit
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_select
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark_session.table.return_value = mock_df
        mock_spark_session.createDataFrame.return_value = mock_df
        mock_spark_session.sql = MagicMock()

        mock_get_job_id.return_value = None
        mock_get_settings.return_value = {
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "queue": None,
            "continuous": None,
            "trigger": None,
            "schedule": None,
        }
        mock_generate_tasks.return_value = (
            [
                {
                    "task_key": "task1",
                    "task_type": "notebook",
                    "notebook_task": {"notebook_path": "/test/notebook", "base_parameters": {}},
                }
            ],
            {"task_config": "{}"},
        )
        mock_task = MagicMock()
        mock_task.sql_task = None
        mock_convert.return_value = mock_task
        mock_serialize.return_value = {"task_key": "task1", "notebook_task": {}}

        mock_created_job = MagicMock()
        mock_created_job.job_id = 12345
        mock_workspace_client.jobs.create.return_value = mock_created_job

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_job")

        assert job_id == 12345
        mock_workspace_client.jobs.create.assert_called_once()
        mock_store.assert_called_once()
        assert mock_store.call_args[0][0] == "test_job"
        assert mock_store.call_args[0][2] == 12345

    @patch("lakeflow_jobs_meta.orchestrator._get_current_user")
    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator.serialize_task_for_api")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.generate_tasks_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator._get_stored_job_id")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.get_job_settings_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.convert_task_config_to_sdk_task")
    def test_create_or_update_job_update_existing(
        self,
        mock_convert,
        mock_get_settings,
        mock_get_job_id,
        mock_generate_tasks,
        mock_serialize,
        mock_get_spark,
        mock_get_current_user,
        mock_workspace_client,
        mock_spark_session,
    ):
        """Test updating an existing job."""
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"

        mock_df_store = MagicMock()
        mock_df_store.withColumn.return_value = mock_df_store
        mock_spark_session.createDataFrame = MagicMock(return_value=mock_df_store)
        mock_spark_session.sql = MagicMock()

        task_data = {"resource_id": "test_job", "job_name": "Test Job Name", "task_key": "task1"}
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = lambda key: task_data.get(key)

        mock_collect = MagicMock(return_value=[mock_row])
        mock_select = MagicMock()
        mock_select.collect = mock_collect
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_select
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        mock_get_job_id.return_value = 12345
        mock_get_settings.return_value = {
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "queue": None,
            "continuous": None,
            "trigger": None,
            "schedule": None,
        }
        mock_generate_tasks.return_value = (
            [
                {
                    "task_key": "task1",
                    "task_type": "notebook",
                    "notebook_task": {"notebook_path": "/test/notebook", "base_parameters": {}},
                }
            ],
            {"task_config": "{}"},
        )
        mock_task = MagicMock()
        mock_task.sql_task = None  # No SQL task
        mock_convert.return_value = mock_task
        mock_serialize.return_value = {"task_key": "task1", "notebook_task": {}}

        mock_existing_job = MagicMock()
        mock_existing_job.job_id = 12345
        mock_workspace_client.jobs.get.return_value = mock_existing_job

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_job")

        assert job_id == 12345
        mock_workspace_client.jobs.reset.assert_called_once()

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator.serialize_task_for_api")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.generate_tasks_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator._get_stored_job_id")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator._store_job_id")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.get_job_settings_for_job")
    @patch("lakeflow_jobs_meta.orchestrator.convert_task_config_to_sdk_task")
    def test_create_or_update_job_with_inline_sql_query(
        self,
        mock_convert,
        mock_get_settings,
        mock_store,
        mock_get_job_id,
        mock_generate_tasks,
        mock_serialize,
        mock_get_spark,
        mock_workspace_client,
        mock_spark_session,
    ):
        """Test creating a job with inline SQL query (auto-creates query)."""
        mock_get_spark.return_value = mock_spark_session

        task_data = {"resource_id": "test_job", "job_name": "Test Job Name", "task_key": "task1"}
        mock_row = MagicMock()
        mock_row.asDict.return_value = task_data

        mock_collect = MagicMock(return_value=[mock_row])
        mock_limit = MagicMock()
        mock_limit.collect = mock_collect
        mock_select = MagicMock()
        mock_select.limit.return_value = mock_limit
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_filter
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark_session.table.return_value = mock_df
        mock_spark_session.createDataFrame.return_value = mock_df
        mock_spark_session.sql = MagicMock()

        mock_get_job_id.return_value = None
        mock_get_settings.return_value = {
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "queue": None,
            "continuous": None,
            "trigger": None,
            "schedule": None,
        }
        mock_generate_tasks.return_value = (
            [
                {
                    "task_key": "sql_task1",
                    "task_type": "sql_query",
                    "sql_task": {"warehouse_id": "warehouse123", "query": {"query": "SELECT 1"}},
                }
            ],
            {"task_config": '{"warehouse_id": "warehouse123", "sql_query": "SELECT 1"}'},
        )
        mock_task = MagicMock()
        mock_task.task_key = "sql_task1"
        mock_sql_task = MagicMock()
        mock_sql_task.warehouse_id = "warehouse123"
        mock_sql_task.query = {"query": "SELECT 1"}
        mock_sql_task.file = None
        mock_task.sql_task = mock_sql_task
        mock_convert.return_value = mock_task

        mock_created_query = MagicMock()
        mock_created_query.id = "query_abc123"
        mock_workspace_client.queries.create.return_value = mock_created_query

        mock_serialize.return_value = {"task_key": "sql_task1", "sql_task": {"query": {"query_id": "query_abc123"}}}

        mock_created_job = MagicMock()
        mock_created_job.job_id = 12345
        mock_workspace_client.jobs.create.return_value = mock_created_job

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_job")

        assert job_id == 12345
        mock_workspace_client.queries.create.assert_called_once()
        assert isinstance(mock_task.sql_task.query, SqlTaskQuery)
        assert mock_task.sql_task.query.query_id == "query_abc123"
        mock_workspace_client.jobs.create.assert_called_once()
        # Verify _store_job_id was called (exact args check is complex due to mocking)
        mock_store.assert_called_once()
        assert mock_store.call_args[0][0] == "test_job"  # resource_id
        assert mock_store.call_args[0][2] == 12345  # job_id

    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.generate_tasks_for_job")
    def test_create_or_update_job_no_tasks_error(
        self, mock_generate_tasks, mock_get_spark, mock_workspace_client, mock_spark_session
    ):
        """Test error when no tasks are found."""
        mock_get_spark.return_value = mock_spark_session

        task_data = {"resource_id": "test_job", "job_name": "Test Job Name", "task_key": "task1"}
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = lambda key: task_data.get(key)

        mock_collect = MagicMock(return_value=[mock_row])
        mock_select = MagicMock()
        mock_select.collect = mock_collect
        mock_filter = MagicMock()
        mock_filter.select.return_value = mock_select
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_filter
        mock_spark_session.table.return_value = mock_df

        mock_generate_tasks.return_value = ([], None)

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)

        with pytest.raises(ValueError, match="No tasks found|No task"):
            orchestrator.create_or_update_job("test_job")

    def test_create_or_update_job_invalid_inputs(self, mock_workspace_client):
        """Test error handling for invalid inputs."""
        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)

        with pytest.raises(ValueError):
            orchestrator.create_or_update_job("")

    @patch("lakeflow_jobs_meta.orchestrator.MetadataManager")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.ensure_setup")
    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_or_update_jobs_success(
        self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client
    ):
        """Test successful orchestration."""
        mock_get_spark.return_value = mock_spark_session

        mock_manager = MagicMock()
        mock_manager.get_all_jobs.return_value = ["job1"]
        mock_metadata_manager.return_value = mock_manager

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        orchestrator.create_or_update_job = MagicMock(return_value=12345)

        jobs = orchestrator.create_or_update_jobs(default_pause_status=False)

        assert len(jobs) == 1
        assert jobs[0]["resource_id"] == "job1"
        assert jobs[0]["job_id"] == 12345

    @patch("lakeflow_jobs_meta.orchestrator.MetadataManager")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.ensure_setup")
    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_or_update_jobs_no_jobs(
        self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client
    ):
        """Test handling when no jobs exist."""
        mock_get_spark.return_value = mock_spark_session

        mock_manager = MagicMock()
        mock_manager.get_all_jobs.return_value = []
        mock_metadata_manager.return_value = mock_manager

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        jobs = orchestrator.create_or_update_jobs(default_pause_status=False)

        assert len(jobs) == 0

    @patch("lakeflow_jobs_meta.orchestrator.MetadataManager")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.ensure_setup")
    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_or_update_jobs_partial_failure(
        self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client
    ):
        """Test handling when some jobs fail."""
        mock_get_spark.return_value = mock_spark_session

        mock_manager = MagicMock()
        mock_manager.get_all_jobs.return_value = ["job1", "job2"]
        mock_metadata_manager.return_value = mock_manager

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        orchestrator.create_or_update_job = MagicMock(side_effect=[12345, Exception("Job creation failed")])

        jobs = orchestrator.create_or_update_jobs(default_pause_status=False)

        assert len(jobs) == 1
        assert jobs[0]["resource_id"] == "job1"

    @patch("lakeflow_jobs_meta.orchestrator.MetadataManager")
    @patch("lakeflow_jobs_meta.orchestrator.JobOrchestrator.ensure_setup")
    @patch("lakeflow_jobs_meta.orchestrator._get_spark")
    def test_create_or_update_jobs_with_yaml(
        self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client
    ):
        """Test orchestration with YAML file loading."""
        mock_get_spark.return_value = mock_spark_session

        mock_manager = MagicMock()
        mock_manager.load_yaml.return_value = (5, ["job1"])  # Returns tuple: (num_tasks, resource_ids)
        mock_metadata_manager.return_value = mock_manager

        orchestrator = JobOrchestrator(control_table="test_table", workspace_client=mock_workspace_client)
        orchestrator.create_or_update_job = MagicMock(return_value=12345)
        jobs = orchestrator.create_or_update_jobs(yaml_path="./test.yaml", default_pause_status=False)

        mock_manager.load_yaml.assert_called_once_with("./test.yaml", var=None)
        assert len(jobs) == 1
        assert jobs[0]["resource_id"] == "job1"
