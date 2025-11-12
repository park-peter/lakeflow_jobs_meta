"""Tests for task builder functions"""

import pytest
import json
from unittest.mock import patch
from lakeflow_jobs_meta.task_builders import (
    create_task_from_config,
    create_notebook_task_config,
    create_sql_query_task_config,
    create_sql_file_task_config,
    create_python_wheel_task_config,
    create_spark_jar_task_config,
    create_pipeline_task_config,
    create_dbt_task_config,
    convert_task_config_to_sdk_task,
)
from lakeflow_jobs_meta.constants import (
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
    TASK_TYPE_PYTHON_WHEEL,
    TASK_TYPE_SPARK_JAR,
    TASK_TYPE_PIPELINE,
    TASK_TYPE_DBT,
)


class TestCreateTaskFromConfig:
    """Tests for create_task_from_config function."""

    def test_notebook_task_creation(self, sample_task_data):
        """Test creation of notebook task."""
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["task_key"] == "test_task_1"
        assert task_config["task_type"] == TASK_TYPE_NOTEBOOK
        assert "notebook_task" in task_config
        assert task_config["notebook_task"]["notebook_path"] == "/Workspace/test/notebook"
        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["catalog"] == "bronze"

    def test_sql_query_task_creation(self, sample_task_data):
        """Test creation of SQL query task."""
        sample_task_data["task_type"] = "sql_query"
        sample_task_data["task_config"] = json.dumps({"warehouse_id": "abc123", "sql_query": "SELECT * FROM table"})
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_SQL_QUERY
        assert "sql_task" in task_config
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["query"]["query"] == "SELECT * FROM table"

    def test_sql_file_task_creation(self, sample_task_data):
        """Test creation of SQL file task."""
        sample_task_data["task_type"] = "sql_file"
        sample_task_data["task_config"] = json.dumps({"warehouse_id": "abc123", "file_path": "/Workspace/test/query.sql"})
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_SQL_FILE
        assert "sql_task" in task_config
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["file"]["path"] == "/Workspace/test/query.sql"
        assert task_config["sql_task"]["file"]["source"] == "WORKSPACE"

    def test_task_with_dependencies(self, sample_task_data):
        """Test task creation with dependencies."""
        control_table = "main.examples.etl_control"
        depends_on_tasks = ["task1", "task2"]
        task_config = create_task_from_config(sample_task_data, control_table, depends_on_task_keys=depends_on_tasks)

        assert "depends_on" in task_config
        assert len(task_config["depends_on"]) == 2
        assert task_config["depends_on"][0]["task_key"] == "task1"

    def test_invalid_json_task_config(self, sample_task_data):
        """Test handling of invalid JSON in task_config."""
        sample_task_data["task_config"] = "invalid json"
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="Invalid task_config JSON"):
            create_task_from_config(sample_task_data, control_table)

    def test_missing_task_type(self, sample_task_data):
        """Test error when task_type is missing."""
        del sample_task_data["task_type"]
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="must have 'task_type' field"):
            create_task_from_config(sample_task_data, control_table)

    def test_unsupported_task_type(self, sample_task_data):
        """Test handling of unsupported task type."""
        sample_task_data["task_type"] = "unsupported_type"
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="Unsupported task_type"):
            create_task_from_config(sample_task_data, control_table)


class TestCreateNotebookTaskConfig:
    """Tests for create_notebook_task_config function."""

    def test_valid_notebook_task(self):
        """Test creation of valid notebook task."""
        task_config_dict = {
            "file_path": "/Workspace/test/notebook",
            "parameters": {"catalog": "bronze", "schema": "raw_data"}
        }
        task_config = create_notebook_task_config("test_task_key", task_config_dict)

        assert task_config["task_key"] == "test_task_key"
        assert task_config["notebook_task"]["notebook_path"] == "/Workspace/test/notebook"
        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["catalog"] == "bronze"
        assert base_params["schema"] == "raw_data"

    def test_missing_file_path(self):
        """Test error when file_path is missing."""
        task_config_dict = {}

        with pytest.raises(ValueError, match="Missing file_path"):
            create_notebook_task_config("test_task_key", task_config_dict)


class TestCreateSqlQueryTaskConfig:
    """Tests for create_sql_query_task_config function."""

    def test_valid_sql_query_task(self):
        """Test creation of valid SQL query task."""
        task_config_dict = {
            "warehouse_id": "abc123",
            "sql_query": "SELECT * FROM table",
            "parameters": {"param1": "value1"}
        }

        task_config = create_sql_query_task_config("test_task_key", task_config_dict)

        assert task_config["task_key"] == "test_task_key"
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["query"]["query"] == "SELECT * FROM table"
        assert task_config["sql_task"]["parameters"]["param1"] == "value1"

    def test_sql_query_with_query_id(self):
        """Test SQL query task with query_id."""
        task_config_dict = {
            "warehouse_id": "abc123",
            "query_id": "query_abc123",
            "parameters": {"param1": "value1"}
        }

        task_config = create_sql_query_task_config("test_task_key", task_config_dict)

        assert task_config["sql_task"]["query"]["query_id"] == "query_abc123"

    def test_missing_warehouse_id(self):
        """Test error when warehouse_id is missing."""
        task_config_dict = {"sql_query": "SELECT * FROM table"}

        with pytest.raises(ValueError, match="Missing warehouse_id"):
            create_sql_query_task_config("test_task_key", task_config_dict)

    def test_missing_sql_query_and_query_id(self):
        """Test error when both sql_query and query_id are missing."""
        task_config_dict = {"warehouse_id": "abc123"}

        with pytest.raises(ValueError, match="Must provide either sql_query or query_id"):
            create_sql_query_task_config("test_task_key", task_config_dict)


class TestCreateSqlFileTaskConfig:
    """Tests for create_sql_file_task_config function."""

    def test_valid_sql_file_task(self):
        """Test creation of valid SQL file task."""
        task_config_dict = {
            "warehouse_id": "abc123",
            "file_path": "/Workspace/test/query.sql",
            "parameters": {"catalog": "bronze", "schema": "raw_data"}
        }

        task_config = create_sql_file_task_config("test_task_key", task_config_dict)

            assert task_config["task_key"] == "test_task_key"
        assert task_config["task_type"] == TASK_TYPE_SQL_FILE
            assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["file"]["path"] == "/Workspace/test/query.sql"
        assert task_config["sql_task"]["file"]["source"] == "WORKSPACE"
        assert task_config["sql_task"]["parameters"]["catalog"] == "bronze"

    def test_missing_file_path(self):
        """Test error when file_path is missing."""
        task_config_dict = {"warehouse_id": "abc123"}

        with pytest.raises(ValueError, match="Missing file_path"):
            create_sql_file_task_config("test_task_key", task_config_dict)


class TestConvertTaskConfigToSdkTask:
    """Tests for convert_task_config_to_sdk_task function."""

    def test_notebook_task_conversion(self):
        """Test conversion of notebook task config to SDK task."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {"param1": "value1"}},
            "existing_cluster_id": "cluster123",
            "disabled": False,
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "test_task"
        assert sdk_task.notebook_task.notebook_path == "/Workspace/test/notebook"
        assert sdk_task.existing_cluster_id == "cluster123"
        assert sdk_task.disabled is False

    def test_sql_query_task_conversion(self):
        """Test conversion of SQL query task config to SDK task."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_SQL_QUERY,
            "sql_task": {"warehouse_id": "abc123", "query": {"query": "SELECT * FROM table"}, "parameters": {}},
            "disabled": True,
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "test_task"
        assert sdk_task.sql_task.warehouse_id == "abc123"
        assert isinstance(sdk_task.sql_task.query, dict)
        assert sdk_task.sql_task.query["query"] == "SELECT * FROM table"
        assert sdk_task.disabled is True

    def test_sql_file_task_conversion(self):
        """Test conversion of SQL file task config to SDK task."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_SQL_FILE,
            "sql_task": {
                "warehouse_id": "abc123",
                "file": {"path": "/Workspace/test/query.sql", "source": "WORKSPACE"},
                "parameters": {},
            },
            "disabled": False,
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "test_task"
        assert sdk_task.sql_task.warehouse_id == "abc123"
        assert sdk_task.sql_task.file.path == "/Workspace/test/query.sql"
        assert sdk_task.disabled is False

    def test_task_with_dependencies(self):
        """Test task conversion with dependencies."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {}},
            "depends_on": [{"task_key": "task1"}, {"task_key": "task2"}],
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert len(sdk_task.depends_on) == 2
        assert sdk_task.depends_on[0].task_key == "task1"

    def test_task_with_timeout(self):
        """Test task conversion with timeout_seconds."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {}},
            "timeout_seconds": 1800,
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.timeout_seconds == 1800

    def test_sql_query_with_query_id_conversion(self):
        """Test conversion of SQL query task with query_id."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_SQL_QUERY,
            "sql_task": {"warehouse_id": "abc123", "query": {"query_id": "query_abc123"}, "parameters": {}},
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "test_task"
        assert sdk_task.sql_task.warehouse_id == "abc123"
        assert isinstance(sdk_task.sql_task.query, dict)
        assert sdk_task.sql_task.query["query_id"] == "query_abc123"

    def test_sql_file_with_git_source(self):
        """Test SQL file task with GIT source."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_SQL_FILE,
            "sql_task": {
                "warehouse_id": "abc123",
                "file": {"path": "/Workspace/test/query.sql", "source": "GIT"},
                "parameters": {},
            },
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.sql_task.file.source.value == "GIT"


class TestNotebookTaskType:
    """Comprehensive tests for Notebook task type."""

    def test_notebook_task_with_all_parameters(self):
        """Test notebook task with all possible parameters."""
        task_config_dict = {
            "file_path": "/Workspace/users/test/notebook",
            "parameters": {
                "catalog": "bronze",
                "schema": "raw_data",
                "source_table": "customers",
                "target_table": "customers_clean",
                "write_mode": "append",
            }
        }
        task_config = create_notebook_task_config("my_notebook_task", task_config_dict)

        assert task_config["task_key"] == "my_notebook_task"
        assert task_config["task_type"] == TASK_TYPE_NOTEBOOK
        assert task_config["notebook_task"]["notebook_path"] == "/Workspace/users/test/notebook"
        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["catalog"] == "bronze"
        assert base_params["schema"] == "raw_data"
        assert base_params["source_table"] == "customers"
        assert base_params["target_table"] == "customers_clean"
        assert base_params["write_mode"] == "append"

    def test_notebook_task_with_empty_parameters(self):
        """Test notebook task with empty parameters dict."""
        task_config_dict = {"file_path": "/Workspace/test/notebook"}

        task_config = create_notebook_task_config("test_task", task_config_dict)

        base_params = task_config["notebook_task"]["base_parameters"]
        assert len(base_params) == 0  # Empty parameters

    def test_notebook_task_with_timeout(self, sample_task_data):
        """Test notebook task creation with timeout_seconds."""
        sample_task_data["task_config"] = json.dumps({"file_path": "/Workspace/test/notebook", "timeout_seconds": 1800})
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["timeout_seconds"] == 1800

    def test_notebook_task_disabled(self, sample_task_data):
        """Test notebook task with disabled=True."""
        sample_task_data["disabled"] = True
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["disabled"] is True

    def test_notebook_task_with_dynamic_value_references(self):
        """Test notebook task with Databricks dynamic value references in parameters."""
        task_config_dict = {
            "file_path": "/Workspace/test/notebook",
            "parameters": {
                "job_id": "{{job.id}}",
                "task_name": "{{task.name}}",
                "start_time": "{{job.start_time.iso_date}}",
            }
        }

        task_config = create_notebook_task_config("test_task", task_config_dict)

        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["job_id"] == "{{job.id}}"
        assert base_params["task_name"] == "{{task.name}}"
        assert base_params["start_time"] == "{{job.start_time.iso_date}}"

    def test_notebook_task_with_custom_path(self):
        """Test notebook task with custom path (not standard prefix)."""
        task_config_dict = {"file_path": "/custom/path/notebook"}

        task_config = create_notebook_task_config("test_task", task_config_dict)
        assert task_config["notebook_task"]["notebook_path"] == "/custom/path/notebook"


class TestSqlQueryTaskType:
    """Comprehensive tests for SQL Query task type."""

    def test_sql_query_task_with_inline_sql(self):
        """Test SQL query task with inline SQL."""
        task_config_dict = {
            "warehouse_id": "warehouse_abc123",
            "sql_query": "SELECT * FROM :catalog.:schema.customers WHERE date > :start_date",
            "parameters": {
                "catalog": "bronze",
                "schema": "raw_data",
                "start_date": "2024-01-01",
            }
        }

        task_config = create_sql_query_task_config("sql_query_task", task_config_dict)

        assert task_config["task_key"] == "sql_query_task"
        assert task_config["task_type"] == TASK_TYPE_SQL_QUERY
        assert task_config["sql_task"]["warehouse_id"] == "warehouse_abc123"
        assert "SELECT * FROM" in task_config["sql_task"]["query"]["query"]
        assert task_config["sql_task"]["parameters"]["catalog"] == "bronze"
        assert task_config["sql_task"]["parameters"]["schema"] == "raw_data"
        assert task_config["sql_task"]["parameters"]["start_date"] == "2024-01-01"

    def test_sql_query_task_with_saved_query(self):
        """Test SQL query task with saved query ID."""
        task_config_dict = {
            "warehouse_id": "warehouse_abc123",
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
        }
        parameters = {"threshold": "5.0", "status": "active"}

        task_config = create_sql_query_task_config("saved_query_task", task_config_dict, parameters)

        assert task_config["sql_task"]["query"]["query_id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert task_config["sql_task"]["parameters"]["threshold"] == "5.0"
        assert task_config["sql_task"]["parameters"]["status"] == "active"

    def test_sql_query_task_with_default_warehouse_id(self):
        """Test SQL query task using default_warehouse_id."""
        task_config_dict = {"sql_query": "SELECT 1"}
        parameters = {}
        default_warehouse_id = "default_warehouse_123"

        task_config = create_sql_query_task_config("test_task", task_config_dict, parameters, default_warehouse_id)

        assert task_config["sql_task"]["warehouse_id"] == "default_warehouse_123"

    def test_sql_query_task_task_config_warehouse_overrides_default(self):
        """Test that warehouse_id in task_config overrides default_warehouse_id."""
        task_config_dict = {"warehouse_id": "config_warehouse", "sql_query": "SELECT 1"}
        parameters = {}
        default_warehouse_id = "default_warehouse_123"

        task_config = create_sql_query_task_config("test_task", task_config_dict, parameters, default_warehouse_id)

        assert task_config["sql_task"]["warehouse_id"] == "config_warehouse"

    def test_sql_query_task_with_dynamic_value_references(self):
        """Test SQL query task with Databricks dynamic value references."""
        task_config_dict = {"warehouse_id": "warehouse_123", "sql_query": "SELECT * FROM table"}
        parameters = {
            "job_id": "{{job.id}}",
            "task_name": "{{task.name}}",
            "start_time": "{{job.start_time.iso_date}}",
            "run_id": "{{job.run_id}}",
        }

        task_config = create_sql_query_task_config("test_task", task_config_dict, parameters)

        sql_params = task_config["sql_task"]["parameters"]
        assert sql_params["job_id"] == "{{job.id}}"
        assert sql_params["task_name"] == "{{task.name}}"
        assert sql_params["start_time"] == "{{job.start_time.iso_date}}"
        assert sql_params["run_id"] == "{{job.run_id}}"

    def test_sql_query_task_parameters_converted_to_strings(self):
        """Test that SQL task parameters are converted to strings."""
        task_config_dict = {"warehouse_id": "warehouse_123", "sql_query": "SELECT * FROM table"}
        parameters = {
            "int_param": 42,
            "float_param": 3.14,
            "bool_param": True,
            "none_param": None,
        }

        task_config = create_sql_query_task_config("test_task", task_config_dict, parameters)

        sql_params = task_config["sql_task"]["parameters"]
        assert sql_params["int_param"] == "42"
        assert sql_params["float_param"] == "3.14"
        assert sql_params["bool_param"] == "True"
        assert sql_params["none_param"] == "None"

    def test_sql_query_task_missing_warehouse_and_default(self):
        """Test error when warehouse_id is missing and no default provided."""
        task_config_dict = {"sql_query": "SELECT * FROM table"}
        parameters = {}

        with pytest.raises(ValueError, match="Missing warehouse_id"):
            create_sql_query_task_config("test_task", task_config_dict, parameters)

    def test_sql_query_task_both_sql_query_and_query_id(self):
        """Test that when both sql_query and query_id are provided, query_id takes precedence."""
        task_config_dict = {
            "warehouse_id": "warehouse_123",
            "sql_query": "SELECT * FROM table",
            "query_id": "query_abc123",
        }
        parameters = {}

        task_config = create_sql_query_task_config("test_task", task_config_dict, parameters)

        # query_id should be used
        assert "query_id" in task_config["sql_task"]["query"]
        assert task_config["sql_task"]["query"]["query_id"] == "query_abc123"
        assert "query" not in task_config["sql_task"]["query"]


class TestSqlFileTaskType:
    """Comprehensive tests for SQL File task type."""

    def test_sql_file_task_workspace_source(self):
        """Test SQL file task with WORKSPACE source."""
        task_config_dict = {
            "warehouse_id": "warehouse_abc123",
            "file_path": "/Workspace/users/test/query.sql",
            "file_source": "WORKSPACE",
        }
        parameters = {"catalog": "bronze", "schema": "raw_data", "table": "customers"}

        task_config = create_sql_file_task_config("sql_file_task", task_config_dict, parameters)

        assert task_config["task_key"] == "sql_file_task"
        assert task_config["task_type"] == TASK_TYPE_SQL_FILE
        assert task_config["sql_task"]["warehouse_id"] == "warehouse_abc123"
        assert task_config["sql_task"]["file"]["path"] == "/Workspace/users/test/query.sql"
        assert task_config["sql_task"]["file"]["source"] == "WORKSPACE"
        assert task_config["sql_task"]["parameters"]["catalog"] == "bronze"

    def test_sql_file_task_git_source(self):
        """Test SQL file task with GIT source."""
        task_config_dict = {
            "warehouse_id": "warehouse_abc123",
            "file_path": "/Repos/user/repo/sql/query.sql",
            "file_source": "GIT",
        }
        parameters = {"catalog": "silver", "schema": "processed"}

        task_config = create_sql_file_task_config("git_sql_task", task_config_dict, parameters)

        assert task_config["sql_task"]["file"]["source"] == "GIT"
        assert task_config["sql_task"]["file"]["path"] == "/Repos/user/repo/sql/query.sql"

    def test_sql_file_task_default_source(self):
        """Test SQL file task defaults to WORKSPACE source when not specified."""
        task_config_dict = {"warehouse_id": "warehouse_abc123", "file_path": "/Workspace/test/query.sql"}
        parameters = {}

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters)

        assert task_config["sql_task"]["file"]["source"] == "WORKSPACE"

    def test_sql_file_task_with_default_warehouse_id(self):
        """Test SQL file task using default_warehouse_id."""
        task_config_dict = {"file_path": "/Workspace/test/query.sql"}
        parameters = {}
        default_warehouse_id = "default_warehouse_123"

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters, default_warehouse_id)

        assert task_config["sql_task"]["warehouse_id"] == "default_warehouse_123"

    def test_sql_file_task_task_config_warehouse_overrides_default(self):
        """Test that warehouse_id in task_config overrides default_warehouse_id."""
        task_config_dict = {"warehouse_id": "config_warehouse", "file_path": "/Workspace/test/query.sql"}
        parameters = {}
        default_warehouse_id = "default_warehouse_123"

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters, default_warehouse_id)

        assert task_config["sql_task"]["warehouse_id"] == "config_warehouse"

    def test_sql_file_task_with_parameters(self):
        """Test SQL file task with multiple parameters."""
        task_config_dict = {"warehouse_id": "warehouse_123", "file_path": "/Workspace/test/query.sql"}
        parameters = {
            "catalog": "bronze",
            "schema": "raw_data",
            "table": "customers",
            "max_hours": "24",
            "threshold": "5.0",
        }

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters)

        sql_params = task_config["sql_task"]["parameters"]
        assert len(sql_params) == 5
        assert sql_params["catalog"] == "bronze"
        assert sql_params["schema"] == "raw_data"
        assert sql_params["table"] == "customers"
        assert sql_params["max_hours"] == "24"
        assert sql_params["threshold"] == "5.0"

    def test_sql_file_task_with_dynamic_value_references(self):
        """Test SQL file task with Databricks dynamic value references."""
        task_config_dict = {"warehouse_id": "warehouse_123", "file_path": "/Workspace/test/query.sql"}
        parameters = {
            "job_id": "{{job.id}}",
            "task_name": "{{task.name}}",
            "start_time": "{{job.start_time.iso_date}}",
        }

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters)

        sql_params = task_config["sql_task"]["parameters"]
        assert sql_params["job_id"] == "{{job.id}}"
        assert sql_params["task_name"] == "{{task.name}}"
        assert sql_params["start_time"] == "{{job.start_time.iso_date}}"

    def test_sql_file_task_parameters_converted_to_strings(self):
        """Test that SQL file task parameters are converted to strings."""
        task_config_dict = {"warehouse_id": "warehouse_123", "file_path": "/Workspace/test/query.sql"}
        parameters = {"int_val": 100, "float_val": 2.5, "bool_val": False}

        task_config = create_sql_file_task_config("test_task", task_config_dict, parameters)

        sql_params = task_config["sql_task"]["parameters"]
        assert sql_params["int_val"] == "100"
        assert sql_params["float_val"] == "2.5"
        assert sql_params["bool_val"] == "False"

    def test_sql_file_task_missing_warehouse_and_default(self):
        """Test error when warehouse_id is missing and no default provided."""
        task_config_dict = {"file_path": "/Workspace/test/query.sql"}
        parameters = {}

        with pytest.raises(ValueError, match="Missing warehouse_id"):
            create_sql_file_task_config("test_task", task_config_dict, parameters)

    def test_sql_file_task_with_timeout(self, sample_task_data):
        """Test SQL file task creation with timeout_seconds."""
        sample_task_data["task_type"] = "sql_file"
        sample_task_data["task_config"] = json.dumps(
            {"warehouse_id": "abc123", "file_path": "/Workspace/test/query.sql", "timeout_seconds": 2400}
        )
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["timeout_seconds"] == 2400

    def test_sql_file_task_disabled(self, sample_task_data):
        """Test SQL file task with disabled=True."""
        sample_task_data["task_type"] = "sql_file"
        sample_task_data["task_config"] = json.dumps({"warehouse_id": "abc123", "file_path": "/Workspace/test/query.sql"})
        sample_task_data["disabled"] = True
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["disabled"] is True


class TestTaskTypeIntegration:
    """Integration tests for all task types with full workflow."""

    def test_notebook_task_full_workflow(self):
        """Test complete notebook task workflow from config to SDK task."""
        task_data = {
            "job_name": "test_job",
            "task_key": "notebook_task_1",
            "depends_on": "[]",
            "task_type": "notebook",
            "parameters": '{"catalog": "bronze", "schema": "raw_data"}',
            "task_config": '{"file_path": "/Workspace/test/notebook"}',
            "disabled": False,
        }

        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)
        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "notebook_task_1"
        assert sdk_task.notebook_task.notebook_path == "/Workspace/test/notebook"
        assert sdk_task.notebook_task.base_parameters["catalog"] == "bronze"
        assert sdk_task.disabled is False

    def test_sql_query_task_full_workflow(self):
        """Test complete SQL query task workflow from config to SDK task."""
        task_data = {
            "job_name": "test_job",
            "task_key": "sql_query_task_1",
            "depends_on": "[]",
            "task_type": "sql_query",
            "parameters": '{"catalog": "bronze", "start_date": "2024-01-01"}',
            "task_config": '{"warehouse_id": "warehouse_123", "sql_query": "SELECT * FROM :catalog.customers"}',
            "disabled": False,
        }

        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)
        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "sql_query_task_1"
        assert sdk_task.sql_task.warehouse_id == "warehouse_123"
        assert "SELECT * FROM" in sdk_task.sql_task.query["query"]
        assert sdk_task.sql_task.parameters["catalog"] == "bronze"
        assert sdk_task.disabled is False

    def test_sql_file_task_full_workflow(self):
        """Test complete SQL file task workflow from config to SDK task."""
        task_data = {
            "job_name": "test_job",
            "task_key": "sql_file_task_1",
            "depends_on": "[]",
            "task_type": "sql_file",
            "parameters": '{"catalog": "bronze", "schema": "raw_data"}',
            "task_config": '{"warehouse_id": "warehouse_123", "file_path": "/Workspace/test/query.sql"}',
            "disabled": False,
        }

        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)
        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "sql_file_task_1"
        assert sdk_task.sql_task.warehouse_id == "warehouse_123"
        assert sdk_task.sql_task.file.path == "/Workspace/test/query.sql"
        assert sdk_task.sql_task.file.source.value == "WORKSPACE"
        assert sdk_task.sql_task.parameters["catalog"] == "bronze"
        assert sdk_task.disabled is False

    def test_all_task_types_with_dependencies(self):
        """Test all task types with dependencies."""
        previous_tasks = ["task_a", "task_b"]

        # Notebook task with dependencies
        notebook_config = {
            "task_key": "notebook_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {}},
            "depends_on": [{"task_key": t} for t in previous_tasks],
        }
        notebook_sdk = convert_task_config_to_sdk_task(notebook_config)
        assert len(notebook_sdk.depends_on) == 2

        # SQL Query task with dependencies
        sql_query_config = {
            "task_key": "sql_query_task",
            "task_type": TASK_TYPE_SQL_QUERY,
            "sql_task": {"warehouse_id": "warehouse_123", "query": {"query": "SELECT 1"}, "parameters": {}},
            "depends_on": [{"task_key": t} for t in previous_tasks],
        }
        sql_query_sdk = convert_task_config_to_sdk_task(sql_query_config)
        assert len(sql_query_sdk.depends_on) == 2

        # SQL File task with dependencies
        sql_file_config = {
            "task_key": "sql_file_task",
            "task_type": TASK_TYPE_SQL_FILE,
            "sql_task": {
                "warehouse_id": "warehouse_123",
                "file": {"path": "/Workspace/test/query.sql", "source": "WORKSPACE"},
                "parameters": {},
            },
            "depends_on": [{"task_key": t} for t in previous_tasks],
        }
        sql_file_sdk = convert_task_config_to_sdk_task(sql_file_config)
        assert len(sql_file_sdk.depends_on) == 2

    def test_all_task_types_with_timeout_and_disabled(self):
        """Test all task types with timeout_seconds and disabled flags."""
        timeout = 1800
        disabled = True

        # Notebook
        notebook_config = {
            "task_key": "notebook_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {}},
            "timeout_seconds": timeout,
            "disabled": disabled,
        }
        notebook_sdk = convert_task_config_to_sdk_task(notebook_config)
        assert notebook_sdk.timeout_seconds == timeout
        assert notebook_sdk.disabled is True

        # SQL Query
        sql_query_config = {
            "task_key": "sql_query_task",
            "task_type": TASK_TYPE_SQL_QUERY,
            "sql_task": {"warehouse_id": "warehouse_123", "query": {"query": "SELECT 1"}, "parameters": {}},
            "timeout_seconds": timeout,
            "disabled": disabled,
        }
        sql_query_sdk = convert_task_config_to_sdk_task(sql_query_config)
        assert sql_query_sdk.timeout_seconds == timeout
        assert sql_query_sdk.disabled is True

        # SQL File
        sql_file_config = {
            "task_key": "sql_file_task",
            "task_type": TASK_TYPE_SQL_FILE,
            "sql_task": {
                "warehouse_id": "warehouse_123",
                "file": {"path": "/Workspace/test/query.sql", "source": "WORKSPACE"},
                "parameters": {},
            },
            "timeout_seconds": timeout,
            "disabled": disabled,
        }
        sql_file_sdk = convert_task_config_to_sdk_task(sql_file_config)
        assert sql_file_sdk.timeout_seconds == timeout
        assert sql_file_sdk.disabled is True


class TestPythonWheelTaskType:
    """Tests for Python wheel task type."""

    def test_python_wheel_task_creation(self):
        """Test creation of Python wheel task."""
        task_data = {
            "task_key": "python_wheel_task_1",
            "task_type": "python_wheel",
            "task_config": json.dumps({
                "package_name": "my_package",
                "entry_point": "main",
                "parameters": ["arg1", "arg2"],
            }),
            "parameters": "{}",
            "disabled": False,
        }
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_PYTHON_WHEEL
        assert "python_wheel_task" in task_config
        assert task_config["python_wheel_task"]["package_name"] == "my_package"
        assert task_config["python_wheel_task"]["entry_point"] == "main"
        assert task_config["python_wheel_task"]["parameters"] == ["arg1", "arg2"]

    def test_python_wheel_task_missing_package_name(self):
        """Test Python wheel task with missing package_name."""
        with pytest.raises(ValueError, match="Missing package_name"):
            create_python_wheel_task_config(
                "test_task",
                {"entry_point": "main"}
            )

    def test_python_wheel_task_missing_entry_point(self):
        """Test Python wheel task with missing entry_point."""
        with pytest.raises(ValueError, match="Missing entry_point"):
            create_python_wheel_task_config(
                "test_task",
                {"package_name": "my_package"}
            )


class TestSparkJarTaskType:
    """Tests for Spark JAR task type."""

    def test_spark_jar_task_creation(self):
        """Test creation of Spark JAR task."""
        task_data = {
            "task_key": "spark_jar_task_1",
            "task_type": "spark_jar",
            "task_config": json.dumps({
                "main_class_name": "com.example.MainClass",
                "parameters": ["param1", "param2"],
            }),
            "parameters": "{}",
            "disabled": False,
        }
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_SPARK_JAR
        assert "spark_jar_task" in task_config
        assert task_config["spark_jar_task"]["main_class_name"] == "com.example.MainClass"
        assert task_config["spark_jar_task"]["parameters"] == ["param1", "param2"]

    def test_spark_jar_task_missing_main_class_name(self):
        """Test Spark JAR task with missing main_class_name."""
        with pytest.raises(ValueError, match="Missing main_class_name"):
            create_spark_jar_task_config(
                "test_task",
                {"parameters": []}
            )


class TestPipelineTaskType:
    """Tests for Pipeline task type."""

    def test_pipeline_task_creation(self):
        """Test creation of Pipeline task."""
        task_data = {
            "task_key": "pipeline_task_1",
            "task_type": "pipeline",
            "task_config": json.dumps({
                "pipeline_id": "1165597e-f650-4bf3-9a4f-fc2f2d40d2c3",
            }),
            "parameters": "{}",
            "disabled": False,
        }
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_PIPELINE
        assert "pipeline_task" in task_config
        assert task_config["pipeline_task"]["pipeline_id"] == "1165597e-f650-4bf3-9a4f-fc2f2d40d2c3"

    def test_pipeline_task_missing_pipeline_id(self):
        """Test Pipeline task with missing pipeline_id."""
        with pytest.raises(ValueError, match="Missing pipeline_id"):
            create_pipeline_task_config(
                "test_task",
                {}
            )


class TestDbtTaskType:
    """Tests for dbt task type."""

    def test_dbt_task_creation(self):
        """Test creation of dbt task."""
        task_data = {
            "task_key": "dbt_task_1",
            "task_type": "dbt",
            "task_config": json.dumps({
                "commands": "dbt run --models my_model",
                "warehouse_id": "abc123",
                "profiles_directory": "/path/to/profiles",
                "project_directory": "/path/to/project",
                "catalog": "main",
                "schema": "analytics",
            }),
            "parameters": "{}",
            "disabled": False,
        }
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(task_data, control_table, default_warehouse_id="default_warehouse")

        assert task_config["task_type"] == TASK_TYPE_DBT
        assert "dbt_task" in task_config
        assert task_config["dbt_task"]["commands"] == "dbt run --models my_model"
        assert task_config["dbt_task"]["warehouse_id"] == "abc123"

    def test_dbt_task_missing_commands(self):
        """Test dbt task with missing commands."""
        with pytest.raises(ValueError, match="Missing commands"):
            create_dbt_task_config(
                "test_task",
                {"warehouse_id": "abc123"},
                "default_warehouse"
            )

    def test_dbt_task_missing_warehouse_id(self):
        """Test dbt task with missing warehouse_id."""
        with pytest.raises(ValueError, match="Missing warehouse_id"):
            create_dbt_task_config(
                "test_task",
                {"commands": "dbt run"},
                None
            )


class TestAdvancedTaskFeatures:
    """Tests for advanced task features (run_if, job_cluster_key, existing_cluster_id, environment_key, notifications)."""

    def test_task_with_run_if(self, sample_task_data):
        """Test task with run_if condition."""
        sample_task_data["task_config"] = json.dumps({
            "file_path": "/Workspace/test/notebook",
            "run_if": "AT_LEAST_ONE_SUCCESS",
        })
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["run_if"] == "AT_LEAST_ONE_SUCCESS"

    def test_task_with_job_cluster_key(self, sample_task_data):
        """Test task with job_cluster_key."""
        sample_task_data["task_config"] = json.dumps({
            "file_path": "/Workspace/test/notebook",
            "job_cluster_key": "Job_cluster",
        })
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["job_cluster_key"] == "Job_cluster"

    def test_task_with_existing_cluster_id(self, sample_task_data):
        """Test task with existing_cluster_id."""
        sample_task_data["task_config"] = json.dumps({
            "file_path": "/Workspace/test/notebook",
            "existing_cluster_id": "1106-160244-2ko4u9ke",
        })
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["existing_cluster_id"] == "1106-160244-2ko4u9ke"

    def test_task_with_environment_key(self, sample_task_data):
        """Test task with environment_key."""
        sample_task_data["task_config"] = json.dumps({
            "file_path": "/Workspace/test/notebook",
            "environment_key": "default_python",
        })
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert task_config["environment_key"] == "default_python"

    def test_task_with_notification_settings(self, sample_task_data):
        """Test task with notification_settings."""
        sample_task_data["task_config"] = json.dumps({
            "file_path": "/Workspace/test/notebook",
            "notification_settings": {
                "email_notifications": {
                    "on_failure": ["alerts@example.com"],
                },
                "alert_on_last_attempt": True,
            },
        })
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_task_data, control_table)

        assert "notification_settings" in task_config
        assert task_config["notification_settings"]["email_notifications"]["on_failure"] == ["alerts@example.com"]
        assert task_config["notification_settings"]["alert_on_last_attempt"] is True
