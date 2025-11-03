"""Tests for task builder functions"""

import pytest
import json
from unittest.mock import patch
from lakeflow_job_meta.task_builders import (
    create_task_from_config,
    create_notebook_task_config,
    create_sql_query_task_config,
    create_sql_file_task_config,
    convert_task_config_to_sdk_task,
)
from lakeflow_job_meta.constants import TASK_TYPE_NOTEBOOK, TASK_TYPE_SQL_QUERY, TASK_TYPE_SQL_FILE


class TestCreateTaskFromConfig:
    """Tests for create_task_from_config function."""

    def test_notebook_task_creation(self, sample_source_data):
        """Test creation of notebook task."""
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_source_data, control_table)

        assert task_config["task_key"] == "test_source_1"
        assert task_config["task_type"] == TASK_TYPE_NOTEBOOK
        assert "notebook_task" in task_config
        assert task_config["notebook_task"]["notebook_path"] == "/Workspace/test/notebook"
        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["source_id"] == "test_source_1"
        assert base_params["control_table"] == control_table

    def test_sql_query_task_creation(self, sample_source_data):
        """Test creation of SQL query task."""
        sample_source_data["transformation_config"] = json.dumps(
            {"task_type": "sql_query", "sql_task": {"warehouse_id": "abc123", "sql_query": "SELECT * FROM table"}}
        )
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_source_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_SQL_QUERY
        assert "sql_task" in task_config
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["sql_query"] == "SELECT * FROM table"

    def test_sql_file_task_creation(self, sample_source_data):
        """Test creation of SQL file task."""
        sample_source_data["transformation_config"] = json.dumps(
            {
                "task_type": "sql_file",
                "sql_task": {"warehouse_id": "abc123", "sql_file_path": "/Workspace/test/query.sql"},
            }
        )
        control_table = "main.examples.etl_control"
        task_config = create_task_from_config(sample_source_data, control_table)

        assert task_config["task_type"] == TASK_TYPE_SQL_FILE
        assert "sql_task" in task_config
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["sql_file_path"] == "/Workspace/test/query.sql"

    def test_task_with_dependencies(self, sample_source_data):
        """Test task creation with dependencies."""
        control_table = "main.examples.etl_control"
        previous_tasks = ["task1", "task2"]
        task_config = create_task_from_config(sample_source_data, control_table, previous_order_tasks=previous_tasks)

        assert "depends_on" in task_config
        assert len(task_config["depends_on"]) == 2
        assert task_config["depends_on"][0]["task_key"] == "task1"

    def test_invalid_json_config(self, sample_source_data):
        """Test handling of invalid JSON in transformation_config."""
        sample_source_data["transformation_config"] = "invalid json"
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="Invalid transformation_config JSON"):
            create_task_from_config(sample_source_data, control_table)

    def test_missing_task_type_defaults_to_notebook(self, sample_source_data):
        """Test that missing task_type defaults to notebook."""
        sample_source_data["transformation_config"] = json.dumps({"notebook_path": "/Workspace/test/notebook"})
        control_table = "main.examples.etl_control"
        task_source_data = create_task_from_config(sample_source_data, control_table)
        assert task_source_data["task_type"] == TASK_TYPE_NOTEBOOK

    def test_unsupported_task_type(self, sample_source_data):
        """Test handling of unsupported task type."""
        sample_source_data["transformation_config"] = json.dumps({"task_type": "unsupported_type"})
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="Unsupported task_type"):
            create_task_from_config(sample_source_data, control_table)


class TestCreateNotebookTaskConfig:
    """Tests for create_notebook_task_config function."""

    def test_valid_notebook_task(self, sample_source_data):
        """Test creation of valid notebook task."""
        trans_config = {"notebook_path": "/Workspace/test/notebook"}
        control_table = "main.examples.etl_control"

        task_config = create_notebook_task_config(sample_source_data, "test_task_key", trans_config, control_table)

        assert task_config["task_key"] == "test_task_key"
        assert task_config["notebook_task"]["notebook_path"] == "/Workspace/test/notebook"
        base_params = task_config["notebook_task"]["base_parameters"]
        assert base_params["source_id"] == "test_source_1"
        assert base_params["control_table"] == control_table

    def test_missing_notebook_path(self, sample_source_data):
        """Test error when notebook_path is missing."""
        trans_config = {}
        control_table = "main.examples.etl_control"

        with pytest.raises(ValueError, match="Missing notebook_path"):
            create_notebook_task_config(sample_source_data, "test_task_key", trans_config, control_table)


class TestCreateSqlQueryTaskConfig:
    """Tests for create_sql_query_task_config function."""

    def test_valid_sql_query_task(self, sample_source_data):
        """Test creation of valid SQL query task."""
        trans_config = {"sql_task": {"warehouse_id": "abc123", "sql_query": "SELECT * FROM table"}}

        task_config = create_sql_query_task_config(sample_source_data, "test_task_key", trans_config)

        assert task_config["task_key"] == "test_task_key"
        assert task_config["sql_task"]["warehouse_id"] == "abc123"
        assert task_config["sql_task"]["sql_query"] == "SELECT * FROM table"

    def test_sql_query_with_parameters(self, sample_source_data):
        """Test SQL query task with parameters."""
        trans_config = {
            "sql_task": {
                "warehouse_id": "abc123",
                "sql_query": "SELECT * FROM table",
                "parameters": {"param1": "value1"},
            }
        }

        task_config = create_sql_query_task_config(sample_source_data, "test_task_key", trans_config)

        assert task_config["sql_task"]["parameters"]["param1"] == "value1"

    def test_missing_sql_task(self, sample_source_data):
        """Test error when sql_task is missing."""
        trans_config = {}

        with pytest.raises(ValueError, match="Missing sql_task"):
            create_sql_query_task_config(sample_source_data, "test_task_key", trans_config)

    def test_missing_warehouse_id(self, sample_source_data):
        """Test error when warehouse_id is missing."""
        trans_config = {"sql_task": {"sql_query": "SELECT * FROM table"}}

        with pytest.raises(ValueError, match="Missing warehouse_id"):
            create_sql_query_task_config(sample_source_data, "test_task_key", trans_config)

    def test_missing_sql_query(self, sample_source_data):
        """Test error when sql_query is missing."""
        trans_config = {"sql_task": {"warehouse_id": "abc123"}}

        with pytest.raises(ValueError, match="Missing sql_query"):
            create_sql_query_task_config(sample_source_data, "test_task_key", trans_config)


class TestCreateSqlFileTaskConfig:
    """Tests for create_sql_file_task_config function."""

    def test_valid_sql_file_task(self, sample_source_data):
        """Test creation of valid SQL file task."""
        trans_config = {"sql_task": {"warehouse_id": "abc123", "sql_file_path": "/Workspace/test/query.sql"}}

        with patch("lakeflow_job_meta.task_builders.dbutils") as mock_dbutils:
            mock_dbutils.fs.head.return_value = "SELECT * FROM table"

            task_config = create_sql_file_task_config(sample_source_data, "test_task_key", trans_config)

            assert task_config["task_key"] == "test_task_key"
            assert task_config["sql_task"]["warehouse_id"] == "abc123"
            assert task_config["sql_task"]["sql_file_path"] == "/Workspace/test/query.sql"

    def test_missing_sql_file_path(self, sample_source_data):
        """Test error when sql_file_path is missing."""
        trans_config = {"sql_task": {"warehouse_id": "abc123"}}

        with pytest.raises(ValueError, match="Missing sql_file_path"):
            create_sql_file_task_config(sample_source_data, "test_task_key", trans_config)


class TestConvertTaskConfigToSdkTask:
    """Tests for convert_task_config_to_sdk_task function."""

    def test_notebook_task_conversion(self):
        """Test conversion of notebook task config to SDK task."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_NOTEBOOK,
            "notebook_task": {"notebook_path": "/Workspace/test/notebook", "base_parameters": {"param1": "value1"}},
        }

        sdk_task = convert_task_config_to_sdk_task(task_config, cluster_id="cluster123")

        assert sdk_task.task_key == "test_task"
        assert sdk_task.notebook_task.notebook_path == "/Workspace/test/notebook"
        assert sdk_task.existing_cluster_id == "cluster123"

    def test_sql_query_task_conversion(self):
        """Test conversion of SQL query task config to SDK task."""
        task_config = {
            "task_key": "test_task",
            "task_type": TASK_TYPE_SQL_QUERY,
            "sql_task": {"warehouse_id": "abc123", "query": {"query": "SELECT * FROM table"}},
        }

        sdk_task = convert_task_config_to_sdk_task(task_config)

        assert sdk_task.task_key == "test_task"
        assert sdk_task.sql_task.warehouse_id == "abc123"
        assert sdk_task.sql_task.query.query == "SELECT * FROM table"

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
