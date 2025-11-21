"""Pytest configuration and fixtures"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, Any


@pytest.fixture
def mock_spark_session():
    """Mock Spark session."""
    spark = MagicMock()
    
    # Mock table() method
    mock_df = MagicMock()
    mock_table = MagicMock(return_value=mock_df)
    spark.table = mock_table
    
    # Mock sql() method
    spark.sql = MagicMock()
    
    # Mock createDataFrame
    spark.createDataFrame = MagicMock(return_value=mock_df)
    
    return spark


@pytest.fixture
def mock_workspace_client():
    """Mock Databricks WorkspaceClient."""
    client = MagicMock()
    
    # Mock jobs API
    mock_jobs_api = MagicMock()
    client.jobs = mock_jobs_api
    
    # Mock create and update methods
    mock_create_response = MagicMock()
    mock_create_response.job_id = 12345
    mock_jobs_api.create = MagicMock(return_value=mock_create_response)
    
    mock_update_response = MagicMock()
    mock_jobs_api.update = MagicMock(return_value=mock_update_response)
    
    mock_run_response = MagicMock()
    mock_run_response.run_id = 67890
    mock_jobs_api.run_now = MagicMock(return_value=mock_run_response)
    
    return client


@pytest.fixture
def sample_task_data():
    """Sample task data from control table."""
    return {
        'resource_id': 'test_job',
        'job_name': 'test_job',
        'task_key': 'test_task_1',
        'depends_on': '[]',
        'task_type': 'notebook',
        'job_config': '{"timeout_seconds": 7200}',
        'task_config': '{"file_path": "/Workspace/test/notebook", "parameters": {"catalog": "bronze", "schema": "raw_data", "source_table": "source_table", "target_table": "target_table"}}',
        'disabled': False
    }


@pytest.fixture
def sample_yaml_config():
    """Sample YAML configuration (v0.2.0 dict format)."""
    return {
        'jobs': {
            'test_job': {
                'tasks': [
                    {
                        'task_key': 'task1',
                        'task_type': 'notebook',
                        'depends_on': None,
                        'file_path': '/Workspace/test/notebook',
                        'parameters': {
                            'catalog': 'bronze',
                            'schema': 'raw_data',
                            'source_table': 'source_table',
                            'target_table': 'target_table'
                        }
                    }
                ]
            }
        }
    }


@pytest.fixture
def sample_metadata_changes():
    """Sample metadata changes response."""
    return {
        'new_jobs': ['job1'],
        'updated_jobs': ['job2'],
        'disabled_jobs': [],
        'changed_tasks': [
            {'task_key': 'task1', 'job_name': 'job1', 'action': 'new'}
        ]
    }

