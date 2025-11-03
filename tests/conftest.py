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
def sample_source_data():
    """Sample source data from control table."""
    return {
        'source_id': 'test_source_1',
        'module_name': 'test_module',
        'source_type': 'sql',
        'execution_order': 1,
        'source_config': '{"catalog": "bronze", "schema": "raw_data", "table": "source_table"}',
        'target_config': '{"catalog": "bronze", "schema": "raw_data", "table": "target_table"}',
        'transformation_config': '{"task_type": "notebook", "notebook_path": "/Workspace/test/notebook"}',
        'is_active': True
    }


@pytest.fixture
def sample_yaml_config():
    """Sample YAML configuration."""
    return {
        'modules': [
            {
                'module_name': 'test_module',
                'sources': [
                    {
                        'source_id': 'source1',
                        'execution_order': 1,
                        'source_type': 'sql',
                        'source_config': {'catalog': 'bronze', 'schema': 'raw_data', 'table': 'source_table'},
                        'target_config': {'catalog': 'bronze', 'schema': 'raw_data', 'table': 'target_table'},
                        'transformation_config': {
                            'task_type': 'notebook',
                            'notebook_path': '/Workspace/test/notebook'
                        }
                    }
                ]
            }
        ]
    }


@pytest.fixture
def sample_metadata_changes():
    """Sample metadata changes response."""
    return {
        'new_modules': ['module1'],
        'updated_modules': ['module2'],
        'deactivated_modules': [],
        'changed_sources': [
            {'source_id': 'source1', 'module_name': 'module1', 'action': 'new'}
        ]
    }

