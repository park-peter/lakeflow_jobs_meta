"""Tests for MetadataManager class"""

import pytest
import tempfile
import os
import yaml
from unittest.mock import MagicMock, patch
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


@patch("lakeflow_jobs_meta.metadata_manager.F", _create_mock_f())
class TestMetadataManager:
    """Tests for MetadataManager class."""
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_ensure_exists_success(self, mock_get_spark, mock_spark_session):
        """Test successful table creation."""
        mock_get_spark.return_value = mock_spark_session
        
        manager = MetadataManager("test_catalog.schema.control_table")
        manager.ensure_exists()
        
        mock_spark_session.sql.assert_called_once()
        call_args = mock_spark_session.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "test_catalog.schema.control_table" in call_args
        assert "resource_id" in call_args  # New column in v0.2.0
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_ensure_exists_table_creation_error(self, mock_get_spark, mock_spark_session):
        """Test handling of table creation errors."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.sql.side_effect = Exception("Database error")
        
        manager = MetadataManager("test_table")
        
        with pytest.raises(RuntimeError, match="Failed to create control table"):
            manager.ensure_exists()
    
    def test_init_invalid_table_name(self):
        """Test error with invalid table name."""
        with pytest.raises(ValueError):
            MetadataManager("")
        
        with pytest.raises(ValueError):
            MetadataManager(None)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_current_user')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_valid(self, mock_get_spark, mock_get_current_user, mock_spark_session, sample_yaml_config):
        """Test loading valid YAML file."""
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"
        
        # Create temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(sample_yaml_config, tmp)
            tmp_path = tmp.name
        
        try:
            mock_df = MagicMock()
            mock_df.createOrReplaceTempView = MagicMock()
            mock_df.filter.return_value.select.return_value.collect.return_value = []
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.table.return_value = mock_df
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            tasks_loaded, resource_ids = manager.load_yaml(tmp_path)
            
            assert tasks_loaded == 1  # One task loaded
            assert resource_ids == ['test_job']
            mock_spark_session.createDataFrame.assert_called_once()
        finally:
            os.unlink(tmp_path)
    
    def test_load_yaml_file_not_found(self):
        """Test error when YAML file doesn't exist."""
        manager = MetadataManager("test_table")
        
        with pytest.raises(FileNotFoundError):
            manager.load_yaml("nonexistent.yaml")
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_file_not_found_skip_validation(self, mock_get_spark, mock_spark_session):
        """Test skipping file existence validation."""
        mock_get_spark.return_value = mock_spark_session

        manager = MetadataManager("test_table")
        
        with pytest.raises(ValueError, match="Failed to read YAML"):  # Should fail when trying to open file
            manager.load_yaml("nonexistent.yaml", validate_file_exists=False)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_invalid(self, mock_get_spark, mock_spark_session):
        """Test handling of invalid YAML."""
        mock_get_spark.return_value = mock_spark_session
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            tmp.write("invalid: yaml: content: [")
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="Failed to parse YAML"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_current_user')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_empty_jobs(self, mock_get_spark, mock_get_current_user, mock_spark_session):
        """Test handling of YAML with no jobs."""
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"
        
        empty_config = {'jobs': {}}  # Empty dict in new format
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(empty_config, tmp)
            tmp_path = tmp.name
        
        try:
            mock_df = MagicMock()
            mock_df.filter.return_value.select.return_value.collect.return_value = []
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.table.return_value = mock_df
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            tasks_loaded, resource_ids = manager.load_yaml(tmp_path)
            assert tasks_loaded == 0
            assert resource_ids == []
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_missing_jobs_key(self, mock_get_spark, mock_spark_session):
        """Test error when YAML lacks jobs key."""
        mock_get_spark.return_value = mock_spark_session
        
        invalid_config = {}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(invalid_config, tmp)
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="must contain 'jobs' key"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_current_user')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_missing_task_type(self, mock_get_spark, mock_get_current_user, mock_spark_session):
        """Test error when task_type is missing."""
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"
        
        config = {
            'jobs': {
                'test_job': {
                    'tasks': [{
                        'task_key': 'task1',
                        'depends_on': []
                        # Missing task_type - should raise error
                    }]
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            tasks_loaded, resource_ids = manager.load_yaml(tmp_path)
            assert tasks_loaded == 0
            assert len(resource_ids) == 0
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_current_user')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_partial_failure(self, mock_get_spark, mock_get_current_user, mock_spark_session):
        """Test that valid jobs are loaded even when one job has errors."""
        mock_get_spark.return_value = mock_spark_session
        mock_get_current_user.return_value = "test_user"
        
        config = {
            'jobs': {
                'valid_job': {
                    'tasks': [{
                        'task_key': 'task1',
                        'task_type': 'notebook_task',
                        'file_path': '/path/to/notebook'
                    }]
                },
                'invalid_job': {
                    'tasks': [{
                        'task_key': 'task1',
                        'task_type': 'notebook_task',
                        'file_path': '/path/to/notebook',
                        'depends_on': ['nonexistent_task']  # Invalid dependency
                    }]
                },
                'another_valid_job': {
                    'tasks': [{
                        'task_key': 'task1',
                        'task_type': 'sql_query_task',
                        'sql_query': 'SELECT 1',
                        'warehouse_id': 'test'
                    }]
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp_path = tmp.name
        
        try:
            mock_df = MagicMock()
            mock_df.filter.return_value.select.return_value.collect.return_value = []
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.table.return_value = mock_df
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            tasks_loaded, resource_ids = manager.load_yaml(tmp_path)
            assert tasks_loaded == 2
            assert len(resource_ids) == 2
            assert 'valid_job' in resource_ids
            assert 'another_valid_job' in resource_ids
            assert 'invalid_job' not in resource_ids
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_no_changes(self, mock_get_spark, mock_spark_session):
        """Test detection when no changes exist."""
        mock_get_spark.return_value = mock_spark_session

        mock_df = MagicMock()
        mock_df.filter.return_value.count.return_value = 0
        mock_df.select.return_value.distinct.return_value.collect.return_value = []
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes("2024-01-01T00:00:00")
        
        assert changes['new_jobs'] == []
        assert changes['updated_jobs'] == []
        assert changes['disabled_jobs'] == []
        assert changes['changed_tasks'] == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_updated(self, mock_get_spark, mock_spark_session):
        """Test detection of updated jobs."""
        mock_get_spark.return_value = mock_spark_session

        mock_df = MagicMock()

        mock_job_row = MagicMock()
        mock_job_row.__getitem__.side_effect = lambda key: {
            'job_name': 'job1',
            'disabled': False,
            'created_timestamp': '2024-01-01',
            'updated_timestamp': '2024-01-02'
        }.get(key)
        mock_job_row.get.side_effect = lambda key, default=None: {
            'job_name': 'job1',
            'disabled': False
        }.get(key, default)
        
        mock_filtered_df = MagicMock()
        mock_filtered_df.collect.return_value = [mock_job_row]
        mock_df.filter.return_value = mock_filtered_df
        
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes("2024-01-01T00:00:00")
        
        assert 'job1' in changes['updated_jobs']
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_new_jobs(self, mock_get_spark, mock_spark_session):
        """Test detection of new jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_df = MagicMock()
        mock_job_row = MagicMock()
        mock_job_row.__getitem__.side_effect = lambda key: 'new_job' if key == 'job_name' else None
        
        mock_changed_df = MagicMock()
        mock_changed_df.count.return_value = 0
        
        mock_all_jobs_df = MagicMock()
        mock_all_jobs_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row]
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row]
        mock_df.filter.return_value = mock_changed_df
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes(None)
        
        assert 'new_job' in changes['new_jobs']
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_spark_error_handling(self, mock_get_spark, mock_spark_session):
        """Test error handling when Spark operations fail."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.table.side_effect = Exception("Spark error")
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes()
        
        assert changes['new_jobs'] == []
        assert changes['updated_jobs'] == []
        assert changes['disabled_jobs'] == []
        assert changes['changed_tasks'] == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_all_jobs(self, mock_get_spark, mock_spark_session):
        """Test getting all jobs (returns resource_ids in v0.2.0)."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_job_row1 = MagicMock()
        mock_job_row1.__getitem__.side_effect = lambda key: 'job1' if key == 'resource_id' else None
        
        mock_job_row2 = MagicMock()
        mock_job_row2.__getitem__.side_effect = lambda key: 'job2' if key == 'resource_id' else None
        
        mock_df = MagicMock()
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row1, mock_job_row2]
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        resource_ids = manager.get_all_jobs()
        
        assert len(resource_ids) == 2
        assert 'job1' in resource_ids
        assert 'job2' in resource_ids
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_job_tasks(self, mock_get_spark, mock_spark_session):
        """Test getting tasks for a job."""
        mock_get_spark.return_value = mock_spark_session
        
        task_data = {
            'task_key': 'task1',
            'job_name': 'job1',
            'resource_id': 'job1',
            'depends_on': None,
            'disabled': False,
            'task_type': 'notebook',
            'task_config': '{"file_path": "/test"}'
        }
        mock_task_row = MagicMock()
        mock_task_row.asDict.return_value = task_data
        mock_task_row.__getitem__.side_effect = lambda key: task_data.get(key)
        
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = [mock_task_row]
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        tasks = manager.get_job_tasks("job1")
        
        assert len(tasks) == 1
        assert tasks[0]['task_key'] == 'task1'
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_current_user')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume(self, mock_get_spark, mock_get_current_user):
        """Test syncing YAML files from volume (atomic loading in v0.2.0)."""
        mock_get_current_user.return_value = "test_user"
        
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        mock_dbutils = MagicMock()
        mock_file1 = MagicMock()
        mock_file1.isDir.return_value = False
        mock_file1.name = "config1.yaml"
        mock_file1.path = "dbfs:/Volumes/test/config1.yaml"
        
        mock_file2 = MagicMock()
        mock_file2.isDir.return_value = False
        mock_file2.name = "config2.yaml"
        mock_file2.path = "dbfs:/Volumes/test/config2.yaml"
        
        mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2]
        
        yaml_content1 = "jobs:\n  test_job1:\n    tasks:\n      - task_key: task1\n        task_type: notebook\n        file_path: /test"
        yaml_content2 = "jobs:\n  test_job2:\n    tasks:\n      - task_key: task1\n        task_type: notebook\n        file_path: /test"
        mock_dbutils.fs.head.side_effect = [yaml_content1, yaml_content2]
        
        mock_df = MagicMock()
        mock_df.filter.return_value.select.return_value.collect.return_value = []
        mock_spark.createDataFrame = MagicMock(return_value=mock_df)
        mock_spark.table.return_value = mock_df
        mock_spark.sql = MagicMock()
        
        with patch('lakeflow_jobs_meta.metadata_manager._get_dbutils', return_value=mock_dbutils):
            manager = MetadataManager("test_table")
            tasks_loaded, resource_ids = manager.sync_from_volume("/Volumes/test/volume")
            
            assert tasks_loaded == 2
            assert len(resource_ids) == 2
            assert mock_spark.createDataFrame.called
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_dbutils')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume_no_files(self, mock_get_spark, mock_get_dbutils):
        """Test handling when no YAML files exist."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        mock_dbutils = MagicMock()
        mock_dbutils.fs.ls.return_value = []
        mock_get_dbutils.return_value = mock_dbutils
        
        manager = MetadataManager("test_table")
        tasks_loaded, resource_ids = manager.sync_from_volume("/Volumes/test/volume")
        
        assert tasks_loaded == 0
        assert resource_ids == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_dbutils')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume_dbutils_not_available(self, mock_get_spark, mock_get_dbutils):
        """Test error when dbutils and spark list fails."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_dbutils.return_value = None
        mock_spark.sql.side_effect = Exception("Unable to list files")
        
        manager = MetadataManager("test_table")
        with pytest.raises(RuntimeError, match="Failed to sync YAML files from volume"):
            manager.sync_from_volume("/Volumes/test/volume")
