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
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_valid(self, mock_get_spark, mock_spark_session, sample_yaml_config):
        """Test loading valid YAML file."""
        mock_get_spark.return_value = mock_spark_session
        
        # Create temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(sample_yaml_config, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock DataFrame operations
            mock_df = MagicMock()
            mock_df.createOrReplaceTempView = MagicMock()
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            tasks_loaded, job_names = manager.load_yaml(tmp_path)
            
            assert tasks_loaded == 1  # One task loaded
            assert job_names == ['test_job']
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
        # Mock sql() to succeed for table creation
        mock_spark_session.sql = MagicMock()
        
        manager = MetadataManager("test_table")
        
        with pytest.raises(ValueError, match="Failed to parse YAML"):  # Should fail when trying to open file
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
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_empty_jobs(self, mock_get_spark, mock_spark_session):
        """Test handling of YAML with no jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        empty_config = {'jobs': []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(empty_config, tmp)
            tmp_path = tmp.name
        
        try:
            mock_df = MagicMock()
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            tasks_loaded, job_names = manager.load_yaml(tmp_path)
            assert tasks_loaded == 0
            assert job_names == []
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
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_missing_task_type(self, mock_get_spark, mock_spark_session):
        """Test error when task_type is missing."""
        mock_get_spark.return_value = mock_spark_session
        
        config = {
            'jobs': [{
                'job_name': 'test_job',
                'tasks': [{
                    'task_key': 'task1',
                    'depends_on': []
                    # Missing task_type - should raise error
                }]
            }]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="must have 'task_type' field"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_no_changes(self, mock_get_spark, mock_spark_session):
        """Test detection when no changes exist."""
        mock_get_spark.return_value = mock_spark_session
        
        # Mock DataFrame with no changes
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
        
        # Mock DataFrame with changes
        mock_df = MagicMock()
        
        # Mock Row object
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
        
        # Mock changed rows - filter().collect() returns changed rows
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
        
        # Mock for updated jobs (empty)
        mock_changed_df = MagicMock()
        mock_changed_df.count.return_value = 0
        
        # Mock for all jobs
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
        
        # Should return empty changes dict
        assert changes['new_jobs'] == []
        assert changes['updated_jobs'] == []
        assert changes['disabled_jobs'] == []
        assert changes['changed_tasks'] == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_all_jobs(self, mock_get_spark, mock_spark_session):
        """Test getting all jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_job_row1 = MagicMock()
        mock_job_row1.__getitem__.side_effect = lambda key: 'job1' if key == 'job_name' else None
        
        mock_job_row2 = MagicMock()
        mock_job_row2.__getitem__.side_effect = lambda key: 'job2' if key == 'job_name' else None
        
        mock_df = MagicMock()
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row1, mock_job_row2]
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        jobs = manager.get_all_jobs()
        
        assert len(jobs) == 2
        assert 'job1' in jobs
        assert 'job2' in jobs
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_job_tasks(self, mock_get_spark, mock_spark_session):
        """Test getting tasks for a job."""
        mock_get_spark.return_value = mock_spark_session
        
        # Mock task row with asDict method
        task_data = {
            'task_key': 'task1',
            'job_name': 'job1',
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
    
    @patch('lakeflow_jobs_meta.metadata_manager.MetadataManager.load_yaml')
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume(self, mock_get_spark, mock_load_yaml):
        """Test syncing YAML files from volume."""
        # Mock Spark session
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        # Mock file listing using Spark SQL LIST
        mock_row1 = MagicMock()
        mock_row1_data = {"name": "config1.yaml", "path": "/Volumes/test/config1.yaml", "type": "file"}
        mock_row1.__getitem__ = lambda key: mock_row1_data.get(key)
        mock_row1.get = lambda key, default="": mock_row1_data.get(key, default)
        
        mock_row2 = MagicMock()
        mock_row2_data = {"name": "config2.yaml", "path": "/Volumes/test/config2.yaml", "type": "file"}
        mock_row2.__getitem__ = lambda key: mock_row2_data.get(key)
        mock_row2.get = lambda key, default="": mock_row2_data.get(key, default)
        
        mock_row3 = MagicMock()
        mock_row3_data = {"name": "data.txt", "path": "/Volumes/test/data.txt", "type": "file"}
        mock_row3.__getitem__ = lambda key: mock_row3_data.get(key)
        mock_row3.get = lambda key, default="": mock_row3_data.get(key, default)
        
        mock_spark.sql.return_value.collect.return_value = [mock_row1, mock_row2, mock_row3]
        mock_spark.sparkContext.textFile.return_value.collect.return_value = ["test: yaml", "content: true"]
        
        mock_load_yaml.return_value = (5, ['job1'])  # 5 tasks loaded, 1 job
        
        manager = MetadataManager("test_table")
        # Mock the load_yaml method
        manager.load_yaml = mock_load_yaml
        
        tasks_loaded, job_names = manager.sync_from_volume("/Volumes/test/volume")
        
        assert tasks_loaded == 10  # 5 + 5 from two files
        assert mock_spark.sql.called
        assert mock_load_yaml.call_count == 2  # Called for each YAML file
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume_no_files(self, mock_get_spark):
        """Test handling when no YAML files exist."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        # Mock empty directory listing
        mock_spark.sql.return_value.filter.return_value.collect.return_value = []
        
        manager = MetadataManager("test_table")
        tasks_loaded, job_names = manager.sync_from_volume("/Volumes/test/volume")
        
        assert tasks_loaded == 0
        assert job_names == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_sync_from_volume_dbutils_not_available(self, mock_get_spark):
        """Test error when spark list fails."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.side_effect = Exception("Unable to list files")
        
        manager = MetadataManager("test_table")
        with pytest.raises(Exception, match="Unable to list files"):
            manager.sync_from_volume("/Volumes/test/volume")
