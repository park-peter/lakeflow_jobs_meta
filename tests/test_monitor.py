"""Tests for MetadataMonitor class"""

import pytest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime

from lakeflow_job_meta.monitor import MetadataMonitor


class TestMetadataMonitor:
    """Tests for MetadataMonitor class."""
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    def test_initialization(self, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test monitor initialization."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", check_interval_seconds=30, volume_path="/Volumes/test")
        
        assert monitor.control_table == "test_table"
        assert monitor.check_interval == 30
        assert monitor.volume_path == "/Volumes/test"
        assert monitor.auto_update_jobs is True
        assert monitor.last_check_timestamp is None
        mock_manager_instance.ensure_exists.assert_called_once()
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    def test_check_and_update_no_changes(self, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test check_and_update when no changes detected."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_instance.detect_changes.return_value = {
            'new_modules': [],
            'updated_modules': [],
            'deactivated_modules': [],
            'changed_sources': []
        }
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table")
        result = monitor.check_and_update()
        
        assert result['changes_detected'] is False
        assert len(result['jobs_updated']) == 0
        mock_orchestrator_instance.create_or_update_job.assert_not_called()
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    @patch('lakeflow_job_meta.monitor.SparkSession')
    def test_check_and_update_with_changes(self, mock_spark_session, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test check_and_update when changes are detected."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_instance.detect_changes.return_value = {
            'new_modules': ['module1'],
            'updated_modules': [],
            'deactivated_modules': [],
            'changed_sources': []
        }
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_instance.create_or_update_job.return_value = 12345
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table")
        
        result = monitor.check_and_update()
        
        assert result['changes_detected'] is True
        assert result['table_changes'] is True
        assert len(result['jobs_updated']) == 1
        assert result['jobs_updated'][0]['module'] == 'module1'
        assert result['jobs_updated'][0]['job_id'] == 12345
        mock_orchestrator_instance.create_or_update_job.assert_called_once_with('module1')
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    def test_yaml_file_changes(self, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test detection of YAML file changes."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_instance.sync_from_volume.return_value = 5
        mock_manager_instance.detect_changes.return_value = {
            'new_modules': [],
            'updated_modules': [],
            'deactivated_modules': [],
            'changed_sources': []
        }
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", volume_path="/Volumes/test")
        
        # Mock YAML file checking
        with patch.object(monitor, '_check_yaml_files_changed', return_value=True):
            result = monitor.check_and_update()
            
            assert result['yaml_changes'] is True
            assert result['changes_detected'] is True
            mock_manager_instance.sync_from_volume.assert_called_once()
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    def test_check_yaml_files_changed_new_file(self, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test detection of new YAML files."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", volume_path="/Volumes/test")
        
        # Mock dbutils
        mock_dbutils = MagicMock()
        mock_file = MagicMock()
        mock_file.name = "config.yaml"
        mock_file.modificationTime = 1234567890
        mock_dbutils.fs.ls.return_value = [mock_file]
        
        with patch('lakeflow_job_meta.monitor.dbutils', mock_dbutils):
            result = monitor._check_yaml_files_changed()
            
            assert result is True
            assert "config.yaml" in monitor.last_yaml_file_times
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    def test_check_yaml_files_changed_modified_file(self, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test detection of modified YAML files."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", volume_path="/Volumes/test")
        monitor.last_yaml_file_times = {"config.yaml": 1234567890}
        
        # Mock dbutils
        mock_dbutils = MagicMock()
        mock_file = MagicMock()
        mock_file.name = "config.yaml"
        mock_file.modificationTime = 1234567891  # Different modification time
        mock_dbutils.fs.ls.return_value = [mock_file]
        
        with patch('lakeflow_job_meta.monitor.dbutils', mock_dbutils):
            result = monitor._check_yaml_files_changed()
            
            assert result is True
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    @patch('lakeflow_job_meta.monitor.time.sleep')
    @patch.object(MetadataMonitor, 'check_and_update')
    def test_run_continuous(self, mock_check, mock_sleep, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test continuous monitoring loop."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", check_interval_seconds=1)
        monitor.run_continuous(max_iterations=2)
        
        # Should check twice (2 iterations)
        assert mock_check.call_count == 2
        assert mock_sleep.call_count == 2
    
    @patch('lakeflow_job_meta.monitor.MetadataManager')
    @patch('lakeflow_job_meta.monitor.JobOrchestrator')
    @patch('lakeflow_job_meta.monitor.WorkspaceClient')
    @patch('lakeflow_job_meta.monitor.time.sleep')
    @patch.object(MetadataMonitor, 'check_and_update')
    def test_run_continuous_keyboard_interrupt(self, mock_check, mock_sleep, mock_client_class, mock_orchestrator_class, mock_manager_class):
        """Test handling of keyboard interrupt."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        mock_manager_instance = MagicMock()
        mock_manager_class.return_value = mock_manager_instance
        
        mock_orchestrator_instance = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator_instance
        
        monitor = MetadataMonitor("test_table", check_interval_seconds=1)
        mock_check.side_effect = [None, KeyboardInterrupt()]
        
        # Should handle gracefully
        monitor.run_continuous(max_iterations=None)
        
        assert mock_check.call_count == 2
