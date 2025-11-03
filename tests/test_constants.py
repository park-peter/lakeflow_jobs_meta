"""Tests for constants"""

from lakeflow_job_meta.constants import (
    TASK_TIMEOUT_SECONDS,
    JOB_TIMEOUT_SECONDS,
    MAX_CONCURRENT_RUNS,
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
    SUPPORTED_TASK_TYPES,
    FRAMEWORK_PATH_PREFIX
)


class TestConstants:
    """Tests for framework constants."""
    
    def test_timeout_constants(self):
        """Test timeout constant values."""
        assert isinstance(TASK_TIMEOUT_SECONDS, int)
        assert TASK_TIMEOUT_SECONDS > 0
        
        assert isinstance(JOB_TIMEOUT_SECONDS, int)
        assert JOB_TIMEOUT_SECONDS > 0
        
        assert JOB_TIMEOUT_SECONDS >= TASK_TIMEOUT_SECONDS
    
    def test_concurrent_runs(self):
        """Test max concurrent runs constant."""
        assert isinstance(MAX_CONCURRENT_RUNS, int)
        assert MAX_CONCURRENT_RUNS > 0
    
    def test_task_types(self):
        """Test task type constants."""
        assert TASK_TYPE_NOTEBOOK == "notebook"
        assert TASK_TYPE_SQL_QUERY == "sql_query"
        assert TASK_TYPE_SQL_FILE == "sql_file"
    
    def test_supported_task_types(self):
        """Test supported task types list."""
        assert isinstance(SUPPORTED_TASK_TYPES, list)
        assert len(SUPPORTED_TASK_TYPES) > 0
        assert TASK_TYPE_NOTEBOOK in SUPPORTED_TASK_TYPES
        assert TASK_TYPE_SQL_QUERY in SUPPORTED_TASK_TYPES
        assert TASK_TYPE_SQL_FILE in SUPPORTED_TASK_TYPES
    
    def test_framework_path_prefix(self):
        """Test framework path prefix."""
        assert isinstance(FRAMEWORK_PATH_PREFIX, str)
        assert FRAMEWORK_PATH_PREFIX.startswith("/")

