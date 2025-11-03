"""Tests for utility functions"""

import pytest
from lakeflow_job_meta.utils import sanitize_task_key, validate_notebook_path


class TestSanitizeTaskKey:
    """Tests for sanitize_task_key function."""
    
    def test_basic_sanitization(self):
        """Test basic task key sanitization."""
        assert sanitize_task_key("test_source") == "test_source"
        assert sanitize_task_key("test-source") == "test_source"
        assert sanitize_task_key("test_source_1") == "test_source_1"
    
    def test_special_characters(self):
        """Test sanitization of special characters."""
        assert sanitize_task_key("test@source") == "test_source"
        assert sanitize_task_key("test.source") == "test_source"
        assert sanitize_task_key("test-source.name") == "test_source_name"
    
    def test_consecutive_underscores(self):
        """Test removal of consecutive underscores."""
        assert sanitize_task_key("test__source") == "test_source"
        assert sanitize_task_key("test___source") == "test_source"
    
    def test_leading_trailing_underscores(self):
        """Test removal of leading/trailing underscores."""
        assert sanitize_task_key("_test_source_") == "test_source"
        assert sanitize_task_key("__test__") == "test"
    
    def test_starts_with_non_alphanumeric(self):
        """Test handling of keys starting with non-alphanumeric."""
        assert sanitize_task_key("@test_source") == "task_test_source"
        assert sanitize_task_key("-test_source") == "task_test_source"
        assert sanitize_task_key(".test_source") == "task_test_source"
    
    def test_empty_string(self):
        """Test handling of empty string."""
        result = sanitize_task_key("")
        assert isinstance(result, str)
        assert len(result) == 0 or result.startswith("task_")
    
    def test_numeric_input(self):
        """Test handling of numeric input."""
        assert sanitize_task_key("123") == "123"
        assert sanitize_task_key(123) == "123"


class TestValidateNotebookPath:
    """Tests for validate_notebook_path function."""
    
    def test_standard_paths(self):
        """Test validation of standard notebook paths."""
        assert validate_notebook_path("/Workspace/test/notebook") is True
        assert validate_notebook_path("/frameworks/test/notebook") is True
        assert validate_notebook_path("/pipelines/test/notebook") is True
    
    def test_custom_paths(self):
        """Test validation of custom notebook paths (should still pass)."""
        assert validate_notebook_path("/custom/path/notebook") is True
        assert validate_notebook_path("/Users/test/notebook") is True
    
    def test_empty_path(self):
        """Test handling of empty path."""
        assert validate_notebook_path("") is True
    
    def test_none_path(self):
        """Test handling of None path."""
        assert validate_notebook_path(None) is True

