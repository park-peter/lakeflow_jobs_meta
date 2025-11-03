# Test Suite

Comprehensive test suite for Lakeflow Job Meta framework.

## Test Files

- **test_utils.py** - Tests for utility functions (`sanitize_task_key`, `validate_notebook_path`)
- **test_task_builders.py** - Tests for task configuration creation and conversion
- **test_metadata_manager.py** - Tests for metadata loading, YAML parsing, and change detection
- **test_orchestrator.py** - Tests for job creation, updating, and orchestration
- **test_monitor.py** - Tests for continuous monitoring functionality
- **test_constants.py** - Tests for constant validation

## Running Tests

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=lakeflow_job_meta --cov-report=html --cov-report=term-missing

# Run specific test module
pytest tests/test_utils.py -v

# Run specific test class
pytest tests/test_utils.py::TestSanitizeTaskKey -v

# Run specific test
pytest tests/test_utils.py::TestSanitizeTaskKey::test_basic_sanitization -v

# Run with markers
pytest -m unit
```

## Test Coverage

The test suite uses mocking extensively to avoid requiring actual Databricks or Spark environments:
- Mock Spark session for DataFrame operations
- Mock Databricks WorkspaceClient for job operations
- Mock dbutils for file operations
- Mock file system for YAML loading tests

## Writing New Tests

When adding new functionality:

1. Create test file: `tests/test_<module_name>.py`
2. Use fixtures from `conftest.py` when available
3. Mock external dependencies (Spark, Databricks SDK, dbutils)
4. Test both success and error cases
5. Use descriptive test names following `test_<what>_<condition>` pattern

Example:
```python
def test_create_task_with_invalid_source_id(self):
    """Test error handling when source_id is invalid."""
    with pytest.raises(ValueError):
        create_task_from_config(invalid_source_data)
```

