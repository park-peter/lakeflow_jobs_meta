"""Constants for the Lakeflow Job Meta framework"""

# Timeouts
TASK_TIMEOUT_SECONDS = 3600
JOB_TIMEOUT_SECONDS = 7200
MAX_CONCURRENT_RUNS = 1

# Task types
TASK_TYPE_NOTEBOOK = "notebook"
TASK_TYPE_SQL_QUERY = "sql_query"
TASK_TYPE_SQL_FILE = "sql_file"
TASK_TYPE_PYTHON_SCRIPT = "python_script"  # Future
TASK_TYPE_PIPELINE = "pipeline"  # Future

# Supported task types
SUPPORTED_TASK_TYPES = [
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
]

# Framework paths
FRAMEWORK_PATH_PREFIX = "/frameworks/"

