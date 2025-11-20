"""Utility functions for the Lakeflow Jobs Meta framework"""

import re
from typing import Optional, Dict, Any, Union


def sanitize_task_key(task_key: str) -> str:
    """Sanitize task_key to create a valid task key.

    Args:
        task_key: The task identifier

    Returns:
        Sanitized task key safe for use in Databricks job definitions
    """
    original = str(task_key)
    # Check if original starts with non-alphanumeric and non-underscore
    starts_with_invalid = original and not original[0].isalnum() and original[0] != "_"
    
    # Replace invalid characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", original)
    # Remove consecutive underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Ensure it doesn't start or end with underscore
    sanitized = sanitized.strip("_")
    
    # If original started with invalid char (not underscore), prepend "task_"
    if starts_with_invalid and sanitized:
        sanitized = "task_" + sanitized
    
    return sanitized


def validate_notebook_path(notebook_path: str) -> bool:
    """Validate notebook path format (non-blocking validation).

    This is a lightweight validation that checks path format.
    Does not verify actual file existence to avoid blocking execution.

    Args:
        notebook_path: Path to the notebook

    Returns:
        True if validation passes (always returns True - non-blocking)
    """
    if notebook_path and not notebook_path.startswith(("/pipelines/", "/frameworks/", "/Workspace/")):
        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"Using custom notebook path: {notebook_path}")
    return True


def substitute_variables(content: str, variables: Dict[str, Any]) -> str:
    """Substitute ${var.name} patterns in content with values from variables dict.

    Uses Databricks Asset Bundles compatible syntax: ${var.variable_name}

    Args:
        content: String content with variable placeholders
        variables: Dictionary of variable names to values

    Returns:
        Content with all variables substituted

    Raises:
        ValueError: If a variable is referenced but not found in variables dict

    Example:
        >>> variables = {'env': 'prod', 'catalog': 'bronze'}
        >>> substitute_variables("SELECT * FROM ${var.catalog}.${var.env}_data", variables)
        'SELECT * FROM bronze.prod_data'
    """
    if not variables:
        return content

    # Pattern to match ${var.variable_name}
    # Allows alphanumeric and underscore in variable names, must start with letter or underscore
    pattern = r'\$\{var\.([a-zA-Z_][a-zA-Z0-9_]*)\}'

    def replacer(match):
        var_name = match.group(1)
        if var_name not in variables:
            raise ValueError(
                f"Variable '${{{var_name}}}' is referenced but not provided in variables dict. "
                f"Available variables: {list(variables.keys())}"
            )
        # Convert value to string for substitution
        return str(variables[var_name])

    # Substitute all occurrences
    result = re.sub(pattern, replacer, content)
    return result
