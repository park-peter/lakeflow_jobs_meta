"""Utility functions for the Lakeflow Job Meta framework"""

import re
from typing import Optional


def sanitize_task_key(source_id: str) -> str:
    """Sanitize source_id to create a valid task key.
    
    Args:
        source_id: The source identifier
        
    Returns:
        Sanitized task key safe for use in Databricks job definitions
    """
    # Replace invalid characters with underscores and ensure it starts with alphanumeric
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(source_id))
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Ensure it doesn't start or end with underscore
    sanitized = sanitized.strip('_')
    # Ensure it starts with alphanumeric
    if sanitized and not sanitized[0].isalnum():
        sanitized = 'task_' + sanitized
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
    if notebook_path and not notebook_path.startswith(('/pipelines/', '/frameworks/', '/Workspace/')):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Using custom notebook path: {notebook_path}")
    return True

