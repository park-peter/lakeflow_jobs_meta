"""Main entry point for running Lakeflow Job Meta as a continuous monitoring task"""

import argparse
import logging
import sys

from lakeflow_job_meta.monitor import MetadataMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for running as a Lakeflow Job task.
    
    Accepts command line arguments:
    - --control-table: Delta table containing metadata (required)
    - --volume-path: Unity Catalog volume path for YAML files (optional)
    - --check-interval-seconds: How often to check for changes (default: 60)
    - --max-iterations: Maximum iterations before stopping (optional, None = infinite)
    """
    
    parser = argparse.ArgumentParser(
        description="Lakeflow Job Meta - Continuous Monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--control-table",
        required=True,
        help="Delta table containing metadata (required)"
    )
    
    parser.add_argument(
        "--volume-path",
        default=None,
        help="Unity Catalog volume path for YAML files (optional)"
    )
    
    parser.add_argument(
        "--check-interval-seconds",
        type=int,
        default=60,
        help="How often to check for changes in seconds (default: 60)"
    )
    
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum iterations before stopping (default: None = infinite)"
    )
    
    args = parser.parse_args()
    
    control_table = args.control_table
    volume_path = args.volume_path
    check_interval = args.check_interval_seconds
    max_iterations = args.max_iterations
    
    logger.info("=" * 60)
    logger.info("Lakeflow Job Meta - Continuous Monitoring")
    logger.info("=" * 60)
    logger.info(f"Control Table: {control_table}")
    logger.info(f"Volume Path: {volume_path or 'Not configured'}")
    logger.info(f"Check Interval: {check_interval} seconds")
    logger.info(f"Max Iterations: {max_iterations or 'Infinite'}")
    logger.info("=" * 60)
    
    # Create monitor
    monitor = MetadataMonitor(
        control_table=control_table,
        check_interval_seconds=check_interval,
        volume_path=volume_path,
        auto_update_jobs=True
    )
    
    # Run continuously
    try:
        monitor.run_continuous(max_iterations=max_iterations)
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in monitoring: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

