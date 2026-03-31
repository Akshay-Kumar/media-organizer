import logging
import logging.config
from pathlib import Path
import yaml
import os
from typing import Optional, Dict, Any

def add_pid_to_log_records():
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.pid = os.getpid()
        return record

    logging.setLogRecordFactory(record_factory)


def setup_logging(
        config_path: str = 'config/logging.conf',
        default_level: int = logging.INFO,
        env_key: str = 'LOG_CFG'
) -> bool:
    """
    Setup logging configuration from YAML file

    Args:
        config_path: Path to logging configuration file
        default_level: Default logging level if config file not found
        env_key: Environment variable that can override config path

    Returns:
        bool: True if configuration was loaded successfully
    """
    # Check if environment variable overrides config path
    config_path = os.getenv(env_key, config_path)
    config_file = Path(config_path)

    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    if config_file.exists():
        try:
            with open(config_file, 'rt', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # Ensure log directories exist for file handlers
            if config and 'handlers' in config:
                for handler_name, handler_config in config['handlers'].items():
                    if 'filename' in handler_config:
                        handler_file = Path(handler_config['filename'])
                        handler_file.parent.mkdir(parents=True, exist_ok=True)

            logging.config.dictConfig(config)
            add_pid_to_log_records()
            old_factory = logging.getLogRecordFactory()

            def record_factory(*args, **kwargs):
                record = old_factory(*args, **kwargs)
                record.pid = os.getpid()
                return record

            logging.setLogRecordFactory(record_factory)
            logger = logging.getLogger(__name__)
            logger.info(f"Logging configured successfully from {config_path}")
            return True

        except Exception as e:
            print(f"Error loading logging configuration: {e}")
            # Fall back to basic configuration
            logging.basicConfig(
                level=default_level,
                format='%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            add_pid_to_log_records()
            logging.error(f"Failed to load logging config: {e}")
            return False
    else:
        # Use basic configuration if config file doesn't exist
        logging.basicConfig(
            level=default_level,
            format='%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        add_pid_to_log_records()
        logging.warning(f"Logging config file not found: {config_path}. Using basic configuration.")
        return False


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get a logger with optional level setting

    Args:
        name: Logger name
        level: Optional logging level to set

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger


def set_log_level(level: str) -> None:
    """
    Set logging level for all loggers

    Args:
        level: Logging level as string ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    """
    level_num = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger().setLevel(level_num)

    # Also set level for common loggers
    for logger_name in ['core', 'utils', '__main__']:
        logging.getLogger(logger_name).setLevel(level_num)


def create_file_handler(
        filename: str,
        level: int = logging.DEBUG,
        max_bytes: int = 104857600,
        backup_count: int = 5,
        formatter: Optional[logging.Formatter] = None
) -> logging.Handler:
    """
    Create a rotating file handler

    Args:
        filename: Log file path
        level: Logging level
        max_bytes: Maximum file size before rotation
        backup_count: Number of backup files to keep
        formatter: Optional formatter

    Returns:
        logging.Handler: Configured file handler
    """
    from logging.handlers import RotatingFileHandler

    # Ensure directory exists
    log_file = Path(filename)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        filename=filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    handler.setLevel(level)

    if formatter is None:
        formatter = logging.Formatter(
            '%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    handler.setFormatter(formatter)

    return handler


def create_console_handler(
        level: int = logging.INFO,
        formatter: Optional[logging.Formatter] = None
) -> logging.Handler:
    """
    Create a console handler

    Args:
        level: Logging level
        formatter: Optional formatter

    Returns:
        logging.Handler: Configured console handler
    """
    handler = logging.StreamHandler()
    handler.setLevel(level)

    if formatter is None:
        formatter = logging.Formatter(
            '%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    handler.setFormatter(formatter)

    return handler


def add_handler_to_logger(
        logger_name: str,
        handler: logging.Handler,
        level: Optional[int] = None
) -> None:
    """
    Add a handler to a specific logger

    Args:
        logger_name: Name of the logger to add handler to
        handler: Handler to add
        level: Optional logging level to set for the logger
    """
    logger = logging.getLogger(logger_name)
    if level is not None:
        logger.setLevel(level)
    logger.addHandler(handler)


def get_logging_config() -> Dict[str, Any]:
    """
    Get current logging configuration as dictionary

    Returns:
        Dict[str, Any]: Current logging configuration
    """
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {},
        'handlers': {},
        'loggers': {},
        'root': {}
    }

    # This is a simplified representation
    # Getting actual config would require more complex introspection
    return config


def shutdown_logging() -> None:
    """Properly shutdown logging system"""
    logging.shutdown()


# Example of programmatic configuration if config file is not available
def setup_basic_logging(level: int = logging.INFO) -> None:
    """
    Setup basic logging configuration programmatically

    Args:
        level: Logging level
    """
    # Create logs directory
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    # Create formatters
    standard_formatter = logging.Formatter(
        '%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    detailed_formatter = logging.Formatter(
        '%(asctime)s - [PID:%(pid)s] - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Create handlers
    console_handler = create_console_handler(level, standard_formatter)
    file_handler = create_file_handler(
        'logs/media_organizer.log',
        logging.DEBUG,
        formatter=detailed_formatter
    )
    error_handler = create_file_handler(
        'logs/errors.log',
        logging.ERROR,
        formatter=detailed_formatter
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)

    # Configure specific loggers
    core_logger = logging.getLogger('core')
    core_logger.setLevel(logging.DEBUG)

    utils_logger = logging.getLogger('utils')
    utils_logger.setLevel(logging.DEBUG)

    logging.info("Basic logging configuration applied programmatically")