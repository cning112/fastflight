# The logging setup in this file provides flexible logging configuration with optional structlog support.
# When structlog is available, it uses advanced structured logging with processors and formatters.
# When structlog is not available, it falls back to standard library logging with similar functionality.
# See https://www.structlog.org/en/stable/standard-library.html#rendering-using-structlog-based-formatters-within-logging
import json
import logging
import logging.config
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

# Try to import structlog, fallback to basic logging if not available
# This allows the library to work without structlog as a hard dependency
try:
    import structlog

    HAS_STRUCTLOG = True
except ImportError:
    HAS_STRUCTLOG = False


def _get_shared_processors():
    """
    Get shared processors for structlog configuration.

    This function returns the list of processors used by structlog for consistent
    log formatting. If structlog is not available, returns an empty list.

    Returns:
        list: List of structlog processors, or empty list if structlog unavailable

    Note:
        These processors are only used when structlog is available and provide:
        - Log level filtering
        - Logger name addition
        - Timestamp formatting in ISO 8601
        - Stack trace rendering
        - Unicode decoding
        - Callsite parameter addition (filename, function, line number, etc.)
    """
    if not HAS_STRUCTLOG:
        return []

    return [
        # If log level is too low, abort pipeline and throw away log entry.
        structlog.stdlib.filter_by_level,
        # Add the name of the logger to event dict.
        structlog.stdlib.add_logger_name,
        # Add log level to event dict.
        structlog.stdlib.add_log_level,
        # Perform %-style formatting.
        structlog.stdlib.PositionalArgumentsFormatter(),
        # Add a timestamp in ISO 8601 format.
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # If the "stack_info" key in the event dict is true, remove it and
        # render the current stack trace in the "stack" key.
        structlog.processors.StackInfoRenderer(),
        # If some value is in bytes, decode it to a Unicode str.
        structlog.processors.UnicodeDecoder(),
        # Add callsite parameters.
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
                structlog.processors.CallsiteParameter.THREAD_NAME,
                structlog.processors.CallsiteParameter.PROCESS_NAME,
            }
        ),
    ]


class JSONFormatter(logging.Formatter):
    """
    Simple JSON formatter for basic logging when structlog is not available.

    This formatter is only used by the basic logging configuration (_setup_basic_logging)
    when structlog is not installed. It provides JSON-formatted log output similar to
    structlog's JSONRenderer but using only the standard library.

    The JSON output includes:
    - ISO 8601 timestamp with 'Z' suffix (UTC)
    - Log level and logger name
    - Message content
    - Source code location (filename, function, line number)
    - Thread and process information
    - Exception traceback (if present)

    Note:
        This is a fallback implementation. When structlog is available, the system
        uses structlog's more sophisticated JSONRenderer instead.
    """

    def format(self, record):
        """
        Format log record as JSON string.

        Args:
            record (logging.LogRecord): The log record to format

        Returns:
            str: JSON-formatted log entry

        Note:
            The timestamp includes 'Z' suffix to indicate UTC timezone,
            matching structlog's default timestamp format.
        """
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.threadName,
            "process": record.processName,
        }

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, sort_keys=True)


def setup_logging(
    console_log_level: str | int = "DEBUG",
    log_file: None | Path | str = "app.log",
    file_log_level: str | int = "INFO",
    file_format: Literal["plain", "json"] = "plain",
    force_basic: bool = False,
):
    """
    Set up the logging configuration for the application.

    This function provides flexible logging setup with automatic fallback support.
    It detects whether structlog is available and configures the appropriate logging
    system accordingly.

    Behavior:
    - If structlog is available and force_basic=False: Uses advanced structlog configuration
    - If structlog is not available or force_basic=True: Uses standard library logging

    Both configurations provide:
    - Console and optional file logging
    - Multiple output formats (plain text, colored, JSON)
    - Log rotation for file output
    - Detailed context information (filename, function, line number)

    Args:
        console_log_level (str | int): The log level for the console handler (default: "DEBUG").
            Accepts standard logging levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
        log_file (Path | str | None): The path to the log file (default: "app.log").
            If None, no file handler will be created. Parent directories are created automatically.
        file_log_level (str | int): The log level for the file handler (default: "INFO").
            Only used when log_file is not None.
        file_format (Literal["plain", "json"]): The format for file logging (default: "plain").
            - "plain": Human-readable text format
            - "json": Structured JSON format for log aggregation systems
        force_basic (bool): Force use of basic logging even if structlog is available (default: False).
            Useful for testing fallback behavior or environments where structlog causes issues.

    Returns:
        None

    Examples:
        Basic setup with structlog auto-detection:
        >>> setup_logging()

        Force basic logging mode:
        >>> setup_logging(force_basic=True)

        Configure for production with JSON file output:
        >>> setup_logging(
        ...     console_log_level="INFO",
        ...     log_file="/var/log/app.log",
        ...     file_format="json"
        ... )

        Disable file logging:
        >>> setup_logging(log_file=None)

    Note:
        The function automatically creates parent directories for log files if they don't exist.
        File logging uses TimedRotatingFileHandler with daily rotation and 7-day retention.

        When structlog is available, the configuration uses ProcessorFormatter to render
        both standard logging and structlog entries consistently. When structlog is not
        available, the fallback provides similar functionality using only standard library.
    """
    if HAS_STRUCTLOG and not force_basic:
        _setup_structlog_logging(console_log_level, log_file, file_log_level, file_format)
    else:
        _setup_basic_logging(console_log_level, log_file, file_log_level, file_format)


def _setup_structlog_logging(
    console_log_level: str | int,
    log_file: None | Path | str,
    file_log_level: str | int,
    file_format: Literal["plain", "json"],
):
    """
    Setup logging using structlog (original advanced implementation).

    This function configures the advanced structlog-based logging system with:
    - Structured log processing pipeline
    - ProcessorFormatter integration with standard logging
    - Rich console output with colors
    - Consistent JSON formatting
    - Integration between structlog and standard library loggers

    The configuration uses structlog's ProcessorFormatter to render both structlog
    and standard library log entries, ensuring consistent output format regardless
    of which logging interface is used.

    Args:
        console_log_level (str | int): Console log level
        log_file (None | Path | str): Path to log file, None to disable file logging
        file_log_level (str | int): File log level
        file_format (Literal["plain", "json"]): File output format

    Note:
        This function is only called when structlog is available (HAS_STRUCTLOG=True).
        It preserves the original sophisticated logging configuration that was in the
        previous version of this file.

        The 'foreign_pre_chain' processors handle log entries from standard library
        loggers, ensuring they receive the same processing as structlog entries.
    """
    shared_processors = _get_shared_processors()

    # Configure structlog with processor pipeline
    structlog.configure(
        processors=[
            *shared_processors,
            # This is needed to convert the event dict to data that can be processed by the `ProcessorFormatter`
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        # `logger_factory` is used to create wrapped loggers that are used for
        # OUTPUT. This one returns a `logging.Logger`. The final value (a JSON
        # string) from the final processor (`JSONRenderer`) will be passed to
        # the method of the same name as that you've called on the bound logger.
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Effectively freeze configuration after creating the first bound logger.
        cache_logger_on_first_use=True,
    )

    # The `ProcessorFormatter` has a `foreign_pre_chain` argument which is responsible for adding properties to
    # events from the standard library - in other words, those that do not originate from a structlog logger - and
    # which should in general match the processors argument to structlog.configure() so you get a consistent output.
    foreign_pre_chain = [
        *shared_processors,
        # Add extra attributes of LogRecord objects to the event dictionary so that values passed in the extra
        # parameter of log methods pass through to log output.
        structlog.stdlib.ExtraAdder(),
    ]

    root_logger = logging.getLogger()

    # Create log file directory if needed
    if log_file is not None:
        Path(log_file).parent.mkdir(exist_ok=True, parents=True)

    # Configure file handler (empty dict if no file logging)
    file_handler = (
        {}
        if log_file is None
        else {
            "file": {
                "level": file_log_level,
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": str(log_file),
                "formatter": file_format,
                "when": "midnight",
                "interval": 1,
                "backupCount": 7,
                "encoding": "utf-8",
            }
        }
    )

    # Always include console handler, optionally include file handler
    handlers: dict[str, Any] = {
        "default": {"level": console_log_level, "class": "logging.StreamHandler", "formatter": "colored"},
        **file_handler,
    }

    # Configure logging with structlog-based formatters
    # Using explicit type annotation to satisfy mypy
    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            # Plain text formatter for file output (no colors)
            "plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": [
                    # If the "exc_info" key in the event dict is either true or a
                    # sys.exc_info() tuple, remove "exc_info" and render the exception
                    # with traceback into the "exception" key.
                    structlog.processors.format_exc_info,
                    # Remove _record & _from_structlog from the event dict.
                    structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                    structlog.dev.ConsoleRenderer(colors=False),
                ],
                "foreign_pre_chain": foreign_pre_chain,
                "logger": root_logger,
            },
            # Colored formatter for console output
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": [
                    # If the "exc_info" key in the event dict is either true or a
                    # sys.exc_info() tuple, remove "exc_info" and render the exception
                    # with traceback into the "exception" key.
                    structlog.processors.format_exc_info,
                    # Remove _record & _from_structlog from the event dict.
                    structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                    structlog.dev.ConsoleRenderer(colors=True),
                ],
                "foreign_pre_chain": foreign_pre_chain,
                "logger": root_logger,
            },
            # JSON formatter for structured logging
            "json": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": [
                    structlog.processors.format_exc_info,
                    structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                    structlog.processors.JSONRenderer(sort_keys=True),
                ],
                "foreign_pre_chain": foreign_pre_chain,
                "logger": root_logger,
            },
        },
        "handlers": handlers,
        "root": {"handlers": list(handlers.keys()), "level": "DEBUG"},
    }
    logging.config.dictConfig(config)


def _setup_basic_logging(
    console_log_level: str | int,
    log_file: None | Path | str,
    file_log_level: str | int,
    file_format: Literal["plain", "json"],
):
    """
    Setup basic logging using standard library only (fallback implementation).

    This function provides a fallback logging configuration when structlog is not available.
    It uses only Python's standard library to provide similar functionality to the structlog
    configuration, including:

    - Console and file logging with rotation
    - Plain text and JSON output formats
    - Detailed log messages with context information
    - Automatic log file directory creation

    The JSON formatter used here is the custom JSONFormatter class defined in this module,
    which provides JSON output similar to structlog's JSONRenderer but without the
    advanced processor pipeline.

    Args:
        console_log_level (str | int): Console log level
        log_file (None | Path | str): Path to log file, None to disable file logging
        file_log_level (str | int): File log level
        file_format (Literal["plain", "json"]): File output format

    Note:
        This function is only called when structlog is not available (HAS_STRUCTLOG=False)
        or when force_basic=True is used in setup_logging().

        The JSON formatter referenced here is "fastflight.utils.custom_logging.JSONFormatter",
        which is the JSONFormatter class defined above in this same module.

        While this provides good functionality, the structlog configuration offers more
        advanced features like structured data handling and processor pipelines.
    """

    # Create log file directory if needed
    if log_file is not None:
        Path(log_file).parent.mkdir(exist_ok=True, parents=True)

    # Define formatters for different output styles
    # Note: The JSON formatter references our custom JSONFormatter class
    # Using Dict[str, Any] to satisfy mypy type checking for logging.config.dictConfig
    formatters: dict[str, Any] = {
        "plain": {
            "format": "%(asctime)s [%(levelname)8s] %(name)s: %(message)s (%(filename)s:%(funcName)s:%(lineno)d)",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
        "colored": {
            # Note: This is the same as plain since standard logging doesn't support colors natively
            # For true colored output, libraries like colorlog could be added as an optional dependency
            "format": "%(asctime)s [%(levelname)8s] %(name)s: %(message)s (%(filename)s:%(funcName)s:%(lineno)d)",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
        "json": {
            # This references the JSONFormatter class defined above
            # It provides JSON output when structlog is not available
            "()": "fastflight.utils.custom_logging.JSONFormatter"
        },
    }

    # Define handlers - always include console, optionally include file
    # Using Dict[str, Any] to satisfy mypy type checking
    handlers: dict[str, Any] = {
        "default": {"level": console_log_level, "class": "logging.StreamHandler", "formatter": "colored"}
    }

    # Add file handler if log file is specified
    if log_file is not None:
        handlers["file"] = {
            "level": file_log_level,
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": str(log_file),
            "formatter": file_format,  # Uses the format specified by user (plain or json)
            "when": "midnight",  # Rotate daily at midnight
            "interval": 1,
            "backupCount": 7,  # Keep 7 days of logs
            "encoding": "utf-8",
        }

    # Apply the logging configuration
    # Using explicit type annotation to satisfy mypy
    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "handlers": handlers,
        "root": {
            "handlers": list(handlers.keys()),
            "level": "DEBUG",  # Root logger level - individual handlers filter further
        },
    }
    logging.config.dictConfig(config)


def get_logger(name: str):
    """
    Get a logger instance that works with both structlog and standard logging.

    This function provides a unified interface for obtaining loggers regardless of whether
    structlog is available. It automatically returns the appropriate logger type:

    - If structlog is available: Returns a structlog logger with full structured logging capabilities
    - If structlog is not available: Returns a standard library logger

    This allows application code to use a consistent API without needing to check whether
    structlog is installed.

    Args:
        name (str): Logger name, typically __name__ from the calling module

    Returns:
        Logger instance: Either structlog.BoundLogger or logging.Logger

    Examples:
        Recommended usage in application modules:
        >>> from fastflight.utils.custom_logging import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("This works with both structlog and standard logging")

        With structured data (works best with structlog, but gracefully degrades):
        >>> logger.info("User logged in", user_id=123, session_id="abc-def")

    Note:
        When using structlog, you can pass structured data as keyword arguments
        which will be properly formatted in JSON output. With standard logging,
        these will be ignored but the message will still be logged.

        For maximum compatibility, prefer simple string messages when not specifically
        targeting structured logging scenarios.
    """
    if HAS_STRUCTLOG:
        return structlog.get_logger(name)
    else:
        return logging.getLogger(name)
