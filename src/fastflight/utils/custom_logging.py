# The logging setup in this file uses structlog-based formatter (`ProcessorFormatter`) to render both `logging`
# and `structlog` log entries, and then send rendered log strings to `logging` handlers. See
# https://www.structlog.org/en/stable/standard-library.html#rendering-using-structlog-based-formatters-within-logging
import logging
import logging.config
from pathlib import Path
from typing import Literal

import structlog

shared_processors = [
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


def setup_logging(
    console_log_level: str | int = "DEBUG",
    log_file: None | Path | str = "app.log",
    file_log_level: str | int = "INFO",
    file_format: Literal["plain", "json"] = "plain",
):
    """
    Set up the logging configuration for the application.

    Args:
        console_log_level (str | int): The log level for the console handler.
        log_file (Path | str | None): The path to the log file. If None, no file handler will be created.
        file_log_level (str | int): The log level for the file handler if log_file is not None.
        file_format (str): The format to use for file logging. Options: "plain", "json".

    Returns:
        None

    This function configures the logging system using the structlog library. It sets up the processors, logger factory,
    and logging configuration. It also creates and configures the console and file handlers. The console handler logs
    to the console with colored output, while the file handler logs to the specified log file in plain text format. The
    log levels for both handlers are set based on the provided arguments. The root logger is configured to propagate
    logs to both the console and file handlers. Additionally, the uvicorn logger is configured to log at the INFO level
    and propagate logs to both handlers.

    Note:
        The `foreign_pre_chain` argument in the `ProcessorFormatter` is responsible for adding properties to events from
        the standard library. It should match the processors argument to `structlog.configure()` to ensure consistent
        output. The `ExtraAdder` processor adds extra attributes of LogRecord objects to the event dictionary, allowing
        values passed in the extra parameter of log methods to be included in the log output.

        The `structlog.dev.ConsoleRenderer` class is used to render log messages in colored or plain text format based on
        the `colors` parameter. The `structlog.stdlib.ProcessorFormatter` class is used to convert the event dictionary
        to data that can be processed by the formatter.

        The `logging.config.dictConfig` function is used to configure the logging system using a dictionary-based
        configuration. The configuration includes the formatters, handlers, and root logger configuration.

        The `Path(log_file).parent.mkdir(exist_ok=True, parents=True)` line creates the parent directory of the log file if
        it is not None and doesn't exist.
    """
    structlog.configure(
        processors=shared_processors  # type: ignore[arg-type]
        + [
            # This is needed to convert the event dict to data that can be processed by the `ProcessorFormatter`
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter
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
    # events from the standard library – in other words, those that do not originate from a structlog logger – and
    # which should in general match the processors argument to structlog.configure() so you get a consistent output.
    foreign_pre_chain = shared_processors + [
        # Add extra attributes of LogRecord objects to the event dictionary so that values passed in the extra
        # parameter of log methods pass through to log output.
        structlog.stdlib.ExtraAdder()
    ]

    root_logger = logging.getLogger()

    if log_file is not None:
        Path(log_file).parent.mkdir(exist_ok=True, parents=True)

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

    handlers = {
        "default": {"level": console_log_level, "class": "logging.StreamHandler", "formatter": "colored"},
        **file_handler,
    }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
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
    )
