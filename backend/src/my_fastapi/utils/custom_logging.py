import contextvars
import json
import logging
import logging.config
import sys
from contextlib import ContextDecorator
from pathlib import Path
from types import MappingProxyType
from typing import Literal

import yaml

from ..dependencies.settings import get_app_settings


class LoggingContext(ContextDecorator):
    __log_context = contextvars.ContextVar("__logging_context_var", default={})

    def __init__(self, **kwargs):
        self.extra = kwargs

    def __enter__(self):
        current_context = self.__get_current_context()
        self.token = self.__log_context.set({**current_context, **self.extra})

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__log_context.reset(self.token)

    @classmethod
    def __get_current_context(cls):
        return cls.__log_context.get()

    @classmethod
    def get_current_context(cls):
        # Return an immutable view of the current context
        return MappingProxyType(cls.__get_current_context())


_record_factory = logging.getLogRecordFactory()


def custom_log_record_factory(*args, **kwargs):
    record = _record_factory(*args, **kwargs)
    for k, v in LoggingContext.get_current_context().items():
        while hasattr(record, k):
            k = "extra_" + k
        setattr(record, k, v)
    return record


# Custom log formatter to handle the extra context
class CustomFormatter(logging.Formatter):
    def __init__(self, format_type: Literal["key_value", "json", "default"] = "default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format_type = format_type
        if self.format_type == "key_value":
            self._style = logging.PercentStyle(
                "timestamp=%(asctime)s name=%(name)s module=%(module)s level=%(levelname)s message=%(message)s lineno=%(lineno)d %(extra)s"
            )
        elif self.format_type == "default":
            self._style = logging.PercentStyle(
                "%(asctime)s - %(module)s:%(lineno)d - %(name)s - %(levelname)s - %(message)s %(extra)s"
            )

    def format(self, record):
        context = LoggingContext.get_current_context()
        if self.format_type == "json":
            log_record = {
                "timestamp": self.formatTime(record, self.datefmt),
                "name": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.module,
                "lineno": record.lineno,
                "extra": context,
            }
            if record.exc_info:
                log_record["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(log_record)
        else:
            extra = " ".join(f"{key}={value}" for key, value in context.items())
            if extra:
                extra = " - " + extra
            # Add the context attributes as a single 'extra' attribute
            record.extra = extra
            return super().format(record)


def setup_logging():
    logging.setLogRecordFactory(custom_log_record_factory)

    settings = get_app_settings()

    if settings.log_config_file and settings.log_file:
        Path(settings.log_file).parent.mkdir(parents=True, exist_ok=True)

        with open(settings.log_config_file, "r") as f:
            log_config_text = f.read()

        log_config_vars = {
            "LOG_FILE_NAME": settings.log_file,
            "CONSOLE_LOG_LEVEL": settings.console_log_level,
            "FILE_LOG_LEVEL": settings.file_log_level,
        }

        for key, value in log_config_vars.items():
            log_config_text = log_config_text.replace(f"${{{key}}}", str(value))

        log_config = yaml.safe_load(log_config_text)
        logging.config.dictConfig(log_config)

    else:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(CustomFormatter())
        logging.basicConfig(handlers=[handler], level=logging.DEBUG, force=True)
