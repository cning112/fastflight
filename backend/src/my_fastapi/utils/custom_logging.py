import contextvars
import json
import logging.config
from contextlib import ContextDecorator
from types import MappingProxyType


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


# Custom log formatter to handle the extra context
class CustomFormatter(logging.Formatter):
    def __init__(self, format_type="key_value", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format_type = format_type
        if self.format_type == "key_value":
            self._style = logging.PercentStyle(
                "timestamp=%(asctime)s name=%(name)s level=%(levelname)s message=%(message)s lineno=%(lineno)d %(extra)s"
            )

    def format(self, record):
        context = LoggingContext.get_current_context()
        if self.format_type == "json":
            log_record = {
                "timestamp": self.formatTime(record, self.datefmt),
                "name": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
                "lineno": record.lineno,
                "context": context,
            }
            if record.exc_info:
                log_record["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(log_record)
        else:
            extra = " ".join(f"{key}={value}" for key, value in context.items())
            # Add the context attributes as a single 'extra' attribute
            record.extra = extra
            return super().format(record)
