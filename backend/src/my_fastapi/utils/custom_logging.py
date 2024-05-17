import contextvars
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
    def format(self, record):
        # Add the context attributes as a single 'extra' attribute
        context = LoggingContext.get_current_context()
        record.extra = ", ".join(f"{key}={value}" for key, value in context.items())
        return super().format(record)
