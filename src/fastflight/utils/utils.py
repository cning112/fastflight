import asyncio
import functools
import logging

logger = logging.getLogger(__name__)


def debuggable(func):
    """A decorator to enable GUI (i.e. PyCharm) debugging in the
    decorated Arrow Flight RPC Server function.

    See: https://github.com/apache/arrow/issues/36844
    for more details...
    """

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        try:
            import pydevd

            pydevd.connected = True
            pydevd.settrace(suspend=False)
        except ImportError:
            # Not running in debugger
            pass
        value = func(*args, **kwargs)
        return value

    return wrapper_decorator


class EventLoopContext:
    """
    A context manager that creates and close an event loop
    """

    def __enter__(self):
        try:
            # from python 3.10, asyncio.get_event_loop() raises RuntimeError if there is no running event loop
            self.loop = asyncio.get_event_loop()

            if self.loop.is_closed():
                raise RuntimeError("Event loop is closed")

            # An event loop automatically starts when running in jupyter notebook a GUI framework (Pycharm) or a web framework (FastAPI)
            if self.loop.is_running():
                raise RuntimeError("Event loop is already running")

        except RuntimeError as e:
            logger.debug("Need to create a new event loop: %s", e)
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.new_loop = True
        else:
            self.new_loop = False
        return self.loop

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.new_loop:
            self.loop.close()
