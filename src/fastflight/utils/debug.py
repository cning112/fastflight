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
            import pydevd  # type: ignore[import-not-found]

            pydevd.connected = True
            pydevd.settrace(suspend=False)
        except ImportError:
            # Not running in debugger
            pass
        value = func(*args, **kwargs)
        return value

    return wrapper_decorator
