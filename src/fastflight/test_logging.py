import logging
from src.fastflight.config import logging_settings
from src.fastflight.utils.custom_logging import setup_logging

def run_logging_test():
    # Override settings for this test
    logging_settings.log_format = "json"
    logging_settings.log_level = "DEBUG" # Ensure DEBUG is effective

    # Setup logging using the overridden settings
    # The setup_logging function in custom_logging.py should use these global settings
    setup_logging(
        console_log_level=logging_settings.log_level.upper(), # Pass directly to ensure it overrides defaults
        file_format=logging_settings.log_format # type: ignore
    )

    logger = logging.getLogger("my_test_logger")

    logger.debug("This is a debug message from the test script.", extra={"key1": "value1", "num_key": 123})
    logger.info("This is an info message from the test script.", extra={"complex_key": {"k": "v"}})
    logger.warning("This is a warning from the test script.")
    logger.error("This is an error message from the test script.")

    try:
        x = 1 / 0
    except ZeroDivisionError as e:
        # Log the exception info
        logger.error("An exception occurred in the test script", exc_info=True, extra={"exception_test": True})
        # Also test logging the exception object directly (structlog might handle this)
        # logger.error("Logging exception object directly", exception=e) # structlog specific, might need different handling

    # Test a message that might use specific structlog features if it was a structlog logger
    # For stdlib logger with structlog backend, it will go through standard formatting
    # structlog_logger = structlog.get_logger("my_structlog_test_logger")
    # structlog_logger.info("A message with structlog specific field", structlog_field="structlog_value")

    print("Logging test finished. Check output above.")

if __name__ == "__main__":
    run_logging_test()
