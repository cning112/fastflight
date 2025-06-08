"""
Enhanced exception hierarchy for FastFlight client operations.

This module provides a comprehensive set of exceptions that allow clients to handle
different types of errors appropriately, enabling better error recovery and user experience.
"""

from typing import Any


class FastFlightError(Exception):
    """
    Base exception class for all FastFlight-related errors.

    This serves as the root of the exception hierarchy, allowing clients to catch
    all FastFlight-specific errors with a single exception type when needed.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class FastFlightConnectionError(FastFlightError):
    """
    Raised when connection to the Flight server fails.

    This includes network connectivity issues, server unavailability,
    and connection timeout scenarios.
    """

    pass


class FastFlightTimeoutError(FastFlightConnectionError):
    """
    Raised when operations exceed their configured timeout limits.

    This can occur during connection establishment, data retrieval,
    or other network operations that have time constraints.
    """

    def __init__(self, message: str, timeout_duration: float | None = None, details: dict[str, Any] | None = None):
        super().__init__(message, details)
        self.timeout_duration = timeout_duration


class FastFlightAuthenticationError(FastFlightError):
    """
    Raised when authentication with the Flight server fails.

    This includes invalid credentials, expired tokens, or insufficient permissions.
    """

    pass


class FastFlightServerError(FastFlightError):
    """
    Raised when the Flight server returns an error response.

    This encompasses various server-side errors that are not related to
    client configuration or network connectivity.
    """

    def __init__(self, message: str, server_error_code: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(message, details)
        self.server_error_code = server_error_code


class FastFlightDataServiceError(FastFlightServerError):
    """
    Raised when a data service on the server encounters an error.

    This includes issues with data source connectivity, query execution failures,
    or data processing errors within the registered data services.
    """

    def __init__(self, message: str, service_name: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(message, details=details)
        self.service_name = service_name


class FastFlightDataValidationError(FastFlightError):
    """
    Raised when data validation fails during parameter processing.

    This includes invalid parameter formats, missing required fields,
    or data that doesn't conform to expected schemas.
    """

    def __init__(
        self, message: str, validation_errors: dict[str, Any] | None = None, details: dict[str, Any] | None = None
    ):
        super().__init__(message, details)
        self.validation_errors = validation_errors or {}


class FastFlightSerializationError(FastFlightError):
    """
    Raised when serialization or deserialization of data fails.

    This includes JSON encoding/decoding errors, Arrow format issues,
    or other data transformation problems.
    """

    pass


class FastFlightResourceExhaustionError(FastFlightError):
    """
    Raised when system resources are exhausted.

    This includes scenarios where connection pools are full, memory limits
    are reached, or other resource constraints prevent operation completion.
    """

    def __init__(self, message: str, resource_type: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(message, details)
        self.resource_type = resource_type


class FastFlightCircuitOpenError(FastFlightError):
    """
    Raised when the circuit breaker is in open state.

    This indicates that the system has detected repeated failures and is
    temporarily preventing new requests to allow the service to recover.
    """

    def __init__(
        self, message: str, circuit_name: str, retry_after: float | None = None, details: dict[str, Any] | None = None
    ):
        super().__init__(message, details)
        self.circuit_name = circuit_name
        self.retry_after = retry_after


class FastFlightRetryExhaustedError(FastFlightError):
    """
    Raised when all retry attempts have been exhausted.

    This indicates that the operation has failed repeatedly and no more
    retry attempts will be made according to the configured retry policy.
    """

    def __init__(
        self,
        message: str,
        attempt_count: int,
        last_error: Exception | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message, details)
        self.attempt_count = attempt_count
        self.last_error = last_error
