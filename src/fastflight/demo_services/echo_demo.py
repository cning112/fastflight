import logging
import time
from collections.abc import AsyncIterator, Iterable

import pyarrow as pa

from fastflight.core.base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


class EchoParams(BaseParams):
    """
    Parameters for echo service that returns a single row with the provided message.
    """

    message: str = "Hello FastFlight!"


class EchoDataService(BaseDataService[EchoParams]):
    """
    A simple echo service that returns a single row table with the provided message.
    Useful for testing, debugging, and demonstrating FastFlight functionality.
    """

    def get_batches(self, params: EchoParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            logger.info(f"SYNC: Processing echo request with message: '{params.message}'")

            # Create single row table
            table = self._create_echo_table(params.message)

            # Convert to batches
            yield from table.to_batches(max_chunksize=batch_size or 10000)

            logger.debug("SYNC: Successfully yielded 1 row")

        except Exception as e:
            logger.error(f"SYNC: Service error: {e}", exc_info=True)
            raise

    async def aget_batches(self, params: EchoParams, batch_size: int | None = None) -> AsyncIterator[pa.RecordBatch]:
        logger.info(f"ASYNC: Processing echo request with message: '{params.message}'")

        try:
            # Create table (fast operation, no need for thread pool)
            table = self._create_echo_table(params.message)

            # Convert to batches
            batches = table.to_batches(max_chunksize=batch_size or 10000)

            for batch in batches:
                yield batch

            logger.debug("ASYNC: Successfully yielded 1 row")

        except Exception as e:
            logger.error(f"ASYNC: Service error: {e}", exc_info=True)
            raise

    def _create_echo_table(self, message: str) -> pa.Table:
        """Create a single row table with the echo message."""
        data = {"message": [message], "timestamp": [int(time.time())], "service": ["echo_service"]}
        return pa.Table.from_pydict(data)


if __name__ == "__main__":
    echo_service = EchoDataService()
    bathes = echo_service.get_batches(EchoParams())
    table = pa.Table.from_batches(bathes)
    print(table)  # noqa: T201
