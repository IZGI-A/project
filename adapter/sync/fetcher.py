"""Fetches data from the external bank service."""
import logging

import requests

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches credit and payment plan data from the external bank API."""

    def __init__(self, base_url: str, tenant_id: str):
        self.base_url = base_url.rstrip('/')
        self.tenant_id = tenant_id

    def fetch(self, loan_type: str, file_type: str) -> list:
        """
        Fetch data from external bank via direct Redis storage read.
        Memory efficient for large datasets — reads chunk by chunk.

        Args:
            loan_type: RETAIL or COMMERCIAL
            file_type: credit or payment_plan

        Returns:
            List of record dicts
        """
        from external_bank import storage

        records = []
        for chunk in storage.get_data_iter(self.tenant_id, loan_type, file_type):
            records.extend(chunk)

        logger.info(
            "Fetched %d %s records for %s/%s",
            len(records), file_type, self.tenant_id, loan_type,
        )
        return records

    def fetch_iter(self, loan_type: str, file_type: str):
        """
        Generator that yields chunks of records from storage.
        Truly memory efficient — only one chunk in memory at a time.

        Yields:
            Lists of record dicts (each list is one chunk)
        """
        from external_bank import storage

        total = 0
        for chunk in storage.get_data_iter(self.tenant_id, loan_type, file_type):
            total += len(chunk)
            yield chunk

        logger.info(
            "Fetched %d %s records (streaming) for %s/%s",
            total, file_type, self.tenant_id, loan_type,
        )

    def fetch_row_count(self, loan_type: str, file_type: str) -> int:
        """O(1) row count check without loading data."""
        from external_bank import storage
        return storage.get_row_count(self.tenant_id, loan_type, file_type)
