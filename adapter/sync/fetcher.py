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
        Fetch data from external bank.

        Args:
            loan_type: RETAIL or COMMERCIAL
            file_type: credit or payment_plan

        Returns:
            List of record dicts
        """
        url = f"{self.base_url}/data/"
        params = {
            'tenant_id': self.tenant_id,
            'loan_type': loan_type,
            'file_type': file_type,
        }

        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            records = data.get('data', [])
            logger.info(
                "Fetched %d %s records for %s/%s",
                len(records), file_type, self.tenant_id, loan_type,
            )
            return records
        except requests.RequestException as e:
            logger.error(
                "Failed to fetch %s data for %s/%s: %s",
                file_type, self.tenant_id, loan_type, e,
            )
            raise
