"""
Airbyte API utilities for triggering and monitoring syncs
"""
import requests
import time
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class AirbyteAPIClient:
    def __init__(self, base_url: str = "http://host.docker.internal:8006"):
        self.base_url = base_url
        self.api_version = "v1"
        self.session = requests.Session()

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/api/{self.api_version}/{endpoint}"

    def get_connection(self, connection_id: str) -> Dict:
        """Get connection details"""
        try:
            response = self.session.get(self._build_url(f"connections/{connection_id}"))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get connection {connection_id}: {e}")
            raise

    def trigger_sync(self, connection_id: str) -> str:
        """Trigger a sync for a connection and return job ID"""
        try:
            response = self.session.post(
                self._build_url("connections/sync"),
                json={"connectionId": connection_id},
                timeout=30
            )
            response.raise_for_status()
            job_id = response.json().get("job", {}).get("id")
            logger.info(f"Triggered sync for connection {connection_id}, job_id: {job_id}")
            return job_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to trigger sync for connection {connection_id}: {e}")
            raise

    def get_job_status(self, job_id: str) -> Dict:
        """Get the current status of a job"""
        try:
            response = self.session.get(self._build_url(f"jobs/{job_id}"))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            raise

    def wait_for_sync(
        self, 
        job_id: str, 
        max_wait_minutes: int = 60,
        poll_interval_seconds: int = 10
    ) -> bool:
        """
        Poll job status until it completes or times out.
        Returns True if succeeded, raises exception if failed.
        """
        start_time = datetime.now()
        timeout = timedelta(minutes=max_wait_minutes)
        
        while True:
            elapsed = datetime.now() - start_time
            if elapsed > timeout:
                logger.error(f"Job {job_id} exceeded timeout of {max_wait_minutes} minutes")
                raise TimeoutError(f"Job {job_id} did not complete within {max_wait_minutes} minutes")
            
            try:
                status_response = self.get_job_status(job_id)
                job_info = status_response.get("job", {})
                status = job_info.get("status", "").lower()
                
                logger.info(f"Job {job_id} status: {status} (elapsed: {elapsed})")
                
                if status == "succeeded":
                    logger.info(f"Job {job_id} completed successfully")
                    return True
                elif status in ["failed", "cancelled"]:
                    raise RuntimeError(f"Job {job_id} {status}")
                elif status == "running":
                    logger.debug(f"Job {job_id} still running, waiting...")
                    time.sleep(poll_interval_seconds)
                else:
                    # pending, etc.
                    logger.debug(f"Job {job_id} in state {status}, waiting...")
                    time.sleep(poll_interval_seconds)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error polling job {job_id}: {e}")
                time.sleep(poll_interval_seconds)

    def list_connections(self) -> list:
        """List all connections"""
        try:
            response = self.session.get(self._build_url("connections"))
            response.raise_for_status()
            return response.json().get("connections", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list connections: {e}")
            raise
