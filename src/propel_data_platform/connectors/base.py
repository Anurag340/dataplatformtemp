import json
import logging
import requests
from requests.adapters import HTTPAdapter

from propel_data_platform.tasks.base import BaseTask
from propel_data_platform.utils.throttler import Throttler

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BaseConnector(BaseTask):

    TIMEOUT = (30, 300)
    CHUNK_SIZE = 1000
    RETRIES = 3
    CONCURRENCY = 10
    BLOB_WRITE_CONCURRENCY = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.chunk_size = kwargs.get("config", {}).get("chunk-size", self.CHUNK_SIZE)
        self.retries = kwargs.get("config", {}).get("retries", self.RETRIES)
        self.concurrency = kwargs.get("config", {}).get("concurrency", self.CONCURRENCY)
        self.redis_config = kwargs.get("config", {}).get("redis-config", {})

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = requests.Session()
            adapter = HTTPAdapter(pool_maxsize=self.concurrency)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
        return self._session

    @property
    def throttler(self):
        if not hasattr(self, "_throttler"):
            self._throttler = Throttler(
                redis_config=self.redis_config,
            )
        return self._throttler

    def write_to_blob(self, data, idx, end_idx):
        content = "\n".join([json.dumps(item) for item in data])
        blob_path = f"{self.run_id}/raw_data/{self.ENDPOINT}/{idx}-to-{end_idx}.jsonl"
        logger.info(f"Writing data chunk to blob: {blob_path}")
        logger.debug(f"Data chunk content: {content[:500]}...")  # Log first 500 chars
        blob_client = self.container_client.get_blob_client(blob_path)
        blob_client.upload_blob(
            content, overwrite=True, max_concurrency=self.BLOB_WRITE_CONCURRENCY
        )

    @property
    def throttled_fetch(self):
        return self.throttler.throttle(
            func=self.fetch,
        )

    @property
    def url(self):
        return f"{self.api_url}/{self.ENDPOINT.lstrip('/')}"

    def run(self):
        self.fetch_all()

    def fetch(self):
        raise NotImplementedError("Subclasses must implement fetch")

    def fetch_all(self):
        raise NotImplementedError("Subclasses must implement fetch_all")
