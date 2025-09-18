import concurrent.futures
import time

from propel_data_platform.connectors.braze.base import BrazeBaseConnector


class BrazeCanvasDetailsConnector(BrazeBaseConnector):
    """Connector for fetching Braze canvas details data.
    https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_details
    """

    ENDPOINT = "canvas/details"
    RATE_LIMIT = {"window": 3600, "limit": 250000}

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.parse_canvas_ids(query=kwargs.get("config", {}).get("canvas-ids-query"))
        self.post_launch_draft_version = kwargs.get("config", {}).get(
            "post-launch-draft-version"
        )

    def parse_canvas_ids(self, query):
        query = query.format(environment=self.environment, client_name=self.client_name)
        self.canvas_ids = self.spark.sql(query).rdd.map(lambda row: row[0]).collect()

    def fetch(self, canvas_id, post_launch_draft_version=None):
        params = {"canvas_id": canvas_id}
        if post_launch_draft_version is not None:
            params["post_launch_draft_version"] = post_launch_draft_version

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }

        try:
            response = self.session.get(self.url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            response_json = response.json()
            if not response_json.get("message") == "success":
                raise Exception(f"Failed to fetch canvas details")
            canvas_data = {"data": response_json, "metadata": metadata}
            return True, canvas_data
        except:
            return False, params

    def fetch_all(self):
        idx, results, retry_list = 0, [], []
        params = [
            {
                "canvas_id": canvas_id,
                "post_launch_draft_version": self.post_launch_draft_version,
            }
            for canvas_id in self.canvas_ids
        ]

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:
            for _ in range(self.retries + 1):
                futures = [
                    executor.submit(self.throttled_fetch, **param) for param in params
                ]
                for future in concurrent.futures.as_completed(futures):
                    success, response = future.result()
                    if not success:
                        retry_list.append(response)
                        continue
                    response["metadata"]["run_id"] = self.run_id
                    results.append(response)
                    while len(results) >= self.chunk_size:
                        chunk, results = (
                            results[: self.chunk_size],
                            results[self.chunk_size :],
                        )
                        self.write_to_blob(chunk, idx, idx + self.chunk_size)
                        idx += self.chunk_size
                params, retry_list = retry_list, []

        if len(results) > 0:
            self.write_to_blob(results, idx, idx + len(results))

        if len(retry_list) > 0:
            raise Exception("Failed to fetch data for some canvases")
