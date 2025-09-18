import concurrent.futures
import datetime
import json
import os
import tempfile
import time

from propel_data_platform.connectors.braze.base import BrazeBaseConnector


class BrazeCanvasDataSeriesConnector(BrazeBaseConnector):
    """Connector for fetching Braze canvas analytics data.
    https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_analytics
    """

    ENDPOINT = "canvas/data_series"
    RATE_LIMIT = {"window": 3600, "limit": 250000}

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.parse_canvas_ids(query=kwargs.get("config", {}).get("canvas-ids-query"))
        self.length = kwargs.get("config", {}).get("length")
        self.starting_at = kwargs.get("config", {}).get("starting-at")
        self.include_variant_breakdown = kwargs.get("config", {}).get(
            "include-variant-breakdown"
        )
        self.include_step_breakdown = kwargs.get("config", {}).get(
            "include-step-breakdown"
        )
        self.include_deleted_step_data = kwargs.get("config", {}).get(
            "include-deleted-step-data"
        )

    def parse_canvas_ids(self, query):
        query = query.format(environment=self.environment, client_name=self.client_name)
        self.canvas_ids = (
            self.spark.sql(query)
            .rdd.map(lambda row: (row[0], row[1], row[2]))
            .collect()
        )

    def fetch(
        self,
        temp_dir,
        canvas_id,
        ending_at,
        starting_at=None,
        length=None,
        include_variant_breakdown=None,
        include_step_breakdown=None,
        include_deleted_step_data=None,
    ):
        params = {
            "canvas_id": canvas_id,
            "ending_at": ending_at,
        }
        if starting_at is not None:
            params["starting_at"] = starting_at
        if length is not None:
            params["length"] = length
        if include_variant_breakdown is not None:
            params["include_variant_breakdown"] = include_variant_breakdown
        if include_step_breakdown is not None:
            params["include_step_breakdown"] = include_step_breakdown
        if include_deleted_step_data is not None:
            params["include_deleted_step_data"] = include_deleted_step_data

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }

        try:
            response = self.session.get(self.url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            response_json = response.json()
            if not response_json.get("message") == "success":
                raise Exception(f"Failed to fetch canvas data series")
            canvas_data = response_json.get("data", {})
            if "metadata" in canvas_data:
                raise Exception("Key conflict: metadata key already exists in response")
            canvas_data["metadata"] = metadata
            filename = (
                os.path.join(
                    temp_dir,
                    f"{canvas_id}-{ending_at}-{starting_at}-{length}-{include_variant_breakdown}-{include_step_breakdown}-{include_deleted_step_data}.json",
                )
                .replace(":", "-")
                .replace("+", "-")
            )
            with open(filename, "w") as f:
                json.dump(canvas_data, f)
            return True, filename
        except:
            params["temp_dir"] = temp_dir
            return False, params

    def fetch_all(self):
        idx, results, retry_list, params = 0, [], [], []
        temp_dir = os.path.join(
            tempfile.gettempdir(), f"braze_canvas_data_series_{self.run_id}"
        )
        os.makedirs(temp_dir, exist_ok=True)

        # Note: The api return data of N days before the ending_at, eg: if ending_at is 2025-06-22 and length is 3, it will return data of 2025-06-21, 2025-06-20, 2025-06-19.

        end_date = self.end_date
        for canvas_id, created_at, enabled in self.canvas_ids:
            # if not enabled:
            #     continue
            if self.length is None:
                start_date = (
                    datetime.datetime.fromisoformat(created_at)
                    .astimezone(datetime.UTC)
                    .strftime("%Y-%m-%d")
                )
            else:
                start_date = (
                    datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
                    - datetime.timedelta(days=self.length)
                ).strftime("%Y-%m-%d")
            total_length = (
                datetime.datetime.strptime(end_date, "%Y-%m-%d")
                - datetime.datetime.strptime(start_date, "%Y-%m-%d")
            ).days
            for _ in range(0, total_length, 14):
                ending_at = f"{end_date}T00:00:00+00:00"
                length = min(
                    14,
                    (
                        datetime.datetime.strptime(end_date, "%Y-%m-%d")
                        - datetime.datetime.strptime(start_date, "%Y-%m-%d")
                    ).days,
                )
                params.append(
                    {
                        "temp_dir": temp_dir,
                        "canvas_id": canvas_id,
                        "ending_at": ending_at,
                        "length": length,
                        "include_variant_breakdown": self.include_variant_breakdown,
                        "include_step_breakdown": self.include_step_breakdown,
                        "include_deleted_step_data": self.include_deleted_step_data,
                    }
                )
                end_date = (
                    datetime.datetime.strptime(end_date, "%Y-%m-%d")
                    - datetime.timedelta(days=length)
                ).strftime("%Y-%m-%d")

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
                    with open(response, "r") as f:
                        result = json.load(f)
                    result["metadata"]["run_id"] = self.run_id
                    results.append(result)
                    os.remove(response)
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
