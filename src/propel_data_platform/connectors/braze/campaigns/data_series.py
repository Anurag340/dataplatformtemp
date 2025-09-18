import concurrent.futures
import datetime
import json
import os
import tempfile
import time

from propel_data_platform.connectors.braze.base import BrazeBaseConnector


class BrazeCampaignsDataSeriesConnector(BrazeBaseConnector):
    """Connector for fetching Braze campaign analytics data.
    https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_analytics
    """

    ENDPOINT = "campaigns/data_series"
    RATE_LIMIT = {"window": 60, "limit": 50000}

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.parse_campaign_ids(
            query=kwargs.get("config", {}).get("campaign-ids-query")
        )
        self.length = kwargs.get("config", {}).get("length")

    def parse_campaign_ids(self, query):
        query = query.format(environment=self.environment, client_name=self.client_name)
        self.campaign_ids = (
            self.spark.sql(query)
            .rdd.map(lambda row: (row[0], row[1], row[2]))
            .collect()
        )

    def fetch(self, temp_dir, campaign_id, length, ending_at=None):
        params = {
            "campaign_id": campaign_id,
            "length": length,
        }
        if ending_at is not None:
            params["ending_at"] = ending_at

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }
        try:
            response = self.session.get(self.url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            response_json = response.json()
            if not response_json.get("message") == "success":
                raise Exception(f"Failed to fetch campaign data series")
            campaign_data = {
                "data": response_json.get("data", {}),
                "metadata": metadata,
            }
            filename = (
                os.path.join(temp_dir, f"{campaign_id}-{ending_at}-{length}.json")
                .replace(":", "-")
                .replace("+", "-")
            )
            with open(filename, "w") as f:
                json.dump(campaign_data, f)
            return True, filename
        except:
            params["temp_dir"] = temp_dir
            return False, params

    def fetch_all(self):
        idx, results, retry_list, params = 0, [], [], []
        temp_dir = os.path.join(
            tempfile.gettempdir(), f"braze_campaigns_data_series_{self.run_id}"
        )
        os.makedirs(temp_dir, exist_ok=True)

        # Note: The api return data of N days before the ending_at, eg: if ending_at is 2025-06-22 and length is 3, it will return data of 2025-06-21, 2025-06-20, 2025-06-19.

        end_date = self.end_date
        for campaign_id, created_at, enabled in self.campaign_ids:
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
            for _ in range(0, total_length, 100):
                ending_at = f"{end_date}T00:00:00+00:00"
                length = min(
                    100,
                    (
                        datetime.datetime.strptime(end_date, "%Y-%m-%d")
                        - datetime.datetime.strptime(start_date, "%Y-%m-%d")
                    ).days,
                )
                params.append(
                    {
                        "temp_dir": temp_dir,
                        "campaign_id": campaign_id,
                        "ending_at": ending_at,
                        "length": length,
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
            raise Exception("Failed to fetch data for some campaigns")
