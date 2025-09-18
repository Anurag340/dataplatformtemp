import concurrent.futures
import copy
import json
import time

from propel_data_platform.connectors.braze.base import BrazeBaseConnector


class BrazeUserExportIdsConnector(BrazeBaseConnector):
    """Connector to export user profile by identifier
    https://www.braze.com/docs/api/endpoints/export/user_data/post_users_identifier
    """

    ENDPOINT = "users/export/ids"
    RATE_LIMIT = {"window": 60, "limit": 250}
    FIELDS_TO_EXPORT = [
        "external_id",
        "custom_attributes",
        "purchases",
        "dob",
        "country",
        "home_city",
        "language",
        "gender",
        "push_subscribe",
        "email_subscribe",
        "devices",
        "apps",
        "campaigns_received",
        "canvases_received",
    ]
    BATCH_SIZE = 50

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.parse_external_ids(
            query=kwargs.get("config", {}).get("external-ids-pg-query")
        )

    def parse_external_ids(self, query):
        query = query.format(client_name=self.client_name)
        external_ids_df = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", self.postgres_config["user"])
            .option("password", self.postgres_config["password"])
            .option("query", query)
            .load()
        )
        self.external_ids = [
            row.distinct_id for row in external_ids_df.select("distinct_id").collect()
        ]

    def fetch(
        self,
        fields_to_export,
        external_ids=None,
        user_aliases=None,
        device_ids=None,
        braze_ids=None,
        email_addresses=None,
        phone=None,
    ):
        params = {
            "fields_to_export": fields_to_export,
        }
        if external_ids is not None:
            params["external_ids"] = external_ids
        if user_aliases is not None:
            params["user_aliases"] = user_aliases
        if device_ids is not None:
            params["device_ids"] = device_ids
        if braze_ids is not None:
            params["braze_ids"] = braze_ids
        if email_addresses is not None:
            params["email_addresses"] = email_addresses
        if phone is not None:
            params["phone"] = phone

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }

        try:
            response = self.session.post(
                self.url, data=json.dumps(params), timeout=self.TIMEOUT
            )
            response.raise_for_status()
            response_json = response.json()
            if not response_json.get("message") == "success":
                raise Exception(f"Failed to fetch user profile data")
            user_profile_data = {
                "data": response_json.get("users", []),
                "metadata": metadata,
            }
            retry_params = copy.deepcopy(params)
            retry_params["external_ids"] = response_json.get("invalid_user_ids", [])
            return True, user_profile_data, retry_params
        except:
            return False, params, None

    def fetch_all(self):
        idx, results, retry_list = 0, [], []
        params = [
            {
                "fields_to_export": self.FIELDS_TO_EXPORT,
                "external_ids": self.external_ids[i : i + self.BATCH_SIZE],
            }
            for i in range(0, len(self.external_ids), self.BATCH_SIZE)
        ]

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:
            for _ in range(self.retries + 1):
                futures = [
                    executor.submit(self.throttled_fetch, **param) for param in params
                ]
                for future in concurrent.futures.as_completed(futures):
                    success, response, retry_params = future.result()
                    if not success:
                        retry_list.append(response)
                        continue
                    response["metadata"]["run_id"] = self.run_id
                    results.append(response)
                    if len(retry_params["external_ids"]) > 0:
                        retry_list.append(retry_params)
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
            raise Exception("Failed to fetch data for some external IDs")
