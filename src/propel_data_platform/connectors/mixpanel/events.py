import concurrent.futures
import datetime
import json
import os
import tempfile
import time

import pandas as pd
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from propel_data_platform.connectors.mixpanel.base import MixpanelBaseConnector
from propel_data_platform.utils.constants import Constants


class MixpanelEventsConnector(MixpanelBaseConnector):
    """Connector for fetching Mixpanel Events data.
    https://developer.mixpanel.com/reference/raw-event-export
    """

    ENDPOINT = "export"
    RATE_LIMIT = {"window": [3600, 1], "limit": [60, 3]}
    CONCURRENCY = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.limit = kwargs.get("config", {}).get("limit")
        self.where = kwargs.get("config", {}).get("where")
        self.time_in_ms = kwargs.get("config", {}).get("time-in-ms")
        offset = kwargs.get("config", {}).get(
            "offset"
        )  # for 30d run, offset start_date and end_date by 30 days
        if offset is not None:
            start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
            end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")

            end_date = end_date - datetime.timedelta(days=offset)
            end_date = min(end_date, start_date)
            start_date = start_date - datetime.timedelta(days=offset)

            self.start_date = start_date.strftime("%Y-%m-%d")
            self.end_date = end_date.strftime("%Y-%m-%d")

        self.update_events_list()
        self.parse_events()

    def update_events_list(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.environment}.{self.client_name}.mixpanel_events_list (
                event_type string,
                event_type_id bigint PRIMARY KEY,
                is_active boolean
            )"""
        )

        blob_path = f"mixpanel/events.csv"
        try:
            blob_exists = self.container_client.get_blob_client(blob_path).exists()
        except Exception as e:
            blob_exists = False

        if blob_exists:
            blob_df = (
                (
                    self.spark.read.csv(
                        self.get_wasbs_path(blob_path), header=True, inferSchema=True
                    )
                    .withColumnRenamed("event_name", "event_type")
                    .toPandas()
                )[["event_type"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            table_df = self.spark.sql(
                f"""
                SELECT distinct event_type, event_type_id
                FROM {self.environment}.{self.client_name}.mixpanel_events_list
            """
            ).toPandas()

            new_blob_events_df = blob_df[
                ~blob_df["event_type"].isin(table_df["event_type"].tolist())
            ].copy()
            new_events_count = len(new_blob_events_df)
            max_event_type_id = (
                1 if len(table_df) == 0 else max(table_df["event_type_id"])
            )
            new_blob_events_df["event_type_id"] = [
                i
                for i in range(
                    max_event_type_id + 1,
                    max_event_type_id + 1 + new_events_count,
                )
            ]
            new_blob_events_df["is_active"] = True

            overlap_events_df = table_df[
                table_df["event_type"].isin(blob_df["event_type"].tolist())
            ].copy()
            overlap_events_df["is_active"] = True

            remaining_table_events_df = table_df[
                ~table_df["event_type"].isin(blob_df["event_type"].tolist())
            ].copy()
            remaining_table_events_df["is_active"] = False
            final_events_df = pd.concat(
                [new_blob_events_df, overlap_events_df, remaining_table_events_df]
            )

            schema = StructType(
                [
                    StructField("event_type", StringType(), False),
                    StructField("event_type_id", LongType(), False),
                    StructField("is_active", BooleanType(), False),
                ]
            )

            final_events_spark_df = self.spark.createDataFrame(
                final_events_df, schema=schema
            )

            MAX_RETRIES = 3
            RETRY_DELAY_SECONDS = 5
            for _ in range(1, MAX_RETRIES + 1):
                try:
                    final_events_spark_df.write.mode("overwrite").saveAsTable(
                        f"{self.environment}.{self.client_name}.mixpanel_events_list"
                    )
                    break
                except Exception as e:
                    if "DELTA_CONCURRENT_APPEND" in str(e):
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        raise e
            else:
                raise RuntimeError(
                    "Max retries reached. Table write failed due to repeated concurrent modifications."
                )

        if self.postgres_table_exists("mixpanel_events_list"):
            self.run_postgres_query(
                f"""
                TRUNCATE TABLE {self.client_name}.mixpanel_events_list
                """
            )
        final_events_spark_df = self.spark.sql(
            f"""
            SELECT event_type, event_type_id, is_active FROM {self.environment}.{self.client_name}.mixpanel_events_list
        """
        )
        self.insert_to_postgres(final_events_spark_df, "mixpanel_events_list")

    def parse_events(self):
        event_types = (
            self.spark.sql(
                Constants.fetch_campaign_list_details(
                    self.environment, self.client_name
                )
            )
            .select("event_type")
            .collect()
        )
        event_types = [row["event_type"] for row in event_types]
        self.event = json.dumps(event_types)

    def fetch(
        self,
        temp_dir,
        from_date,
        to_date,
        project_id=None,
        limit=None,
        event=None,
        where=None,
        time_in_ms=None,
    ):

        params = {"from_date": from_date, "to_date": to_date}
        if project_id is not None:
            params["project_id"] = project_id
        if limit is not None:
            params["limit"] = limit
        if event is not None:
            params["event"] = event
        if where is not None:
            params["where"] = where
        if time_in_ms is not None:
            params["time_in_ms"] = time_in_ms

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }

        try:
            request_hash = hash(json.dumps(params))
            filename = os.path.join(
                temp_dir, f"{from_date}-to-{to_date}-{request_hash}.jsonl"
            )
            file_handler = open(filename, "w")
            with self.session.get(
                self.url, params=params, stream=True, timeout=self.TIMEOUT
            ) as response:
                response.raise_for_status()
                for line in response.iter_lines():
                    if line:
                        try:  # incase the line is a byte string
                            line = line.decode("utf-8")
                        except (UnicodeDecodeError, AttributeError):
                            continue
                        event = json.loads(line)
                        event["metadata"] = metadata
                        file_handler.write(json.dumps(event) + "\n")
        except:
            return False, params
        finally:
            try:
                file_handler.close()
            except:
                pass
            try:
                response.close()
            except:
                pass
        return True, filename

    def fetch_all(self):
        retry_list, params = [], []
        temp_dir = os.path.join(tempfile.gettempdir(), f"mixpanel_events_{self.run_id}")
        os.makedirs(temp_dir, exist_ok=True)

        # Note: Unlike Braze, Mixpanel end_date is inclusive.
        # By design, this workflow will only fetch data till run date - 1 day, to avoid incomplete data of run date.
        # Therefore, we fetch data from start_date - 1 day to end_date - 1 day.

        i = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        while i <= datetime.datetime.strptime(self.end_date, "%Y-%m-%d"):
            params.append(
                {
                    "from_date": (i - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                    "to_date": (i - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                    "project_id": self.project_id,
                    "limit": self.limit,
                    "event": self.event,
                    "where": self.where,
                    "time_in_ms": self.time_in_ms,
                }
            )
            i = i + datetime.timedelta(days=1)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:
            for _ in range(self.retries + 1):
                futures = [
                    executor.submit(self.throttled_fetch, temp_dir, **param)
                    for param in params
                ]
                for future in concurrent.futures.as_completed(futures):
                    success, response = future.result()
                    if not success:
                        retry_list.append(response)
                        continue
                    filename = os.path.basename(response)
                    blob_path = f"{self.run_id}/raw_data/{self.ENDPOINT}/{filename}"
                    with open(response, "rb") as f:
                        self.container_client.upload_blob(
                            name=blob_path,
                            data=f,
                            overwrite=True,
                            max_concurrency=self.BLOB_WRITE_CONCURRENCY,
                        )
                    os.remove(response)
                retry_list, params = [], retry_list

        if len(retry_list) > 0:
            raise Exception("Failed to fetch events data for some dates")
