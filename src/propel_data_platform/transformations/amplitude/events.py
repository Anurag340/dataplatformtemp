from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql.functions import col, expr, unix_timestamp, when
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from propel_data_platform.transformations.amplitude.base import AmplitudeBaseTransform
from propel_data_platform.transformations.base import AIRBYTE_METADATA_COLUMNS


class AmplitudeEventsTransform(AmplitudeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = "amplitude_events"

    def update_events_list(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.environment}.{self.client_name}.amplitude_events_list (
                event_type string,
                event_type_id bigint PRIMARY KEY,
                is_active boolean
            )
        """
        )

        blob_path = f"amplitude/events.csv"
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
                FROM {self.environment}.{self.client_name}.amplitude_events_list
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
            final_events_spark_df.write.mode("overwrite").saveAsTable(
                f"{self.environment}.{self.client_name}.amplitude_events_list"
            )

        if self.postgres_table_exists("amplitude_events_list"):
            self.run_postgres_query(
                f"""
                TRUNCATE TABLE {self.client_name}.amplitude_events_list
                """
            )
        final_events_spark_df = self.spark.sql(
            f"""
            SELECT event_type, event_type_id, is_active FROM {self.environment}.{self.client_name}.amplitude_events_list
        """
        )
        self.insert_to_postgres(
            final_events_spark_df, "amplitude_events_list", mode="overwrite"
        )

    def _write_to_db(self):

        self.update_events_list()

        # TODO: Add primary key to postgres table and use it to deduplicate data.
        # currently, we are using run date for idempotency.

        # We process data of run date - 1 day, to avoid incomplete data of run date.
        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        self.run_postgres_query(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{self.client_name}' 
                    AND table_name = '{self.destination_table}'
                ) THEN
                    DELETE FROM {self.client_name}.{self.destination_table}
                    WHERE date(event_time_calculated) BETWEEN '{start_date}' AND '{end_date}';
                END IF;
            END$$;
        """
        )

        df = self.spark.sql(
            f"""
            SELECT *
            FROM (
                (SELECT 
                    ae.*, ael.event_type_id, 
                    timestamp(CASE 
                        WHEN (unix_timestamp(server_received_time) - unix_timestamp(client_upload_time)) < 60 
                        THEN unix_timestamp(client_event_time)
                        ELSE unix_timestamp(client_event_time) + unix_timestamp(server_received_time) - unix_timestamp(client_upload_time) 
                        END) as event_time_calculated
                FROM {self.environment}.{self.client_name}.amplitude_events ae
                INNER JOIN {self.environment}.{self.client_name}.amplitude_events_list ael
                ON ae.event_type = ael.event_type
                WHERE ae.user_id IS NOT NULL)
            )
            WHERE date(event_time_calculated) BETWEEN '{start_date}' AND '{end_date}'
        """
        )
        df = df.select(
            [col for col in df.columns if col not in AIRBYTE_METADATA_COLUMNS]
        )
        self.insert_to_postgres(df, self.destination_table)
        self.run_postgres_query(
            f"""
            CREATE INDEX IF NOT EXISTS idx_ei_{self.destination_table} ON {self.client_name}.{self.destination_table} (event_id);
            CREATE INDEX IF NOT EXISTS idx_ui_{self.destination_table} ON {self.client_name}.{self.destination_table} (user_id);
            CREATE INDEX IF NOT EXISTS idx_etc_{self.destination_table} ON {self.client_name}.{self.destination_table} USING BRIN(event_time_calculated);
        """
        )
