import time
from functools import partial

from pyspark.sql.functions import (
    col,
    greatest,
    input_file_name,
    least,
    lit,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (
    udf,
)
from pyspark.sql.types import StringType, TimestampType

from propel_data_platform.ingestors.base import BaseIngestor


def _get_wasbs_path(azure_storage_connection_string, client_name, path):
    conn_parts = dict(
        part.split("=", 1)
        for part in azure_storage_connection_string.split(";")
        if part
    )
    account_name = conn_parts.get("AccountName")
    return f"wasbs://{client_name}@{account_name}.blob.core.windows.net/{path}"


def _get_blob_path_from_wasbs_path(
    azure_storage_connection_string, client_name, wasbs_path
):
    prefix = _get_wasbs_path(azure_storage_connection_string, client_name, "")
    return wasbs_path[len(prefix) :]


class BaseCustomerioIngestor(BaseIngestor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.container_prefix = f"https:/propeldata{self.environment}.blob.core.windows.net/{self.client_name}/{self.client_name}-data-platform"
        self.processed_files_table = kwargs.get("config", {}).get(
            "processed-files-table"
        )

    def _update_parquet_metadata(self, file_type, time_cols=[]):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.environment}.{self.client_name}.{self.processed_files_table} (
                file_name STRING,
                type STRING,
                min_timestamp TIMESTAMP,
                max_timestamp TIMESTAMP,
                inserted_at TIMESTAMP
            )
        """
        )
        MAX_RETRIES = 3
        RETRY_DELAY_SECONDS = 5
        for _ in range(1, MAX_RETRIES + 1):
            try:
                self.spark.sql(
                    f"""
                    CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.{self.processed_files_table}_view AS
                    WITH ranked_rows AS (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER (PARTITION BY file_name ORDER BY inserted_at DESC) AS row_rank
                        FROM {self.environment}.{self.client_name}.{self.processed_files_table}
                    )
                    SELECT * FROM ranked_rows WHERE row_rank = 1;
                """
                )
                break
            except Exception as e:
                if "TABLE_OR_VIEW_ALREADY_EXISTS" in str(e):
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    raise e
        else:
            raise RuntimeError(
                "Max retries reached. View creation failed due to repeated concurrent modifications."
            )

        all_parquet_files = self.container_client.list_blobs(
            name_starts_with=self.container_prefix
        )
        inserted_at_map = {blob.name: blob.last_modified for blob in all_parquet_files}
        all_parquet_files = list(inserted_at_map.keys())
        updated_files = [
            row.file_name
            for row in self.spark.sql(
                f"""
                SELECT file_name
                FROM {self.environment}.{self.client_name}.{self.processed_files_table}_view
                WHERE type = '{file_type}'
            """
            ).collect()
        ]
        to_be_processed_files = list(set(all_parquet_files) - set(updated_files))
        inserted_at_map = {
            file_name: inserted_at_map[file_name] for file_name in to_be_processed_files
        }

        get_wasbs_path = partial(
            _get_wasbs_path, self.azure_storage_connection_string, self.client_name
        )
        get_blob_path_from_wasbs_path = partial(
            _get_blob_path_from_wasbs_path,
            self.azure_storage_connection_string,
            self.client_name,
        )

        recover_path_udf = udf(get_blob_path_from_wasbs_path, StringType())
        get_inserted_at_udf = udf(
            lambda file_name: inserted_at_map.get(file_name), TimestampType()
        )

        if len(time_cols) > 1:
            least_col = least(*[col(time_col) for time_col in time_cols])
            greatest_col = greatest(*[col(time_col) for time_col in time_cols])
        else:
            least_col = col(time_cols[0])
            greatest_col = col(time_cols[0])

        if len(to_be_processed_files) == 0:
            return
        (
            self.spark.read.parquet(*map(get_wasbs_path, to_be_processed_files))
            .withColumn("file_name", recover_path_udf(input_file_name()))
            .withColumn("min_timestamp", least_col)
            .withColumn("max_timestamp", greatest_col)
            .groupBy("file_name")
            .agg(
                spark_min("min_timestamp").alias("min_timestamp"),
                spark_max("max_timestamp").alias("max_timestamp"),
            )
            .withColumn("type", lit(file_type))
            .withColumn("inserted_at", get_inserted_at_udf(col("file_name")))
            .write.mode("append")
            .saveAsTable(
                f"{self.environment}.{self.client_name}.{self.processed_files_table}"
            )
        )
