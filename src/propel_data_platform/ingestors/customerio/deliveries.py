from datetime import datetime, timedelta
from functools import partial

from pyspark.sql import Row
from pyspark.sql.functions import input_file_name, udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from propel_data_platform.ingestors.customerio.base import (
    BaseCustomerioIngestor,
    _get_blob_path_from_wasbs_path,
    _get_wasbs_path,
)

target_deliveries_schema = StructType(
    [
        StructField("delivery_id", StringType(), False),
        StructField("internal_customer_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("delivery_type", StringType(), False),
        StructField("campaign_id", IntegerType(), True),
        StructField("action_id", IntegerType(), True),
        StructField("newsletter_id", IntegerType(), True),
        StructField("content_id", IntegerType(), True),
        StructField("trigger_id", IntegerType(), True),
        StructField("transactional_message_id", IntegerType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("inserted_at", TimestampType(), False),
    ]
)


class CustomerioDeliveriesIngestor(BaseCustomerioIngestor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")
        self.container_prefix += "/deliveries"

    def run(self):
        self._update_parquet_metadata(file_type="deliveries", time_cols=["created_at"])

        # Note: Similar to Mixpanel, we process data of run date - 1 day, to avoid incomplete data of run date.

        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        inserted_at_map = {
            row.file_name: row.inserted_at
            for row in self.spark.sql(
                f"""
                    SELECT file_name, inserted_at FROM {self.environment}.{self.client_name}.{self.processed_files_table}_view
                    WHERE (type = 'deliveries')
                    AND NOT(date(max_timestamp) < '{start_date}' OR '{end_date}' < date(min_timestamp))
                """
            ).collect()
        }
        files_to_be_processed = list(inserted_at_map.keys())

        def _process_file(_start_date, _end_date, row):
            created_at = row["created_at"].strftime("%Y-%m-%d")
            if created_at < _start_date or created_at > _end_date:
                return []

            processed_row = {
                "delivery_id": row["delivery_id"],
                "internal_customer_id": row["internal_customer_id"],
                "event_id": row["event_id"],
                "delivery_type": row["delivery_type"],
                "campaign_id": row["campaign_id"],
                "action_id": row["action_id"],
                "newsletter_id": row["newsletter_id"],
                "content_id": row["content_id"],
                "trigger_id": row["trigger_id"],
                "transactional_message_id": row["transactional_message_id"],
                "created_at": row["created_at"],
                "inserted_at": row["inserted_at"],
            }
            return [Row(**processed_row)]

        get_wasbs_path = partial(
            _get_wasbs_path, self.azure_storage_connection_string, self.client_name
        )
        get_blob_path_from_wasbs_path = partial(
            _get_blob_path_from_wasbs_path,
            self.azure_storage_connection_string,
            self.client_name,
        )
        process_file = partial(_process_file, start_date, end_date)
        get_inserted_at_udf = udf(
            lambda file_name: inserted_at_map.get(
                get_blob_path_from_wasbs_path(file_name)
            ),
            TimestampType(),
        )
        if len(files_to_be_processed) == 0:
            return
        results = (
            self.spark.read.parquet(*map(get_wasbs_path, files_to_be_processed))
            .withColumn("inserted_at", get_inserted_at_udf(input_file_name()))
            .rdd.flatMap(lambda row: process_file(row))
        )
        results = self.spark.createDataFrame(results, schema=target_deliveries_schema)
        results.write.mode("append").saveAsTable(
            f"{self.environment}.{self.client_name}.{self.destination_table}"
        )
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY inserted_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.{self.destination_table}
            )
            SELECT * FROM ranked_rows WHERE row_rank = 1;
        """
        )
