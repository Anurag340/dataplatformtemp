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

target_subjects_schema = StructType(
    [
        StructField("subject_name", StringType(), False),
        StructField("internal_customer_id", StringType(), True),
        StructField("campaign_type", StringType(), False),
        StructField("campaign_id", IntegerType(), False),
        StructField("trigger_id", IntegerType(), True),
        StructField("started_campaign_at", TimestampType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("inserted_at", TimestampType(), False),
    ]
)


class CustomerioSubjectsIngestor(BaseCustomerioIngestor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")
        self.container_prefix += "/subjects"

    def run(self):
        self._update_parquet_metadata(
            file_type="subjects", time_cols=["created_at", "started_campaign_at"]
        )

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
                    WHERE (type = 'subjects')
                    AND NOT(date(max_timestamp) < '{start_date}' OR '{end_date}' < date(min_timestamp))
                """
            ).collect()
        }
        files_to_be_processed = list(inserted_at_map.keys())

        def _process_file(_start_date, _end_date, row):
            min_dt = min(row["created_at"], row["started_campaign_at"]).strftime(
                "%Y-%m-%d"
            )
            max_dt = max(row["created_at"], row["started_campaign_at"]).strftime(
                "%Y-%m-%d"
            )
            if min_dt > _end_date or max_dt < _start_date:
                return []

            processed_row = {
                "subject_name": row["subject_name"],
                "internal_customer_id": row["internal_customer_id"],
                "campaign_type": row["campaign_type"],
                "campaign_id": row["campaign_id"],
                "trigger_id": row["trigger_id"],
                "started_campaign_at": row["started_campaign_at"],
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
        results = self.spark.createDataFrame(results, schema=target_subjects_schema)
        results.write.mode("append").saveAsTable(
            f"{self.environment}.{self.client_name}.{self.destination_table}"
        )
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY subject_name ORDER BY inserted_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.{self.destination_table}
            )
            SELECT * FROM ranked_rows WHERE row_rank = 1;
        """
        )
