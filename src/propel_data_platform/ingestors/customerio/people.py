from datetime import datetime, timedelta
from functools import partial

from pyspark.sql import Row
from pyspark.sql.functions import input_file_name, udf
from pyspark.sql.types import (
    BooleanType,
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

target_people_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("internal_customer_id", StringType(), False),
        StructField("deleted", BooleanType(), True),
        StructField("suppressed", BooleanType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("inserted_at", TimestampType(), False),
    ]
)


class CustomerioPeopleIngestor(BaseCustomerioIngestor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")
        self.container_prefix += "/people_v4"

    def run(self):
        self._update_parquet_metadata(
            file_type="people_changes", time_cols=["created_at", "updated_at"]
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
                    WHERE (type = 'people_changes')
                    AND NOT(date(max_timestamp) < '{start_date}' OR '{end_date}' < date(min_timestamp))
                """
            ).collect()
        }
        files_to_be_processed = list(inserted_at_map.keys())

        def _process_file(_start_date, _end_date, row):
            min_dt = min(row["created_at"], row["updated_at"]).strftime("%Y-%m-%d")
            max_dt = max(row["created_at"], row["updated_at"]).strftime("%Y-%m-%d")
            if min_dt > _end_date or max_dt < _start_date:
                return []

            processed_row = {
                "customer_id": row["customer_id"],
                "internal_customer_id": row["internal_customer_id"],
                "deleted": row["deleted"],
                "suppressed": row["suppressed"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
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
        results = self.spark.createDataFrame(results, schema=target_people_schema)
        results.write.mode("append").saveAsTable(
            f"{self.environment}.{self.client_name}.{self.destination_table}"
        )
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY internal_customer_id ORDER BY updated_at DESC, inserted_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.{self.destination_table}
            )
            SELECT * FROM ranked_rows WHERE row_rank = 1;
        """
        )
