from datetime import datetime, timedelta

from pyspark.sql import functions as F

from propel_data_platform.transformations.customerio.base import (
    CustomerioBaseTransform,
    people_details_desired_columns,
    target_schema_people_details,
)


class CustomerioPeopleTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = "transformed_customerio_people"
        self.table_name = f"{self.client_name}.{self.destination_table}"

    def _write_to_db(self):
        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        try:
            df = self.spark.sql(
                f"""SELECT customer_id AS user_id, internal_customer_id, 
                    CASE WHEN deleted = 'false' THEN FALSE ELSE TRUE END AS deleted, 
                    CASE WHEN suppressed = 'false' THEN FALSE ELSE TRUE END AS suppressed, 
                    CASE WHEN created_at = '1970-01-01 00:00:00.000' THEN null WHEN created_at is null THEN null ELSE to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') END AS created_at, 
                    to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss') updated_at
                    from {self.environment}.{self.client_name}.customerio_people_view
                    where (date(updated_at) BETWEEN '{start_date}' AND '{end_date}') or (date(created_at) BETWEEN '{start_date}' AND '{end_date}')"""
            ).select(people_details_desired_columns)
            for col_name in people_details_desired_columns:
                col_type = target_schema_people_details[col_name].dataType
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            self.upsert(df)
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                return
            else:
                raise e

    @property
    def create_table_query(self):
        return """CREATE TABLE IF NOT EXISTS {table_name}
        (
            user_id text,
            internal_customer_id text PRIMARY KEY,
            deleted boolean,
            suppressed boolean,
            created_at timestamp without time zone,
            updated_at timestamp without time zone
        )"""

    @property
    def upsert_data_query(self):
        return """INSERT INTO {destination_table} 
        SELECT * FROM {source_table}
        ON CONFLICT (internal_customer_id) DO UPDATE 
        SET
            user_id = EXCLUDED.user_id,
            deleted = EXCLUDED.deleted,
            suppressed = EXCLUDED.suppressed,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        WHERE EXCLUDED.updated_at > {destination_table}.updated_at
        """
