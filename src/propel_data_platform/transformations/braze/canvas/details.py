from pyspark.sql.functions import expr

from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    canvas_details_desired_columns,
    target_schema_canvas_details,
)
from propel_data_platform.utils.canvas_utils import CanvasUtils
from propel_data_platform.utils.constants import Constants


class BrazeCanvasDetailsTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_braze_canvas_details"
        self.table_name = f"{self.client_name}.{destination_table}"

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                canvas_id text PRIMARY KEY,
                name text,
                description text,
                is_archived boolean,
                is_enabled boolean,
                is_draft boolean,
                created_at timestamp without time zone,
                updated_at timestamp without time zone,
                schedule_type text,
                channels text,
                first_sent text,
                last_sent text,
                messages integer,
                goal text,
                stage text
            )
        """

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (canvas_id) DO UPDATE 
            SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                is_archived = EXCLUDED.is_archived,
                is_enabled = EXCLUDED.is_enabled,
                is_draft = EXCLUDED.is_draft,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                schedule_type = EXCLUDED.schedule_type,
                channels = EXCLUDED.channels,
                first_sent = EXCLUDED.first_sent,
                last_sent = EXCLUDED.last_sent,
                messages = EXCLUDED.messages,
                goal = EXCLUDED.goal,
                stage = EXCLUDED.stage
            WHERE EXCLUDED.updated_at > {destination_table}.updated_at;
        """

    @staticmethod
    def row_transform(row):
        return CanvasUtils.extract_canvas_details(row["data"], row["canvas_id"])

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_canvas_details(self.environment, self.client_name)
        ).rdd.map(lambda row: BrazeCanvasDetailsTransform.row_transform(row.asDict()))
        df = (
            self.spark.createDataFrame(df, schema=target_schema_canvas_details)
            .withColumn(
                "first_sent",
                expr("CASE WHEN first_sent IS NOT NULL THEN first_sent ELSE NULL END"),
            )
            .withColumn(
                "last_sent",
                expr("CASE WHEN last_sent IS NOT NULL THEN last_sent ELSE NULL END"),
            )
            .select(canvas_details_desired_columns)
        )

        self.upsert(df)
