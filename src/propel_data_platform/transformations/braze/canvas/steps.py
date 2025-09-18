from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    channel_metrics,
    steps_desired_columns,
    target_schema_canvas_steps,
)
from propel_data_platform.utils.canvas_utils import CanvasUtils
from propel_data_platform.utils.constants import Constants


class BrazeCanvasStepsTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_canvas_steps"
        self.table_name = f"{self.client_name}.{destination_table}"

    @staticmethod
    def row_transform(row, channel_metrics):
        return CanvasUtils.extract_steps(
            row["stats"], row["canvas_id"], channel_metrics
        )

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                "time" timestamp without time zone,
                canvas_id text,
                step_id text,
                step_name text,
                revenue double precision,
                conversions integer,
                channels text,
                unique_recipients integer,
                sent integer,
                total_opens integer,
                clicks integer,

                CONSTRAINT unique_{constraint_name} UNIQUE ("time", canvas_id, step_id)
            )
        """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_ci_{index_suffix} ON {table_name} (canvas_id);",
            "CREATE INDEX IF NOT EXISTS idx_si_{index_suffix} ON {table_name} (step_id);",
            'CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} USING BRIN ("time");',
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT ("time", canvas_id, step_id) DO UPDATE SET
                step_name = EXCLUDED.step_name,
                revenue = EXCLUDED.revenue,
                conversions = EXCLUDED.conversions,
                channels = EXCLUDED.channels,
                unique_recipients = EXCLUDED.unique_recipients,
                sent = EXCLUDED.sent,
                total_opens = EXCLUDED.total_opens,
                clicks = EXCLUDED.clicks
        """

    def _write_to_db(self):
        df = self.spark.sql(
            # Always fetches the latest data from view
            Constants.fetch_canvas_data_series(self.environment, self.client_name)
        ).rdd.flatMap(
            lambda row: BrazeCanvasStepsTransform.row_transform(
                row.asDict(), channel_metrics
            )
        )
        df = (
            self.spark.createDataFrame(df, schema=target_schema_canvas_steps)
            .select(steps_desired_columns)
            .dropDuplicates(["time", "canvas_id", "step_id"])
            # The above dropDuplicates may not be needed, data is deduplicated at ["time", "canvas_id"] via the view and "step_id" comes from a dict key
        )

        self.upsert(df)
