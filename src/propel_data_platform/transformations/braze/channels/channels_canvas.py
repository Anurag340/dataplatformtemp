from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    channel_metrics,
    channels_desired_columns_canvas,
    target_schema_channels_canvas,
)
from propel_data_platform.utils.canvas_utils import CanvasUtils
from propel_data_platform.utils.constants import Constants


class BrazeChannelsCanvasTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_channels_canvas"
        self.table_name = f"{self.client_name}.{destination_table}"

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                "time" timestamp without time zone,
                channel_name text,
                type text,
                id text,
                step_id text,
                sends integer,
                opens integer,
                clicks integer,
                delivered integer,

                CONSTRAINT unique_{constraint_name} UNIQUE ("time", channel_name, type, id, step_id)
            )
        """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_i_{index_suffix} ON {table_name} (id);",
            'CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} USING BRIN ("time");',
            "CREATE INDEX IF NOT EXISTS idx_ty_{index_suffix} ON {table_name} (type);",
            "CREATE INDEX IF NOT EXISTS idx_cn_{index_suffix} ON {table_name} (channel_name);",
            "CREATE INDEX IF NOT EXISTS idx_si_{index_suffix} ON {table_name} (step_id);",
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT ("time", channel_name, type, id, step_id) DO UPDATE SET
                sends = EXCLUDED.sends,
                opens = EXCLUDED.opens,
                clicks = EXCLUDED.clicks,
                delivered = EXCLUDED.delivered;
        """

    @staticmethod
    def row_transform(row, channel_metrics):
        return CanvasUtils.extract_channels(
            row["stats"], row["canvas_id"], channel_metrics
        )

    def _write_to_db(self):
        df = self.spark.sql(
            # Always fetches the latest data from view
            Constants.fetch_canvas_data_series(self.environment, self.client_name)
        ).rdd.flatMap(
            lambda row: BrazeChannelsCanvasTransform.row_transform(
                row.asDict(), channel_metrics
            )
        )
        df = (
            self.spark.createDataFrame(df, schema=target_schema_channels_canvas)
            .select(channels_desired_columns_canvas)
            .dropDuplicates(["time", "channel_name", "type", "id", "step_id"])
            # data is deduplicated only at ["time", "id"] level via the view
        )

        self.upsert(df)
