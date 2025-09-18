from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    target_schema_user_playbook_steps_data,
    user_playbook_steps_data_desired_columns,
)
from propel_data_platform.utils.canvas_utils import CanvasUtils
from propel_data_platform.utils.constants import Constants


class BrazeUserCanvasStepsTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_user_playbook_steps_data"
        self.table_name = f"{self.client_name}.{destination_table}"

    @staticmethod
    def row_transform(row):
        return CanvasUtils.extract_user_canvas_steps_details(
            row["user_id"], row["canvases_received"]
        )

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                step_id text,
                step_name text,
                playbook_id text,
                user_id text,
                last_received timestamp without time zone,

                CONSTRAINT unique_{constraint_name} UNIQUE (user_id, step_id, playbook_id)
            )
            """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_si_{index_suffix} ON {table_name} (step_id);",
            "CREATE INDEX IF NOT EXISTS idx_pi_{index_suffix} ON {table_name} (playbook_id);",
            "CREATE INDEX IF NOT EXISTS idx_ui_{index_suffix} ON {table_name} (user_id);",
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (user_id, step_id, playbook_id) DO UPDATE 
            SET
                step_name = EXCLUDED.step_name,
                last_received = EXCLUDED.last_received
            WHERE EXCLUDED.last_received > {destination_table}.last_received;
            """

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_user_playbook_canvas_data(
                self.environment, self.client_name
            )
        ).rdd.flatMap(
            lambda row: (
                BrazeUserCanvasStepsTransform.row_transform(row.asDict())
                if row["canvases_received"] and len(row["canvases_received"]) > 0
                else []
            )
        )
        df = (
            self.spark.createDataFrame(
                df, schema=target_schema_user_playbook_steps_data
            )
            .select(user_playbook_steps_data_desired_columns)
            .dropDuplicates(["user_id", "step_id", "playbook_id"])
            # data is deduplicated only at ["user_id"] level via the view
        )

        self.upsert(df)
