from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    target_schema_user_playbook_data,
    user_playbook_data_desired_columns,
)
from propel_data_platform.utils.campaign_utils import CampaignUtils
from propel_data_platform.utils.constants import Constants


class BrazeUserCampaignTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_user_playbook_data"
        self.table_name = f"{self.client_name}.{destination_table}"

    @staticmethod
    def row_transform(row):
        return CampaignUtils.extract_user_campaign_details(
            row["user_id"], row["campaigns_received"]
        )

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                id text,
                name text,
                type text,
                user_id text,
                last_received timestamp without time zone,
                in_control boolean,
                converted boolean,
                conversion_events_performed text,
                message_status text,

                CONSTRAINT unique_{constraint_name} UNIQUE (user_id, id, type)
            )
            """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_i_{index_suffix} ON {table_name} (id);",
            "CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} (type);",
            "CREATE INDEX IF NOT EXISTS idx_ui_{index_suffix} ON {table_name} (user_id);",
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (user_id, id, type) DO UPDATE 
            SET
                last_received = EXCLUDED.last_received,
                in_control = EXCLUDED.in_control,
                converted = EXCLUDED.converted,
                conversion_events_performed = EXCLUDED.conversion_events_performed,
                message_status = EXCLUDED.message_status
            WHERE EXCLUDED.last_received > {destination_table}.last_received;
            """

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_user_playbook_campaign_data(
                self.environment, self.client_name
            )
        ).rdd.flatMap(
            lambda row: (
                BrazeUserCampaignTransform.row_transform(row.asDict())
                if row["campaigns_received"] and len(row["campaigns_received"]) > 0
                else []
            )
        )
        df = (
            self.spark.createDataFrame(df, schema=target_schema_user_playbook_data)
            .select(user_playbook_data_desired_columns)
            .dropDuplicates(["user_id", "id", "type"])
            # data is deduplicated only at ["user_id"] level via the view
        )

        self.upsert(df)
