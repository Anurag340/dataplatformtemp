from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    channel_metrics,
    channels_desired_columns_campaigns,
    target_schema_channels_campaign,
)
from propel_data_platform.utils.campaign_utils import CampaignUtils
from propel_data_platform.utils.constants import Constants


class BrazeChannelsCampaignTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_channels_campaign"
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
                sends integer,
                opens integer,
                clicks integer,
                delivered integer,
                conversions integer,
                revenue double precision,

                CONSTRAINT unique_{constraint_name} UNIQUE ("time", channel_name, type, id)
            )
            """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_i_{index_suffix} ON {table_name} (id);",
            'CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} USING BRIN ("time");',
            "CREATE INDEX IF NOT EXISTS idx_ty_{index_suffix} ON {table_name} (type);",
            "CREATE INDEX IF NOT EXISTS idx_cn_{index_suffix} ON {table_name} (channel_name);",
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT ("time", channel_name, type, id) DO UPDATE 
            SET
                sends = EXCLUDED.sends,
                opens = EXCLUDED.opens,
                clicks = EXCLUDED.clicks,
                delivered = EXCLUDED.delivered,
                conversions = EXCLUDED.conversions,
                revenue = EXCLUDED.revenue;
        """

    @staticmethod
    def row_transform(row, channel_metrics):
        return CampaignUtils.extract_channels(
            row["data"], row["campaign_id"], channel_metrics
        )

    def _write_to_db(self):
        df = self.spark.sql(
            # Always fetches the latest data from view
            Constants.fetch_campaign_data_series(self.environment, self.client_name)
        ).rdd.flatMap(
            lambda row: BrazeChannelsCampaignTransform.row_transform(
                row, channel_metrics
            )
        )
        df = (
            self.spark.createDataFrame(df, schema=target_schema_channels_campaign)
            .select(channels_desired_columns_campaigns)
            .dropDuplicates(["time", "channel_name", "type", "id"])
            # data is deduplicated only at ["time", "id"] via the view
        )

        self.upsert(df)
