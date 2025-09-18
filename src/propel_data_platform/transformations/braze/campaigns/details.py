from pyspark.sql.functions import expr

from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    campaign_details_desired_columns,
    target_schema_campaign_details,
)
from propel_data_platform.utils.campaign_utils import CampaignUtils
from propel_data_platform.utils.constants import Constants


class BrazeCampaignDetailsTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_braze_campaign_details"
        self.table_name = f"{self.client_name}.{destination_table}"

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                campaign_id      text PRIMARY KEY,
                name             text,
                description      text,
                is_archived      boolean,
                is_enabled       boolean,
                is_draft         boolean,
                created_at       timestamp without time zone,
                updated_at       timestamp without time zone,
                schedule_type    text,
                channels         text,
                first_sent       text,
                last_sent        text,
                messages         integer,
                goal             text,
                stage            text
            );
        """

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (campaign_id) DO UPDATE 
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
        return CampaignUtils.extract_campaigns(row["data"], row["campaign_id"])

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_campaign_details(self.environment, self.client_name)
        ).rdd.map(lambda row: BrazeCampaignDetailsTransform.row_transform(row.asDict()))
        df = (
            self.spark.createDataFrame(df, schema=target_schema_campaign_details)
            .withColumn(
                "first_sent",
                expr("CASE WHEN first_sent IS NOT NULL THEN first_sent ELSE NULL END"),
            )
            .withColumn(
                "last_sent",
                expr("CASE WHEN last_sent IS NOT NULL THEN last_sent ELSE NULL END"),
            )
            .select(campaign_details_desired_columns)
        )

        self.upsert(df)
