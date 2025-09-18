from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

from propel_data_platform.transformations.customerio.base import (
    CustomerioBaseTransform,
    campaign_details_desired_columns,
    target_schema_campaign_details,
)
from propel_data_platform.utils.campaign_utils import CampaignUtils
from propel_data_platform.utils.constants import Constants


class CustomerioCampaignDetailsTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_customerio_campaign_details"
        self.table_name = f"{self.client_name}.{destination_table}"

    @staticmethod
    def row_transform(row):
        return CampaignUtils.extract_customerio_campaigns(row, row["campaign_id"])

    @property
    def create_table_query(self):
        return """CREATE TABLE IF NOT EXISTS {table_name}
        (
            campaign_id integer PRIMARY KEY,
            name text,
            state text,
            active boolean,
            channels text,
            actions text,
            created_at timestamp without time zone,
            updated_at timestamp without time zone,
            first_started timestamp without time zone,
            deduplicate_id text,
            trigger_segment_ids text,
            tags text
        )
        """

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (campaign_id) DO UPDATE SET
                name = EXCLUDED.name,
                state = EXCLUDED.state,
                active = EXCLUDED.active,
                channels = EXCLUDED.channels,
                actions = EXCLUDED.actions,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                first_started = EXCLUDED.first_started,
                deduplicate_id = EXCLUDED.deduplicate_id,
                trigger_segment_ids = EXCLUDED.trigger_segment_ids,
                tags = EXCLUDED.tags
            WHERE EXCLUDED.updated_at > {destination_table}.updated_at;
        """

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_customerio_campaign_details(
                self.environment, self.client_name
            )
        ).rdd.map(
            lambda row: CustomerioCampaignDetailsTransform.row_transform(row.asDict())
        )
        df = self.spark.createDataFrame(
            df, schema=target_schema_campaign_details
        ).select(campaign_details_desired_columns)
        window_spec = Window.partitionBy("campaign_id").orderBy(df["updated_at"].desc())
        df = (
            df.withColumn("row_number", row_number().over(window_spec))
            .filter("row_number = 1")
            .drop("row_number")
        )

        self.upsert(df)
