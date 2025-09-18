from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    channel_metrics,
    target_schema_variants,
    variants_desired_columns,
)
from propel_data_platform.utils.campaign_utils import CampaignUtils
from propel_data_platform.utils.constants import Constants


class BrazeVariantsCampaignTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = "transformed_variants"
        self.table_name = f"{self.client_name}.{destination_table}"

    @staticmethod
    def row_transform(row, channel_metrics):
        return CampaignUtils.extract_campaign_variants(
            row["data"], row["campaign_id"], channel_metrics
        )

    def _write_to_db(self):
        df = self.spark.sql(
            # Always fetches the latest data from view
            Constants.fetch_campaign_data_series(self.environment, self.client_name)
        ).rdd.flatMap(
            lambda row: BrazeVariantsCampaignTransform.row_transform(
                row.asDict(), channel_metrics
            )
        )
        df = (
            self.spark.createDataFrame(df, schema=target_schema_variants)
            .select(variants_desired_columns)
            .dropDuplicates(["variant_id", "type", "id", "time"])
            # data is deduplicated only at ["id", "time"] level via the view
        )

        self.upsert(df)

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                variant_id text,
                type text,
                id text,
                "time" timestamp without time zone,
                variant_name text,
                total_entries integer,
                revenue double precision,
                unique_recipients integer,
                conversions integer,
                is_control_group boolean,

                CONSTRAINT unique_{constraint_name} UNIQUE (variant_id, type, id, "time")
            );
            """

    @property
    def create_index_queries(self):
        return [
            "CREATE INDEX IF NOT EXISTS idx_vi_{index_suffix} ON {table_name} (variant_id);",
            "CREATE INDEX IF NOT EXISTS idx_ty_{index_suffix} ON {table_name} (type);",
            'CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} USING BRIN ("time");',
            "CREATE INDEX IF NOT EXISTS idx_i_{index_suffix} ON {table_name} (id);",
        ]

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (variant_id, type, id, "time") DO UPDATE SET
                total_entries = EXCLUDED.total_entries,
                revenue = EXCLUDED.revenue,
                unique_recipients = EXCLUDED.unique_recipients,
                conversions = EXCLUDED.conversions,
                variant_name = EXCLUDED.variant_name,
                is_control_group = EXCLUDED.is_control_group
            """
