from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import Window

from propel_data_platform.transformations.customerio.base import CustomerioBaseTransform


class CustomerioChannelMetricsTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metrics = [
            "attempted",
            "bounced",
            "clicked",
            "converted",
            "delivered",
            "drafted",
            "dropped",
            "failed",
            "opened",
            "sent",
            "spammed",
            "undeliverable",
            "unsubscribed",
        ]

    def _write_to_db(self):
        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        for metric in self.metrics:
            table_name = f"{self.client_name}.customerio_channel_metrics_{metric}"
            fetch_customerio_channel_metrics_query = f"""
                SELECT
                d.internal_customer_id,
                (case
                    when campaign_id is not null then campaign_id
                    when newsletter_id is not null then newsletter_id
                    when transactional_message_id is not null then transactional_message_id
                    end
                ) playbook_id,
                (case
                    when campaign_id is not null then 'campaign'
                    when newsletter_id is not null then 'newsletter'
                    when transactional_message_id is not null then 'transactional_message'
                    end
                ) type,
                ifnull(action_id, content_id) channel,
                d.delivery_type channel_name, 
                m.metric,
                m.created_at created_at
                from {self.environment}.{self.client_name}.customerio_channel_metrics_view m
                inner join {self.environment}.{self.client_name}.customerio_channel_deliveries_view d
                on m.delivery_id = d.delivery_id
                where m.metric = '{metric}'
                and m.created_at between '{start_date}' and '{end_date}'
                """
            try:
                df = self.spark.sql(fetch_customerio_channel_metrics_query).drop(
                    "metric"
                )
                window_spec = Window.partitionBy(
                    "internal_customer_id", "playbook_id"
                ).orderBy(F.col("created_at").desc())
                df = df.withColumn("row_number", F.row_number().over(window_spec))
                df = df.filter(F.col("row_number") == 1).drop("row_number")
            except Exception as e:
                if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                    continue
                else:
                    raise e

            create_table_query = """
                CREATE TABLE IF NOT EXISTS {table_name} (
                    internal_customer_id VARCHAR,
                    playbook_id INTEGER,
                    "type" VARCHAR,
                    channel INTEGER,
                    channel_name VARCHAR,
                    created_at TIMESTAMP,

                    CONSTRAINT unique_{constraint_name} UNIQUE (internal_customer_id, playbook_id)
                )
            """

            temp_table_name = f"{table_name}_temp_{self.temp_suffix()}"
            self.run_postgres_query(
                self.drop_table_query.format(table_name=temp_table_name)
            )
            constraint_name = temp_table_name.split(".")[-1]
            self.run_postgres_query(
                create_table_query.format(
                    table_name=temp_table_name, constraint_name=constraint_name
                )
            )
            self.insert_to_postgres(df, temp_table_name, full_table_name=True)

            constraint_name = table_name.split(".")[-1]
            self.run_postgres_query(
                create_table_query.format(
                    table_name=table_name, constraint_name=constraint_name
                )
            )
            create_index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_ici_{index_suffix} ON {table_name} (internal_customer_id);",
                "CREATE INDEX IF NOT EXISTS idx_pi_{index_suffix} ON {table_name} (playbook_id);",
            ]
            index_suffix = table_name.split(".")[-1]
            for index_query in create_index_queries:
                self.run_postgres_query(
                    index_query.format(table_name=table_name, index_suffix=index_suffix)
                )
            upsert_data_query = """
                INSERT INTO {destination_table}
                SELECT * FROM {source_table}
                ON CONFLICT (internal_customer_id, playbook_id) DO UPDATE 
                SET
                    type = EXCLUDED.type,
                    channel = EXCLUDED.channel,
                    channel_name = EXCLUDED.channel_name
                WHERE EXCLUDED.created_at > {destination_table}.created_at
            """
            self.run_postgres_query(
                upsert_data_query.format(
                    destination_table=table_name, source_table=temp_table_name
                )
            )
            self.run_postgres_query(
                self.drop_table_query.format(table_name=temp_table_name)
            )
