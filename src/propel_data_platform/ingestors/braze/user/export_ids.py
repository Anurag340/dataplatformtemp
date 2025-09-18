import json

from pyspark.sql import Row

from propel_data_platform.connectors.braze import BrazeUserExportIdsConnector
from propel_data_platform.ingestors.braze.base import BaseBrazeIngestor


class BrazeUserExportIdsIngestor(BaseBrazeIngestor):

    ENDPOINT = BrazeUserExportIdsConnector.ENDPOINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")

    def run(self):

        def parse_json_line(line):
            try:
                row = json.loads(line)
                processed_rows = []
                for user in row.get("data", []):
                    processed_row = {
                        "user_id": user.get("external_id"),
                        "custom_attributes": json.dumps(
                            user.get("custom_attributes", {})
                        ),
                        "purchases": json.dumps(user.get("purchases", [])),
                        "dob": user.get("dob"),
                        "country": user.get("country"),
                        "home_city": user.get("home_city"),
                        "language": user.get("language"),
                        "gender": user.get("gender"),
                        "push_subscribe": user.get("push_subscribe"),
                        "push_opted_in_at": user.get("push_opted_in_at"),
                        "email_subscribe": user.get("email_subscribe"),
                        "email_opted_in_at": user.get("email_opted_in_at"),
                        "devices": json.dumps(user.get("devices", [])),
                        "apps": json.dumps(user.get("apps", [])),
                        "campaigns_received": json.dumps(
                            user.get("campaigns_received", [])
                        ),
                        "canvases_received": json.dumps(
                            user.get("canvases_received", [])
                        ),
                        "metadata": json.dumps(row.get("metadata", {})),
                        "created_at": row.get("metadata", {}).get("timestamp"),
                    }
                    processed_rows.append(Row(**processed_row))
                return processed_rows
            except json.JSONDecodeError:
                return []

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.environment}.{self.client_name}.braze_{self.destination_table} (
                user_id STRING,
                custom_attributes STRING,
                purchases STRING,
                dob STRING,
                country STRING,
                home_city STRING,
                language STRING,
                gender STRING,
                push_subscribe STRING,
                push_opted_in_at STRING,
                email_subscribe STRING,
                email_opted_in_at STRING,
                devices STRING,
                apps STRING,
                campaigns_received STRING,
                canvases_received STRING, 
                metadata STRING,
                created_at STRING
            )
        """
        )
        schema = self.spark.read.table(
            f"{self.environment}.{self.client_name}.braze_{self.destination_table}"
        ).schema
        df = self.spark.read.text(self.jsonl_path)
        rdd = df.rdd.flatMap(lambda row: parse_json_line(row.value))
        processed_df = self.spark.createDataFrame(rdd, schema)
        processed_df.write.mode("append").saveAsTable(
            f"{self.environment}.{self.client_name}.braze_{self.destination_table}"
        )
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.braze_{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.braze_{self.destination_table}
            )
            SELECT 
                *
            FROM ranked_rows
            WHERE row_rank = 1;
        """
        )
