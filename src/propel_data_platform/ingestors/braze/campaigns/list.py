import json

from pyspark.sql import Row

from propel_data_platform.connectors.braze import BrazeCampaignsListConnector
from propel_data_platform.ingestors.braze.base import BaseBrazeIngestor


class BrazeCampaignsListIngestor(BaseBrazeIngestor):

    ENDPOINT = BrazeCampaignsListConnector.ENDPOINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")

    def run(self):

        def parse_json_line(line):
            try:
                row = json.loads(line)
                processed_row = {
                    "id": str(row["id"]),
                    "name": str(row["name"]),
                    "tags": json.dumps(row.get("tags")),
                    "last_edited": str(row["last_edited"]),
                    "is_api_campaign": row["is_api_campaign"],
                    "metadata": json.dumps(row["metadata"]),
                    "created_at": row["metadata"]["timestamp"],
                }
                return Row(**processed_row)
            except json.JSONDecodeError:
                return None

        df = self.spark.read.text(self.jsonl_path)
        rdd = df.rdd.map(lambda row: parse_json_line(row.value)).filter(
            lambda x: x is not None
        )
        processed_df = rdd.toDF()
        processed_df.write.mode("overwrite").saveAsTable(
            f"{self.environment}.{self.client_name}.braze_{self.destination_table}"
        )

        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.braze_{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.braze_{self.destination_table}
            )
            SELECT 
                *
            FROM ranked_rows
            WHERE row_rank = 1;
        """
        )
