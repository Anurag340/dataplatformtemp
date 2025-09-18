import json

from pyspark.sql import Row

from propel_data_platform.connectors.braze import BrazeCanvasDataSeriesConnector
from propel_data_platform.ingestors.braze.base import BaseBrazeIngestor


class BrazeCanvasDataSeriesIngestor(BaseBrazeIngestor):

    ENDPOINT = BrazeCanvasDataSeriesConnector.ENDPOINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = kwargs.get("config", {}).get("destination-table")

    def run(self):

        def parse_json_line(line):
            try:
                row = json.loads(line)
                ouput_rows = []
                for data in row.get("stats", []):
                    processed_row = {
                        "canvas_id": str(row["metadata"]["params"]["canvas_id"]),
                        "name": str(row["name"]),
                        "stats": json.dumps(data),
                        "metadata": json.dumps(row["metadata"]),
                        "created_at": row["metadata"]["timestamp"],
                        "start_date": data["time"],
                    }
                    ouput_rows.append(Row(**processed_row))
                return ouput_rows
            except json.JSONDecodeError:
                return []

        df = self.spark.read.text(self.jsonl_path)
        rdd = df.rdd.flatMap(lambda row: parse_json_line(row.value))
        processed_df = rdd.toDF()
        processed_df.write.mode("append").saveAsTable(
            f"{self.environment}.{self.client_name}.braze_{self.destination_table}"
        )
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.environment}.{self.client_name}.braze_{self.destination_table}_view AS
            WITH ranked_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY canvas_id, start_date ORDER BY created_at DESC) AS row_rank
                FROM {self.environment}.{self.client_name}.braze_{self.destination_table}
            )
            SELECT 
                *
            FROM ranked_rows
            WHERE row_rank = 1;
        """
        )
