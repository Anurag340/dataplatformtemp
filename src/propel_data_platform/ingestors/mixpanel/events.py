import json
import logging

from pyspark.sql import Row
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from propel_data_platform.connectors.mixpanel import MixpanelEventsConnector
from propel_data_platform.ingestors.mixpanel.base import BaseMixpanelIngestor
from propel_data_platform.utils.constants import Constants

logger = logging.getLogger(__name__)

def run_postgres_query(
    pg_host, pg_port, pg_database, pg_user, pg_password, query
):
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password,
    )
    try:
        cursor = conn.cursor()
        logger.info(f"Executing query: {query}")
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def write_to_postgres(
    jdbc_url, pg_user, pg_password, df, table_name, mode="overwrite"
):
    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", table_name
    ).option("user", pg_user).option("password", pg_password).mode(mode).save()


# TODO: This pattern of writing directly to postgres is not robust. to be fixed.


# TODO: This pattern of writing directly to postgres is not robust. to be fixed.


class MixpanelEventsIngestor(BaseMixpanelIngestor):

    ENDPOINT = MixpanelEventsConnector.ENDPOINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        destination_table = kwargs.get("config", {}).get("destination-table")
        self.table_name = f"{self.client_name}.mixpanel_{destination_table}"

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.close_postgres_conn()
            raise e
        self.close_postgres_conn()

    def _run(self):

        def parse_json_line(line):
            try:
                row = json.loads(line)
                processed_row = {
                    "event": str(row["event"]),
                    "time": int(row["properties"]["time"]),
                    "distinct_id": str(row["properties"]["distinct_id"]),
                    "insert_id": str(row["properties"]["$insert_id"]),
                    "properties": json.dumps(row["properties"]),
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
        window_spec = Window.partitionBy(
            "event", "time", "distinct_id", "insert_id"
        ).orderBy(col("created_at").desc())
        result_df = (
            processed_df.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
        event_type_df = self.spark.sql(
            Constants.fetch_campaign_list_details(self.environment, self.client_name)
        )
        merged_df = result_df.join(
            event_type_df,
            result_df["event"] == event_type_df["event_type"],
            how="inner",
        )
        merged_df = merged_df.drop(event_type_df["event_type"])

        drop_table_query = "DROP TABLE IF EXISTS {table_name};"
        create_table_query = """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                event text,
                "time" bigint,
                distinct_id text,
                insert_id text,
                properties text,
                metadata text,
                created_at numeric(20, 6),
                event_type_id bigint,
                CONSTRAINT unique_{constraint_name} UNIQUE (event, "time", distinct_id, insert_id)
            )
        """
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_e_{index_suffix} ON {table_name} (event);",
            'CREATE INDEX IF NOT EXISTS idx_t_{index_suffix} ON {table_name} USING BRIN ("time");',
            "CREATE INDEX IF NOT EXISTS idx_di_{index_suffix} ON {table_name} (distinct_id);",
            "CREATE INDEX IF NOT EXISTS idx_ii_{index_suffix} ON {table_name} (insert_id);",
        ]
        upsert_query = """
            INSERT INTO {table_name}
            SELECT * FROM {temp_table_name}
            ON CONFLICT (event, "time", distinct_id, insert_id) DO UPDATE 
            SET
                properties = EXCLUDED.properties,
                metadata = EXCLUDED.metadata,
                created_at = EXCLUDED.created_at,
                event_type_id = EXCLUDED.event_type_id
            WHERE EXCLUDED.created_at > {table_name}.created_at;
        """

        temp_table_name = f"{self.table_name}_temp_{self.temp_suffix()}"
        self.run_postgres_query(drop_table_query.format(table_name=temp_table_name))

        constraint_name = temp_table_name.split(".")[-1]
        self.run_postgres_query(
            create_table_query.format(
                table_name=temp_table_name, constraint_name=constraint_name
            )
        )
        # TODO: no constraint required for temp table

        self.insert_to_postgres(
            merged_df, temp_table_name, "overwrite", full_table_name=True
        )

        constraint_name = self.table_name.split(".")[-1]
        if not self.postgres_table_exists(self.table_name.split(".")[-1], self.table_name.split(".")[0]):
            self.run_postgres_query(
                create_table_query.format(
                    table_name=self.table_name, constraint_name=constraint_name
                )
            )
        else:
            logger.info(f"Table {self.table_name} already exists. Skipping creation.")
 
        for index_query in index_queries:
            index_suffix = self.table_name.split(".")[-1]
            self.run_postgres_query(
                index_query.format(
                    table_name=self.table_name, index_suffix=index_suffix
                )
            )

        self.run_postgres_query(
            upsert_query.format(
                table_name=self.table_name, temp_table_name=temp_table_name
            )
        )

        self.run_postgres_query(drop_table_query.format(table_name=temp_table_name))
