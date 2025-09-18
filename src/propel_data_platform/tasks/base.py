import random
import string
from abc import ABC
from contextlib import contextmanager

import psycopg2
from azure.storage.blob import BlobServiceClient, ContainerClient
from pyspark.sql import SparkSession


class BaseTask(ABC):

    def __init__(self, **kwargs):
        self.run_id = kwargs.get("run_id")
        self.config = kwargs.get("config")
        self.local = kwargs.get("local")
        self.environment = kwargs.get("config", {}).get("environment").lower()
        self.client_name = kwargs.get("config", {}).get("client").lower()

        # Note: The below dates are run dates, not the data dates. When start-date is 2025-06-22 and end-date is 2025-06-24, it will run as if the pipeline was run on 2025-06-22, 2025-06-23, 2025-06-24.
        self.start_date = kwargs.get("config", {}).get("start-date")
        self.end_date = kwargs.get("config", {}).get("end-date")

        self.azure_storage_connection_string = kwargs.get("config", {}).get(
            "azure-storage-connection-string"
        )
        self.postgres_config = kwargs.get("config", {}).get("postgres-config", {})
        self.clickhouse_config = kwargs.get("config", {}).get("clickhouse-config", {})

        self.maybe_create_container()
        self.maybe_create_database()
        self.maybe_create_schema()

    def maybe_create_container(self):
        blob_service_client = BlobServiceClient.from_connection_string(
            self.azure_storage_connection_string
        )
        container_client = blob_service_client.get_container_client(self.client_name)
        if not container_client.exists():
            container_client.create_container()

    def maybe_create_database(self):
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.environment}.{self.client_name}"
        )

    def maybe_create_schema(self):
        with self.get_postgres_cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.client_name};")

    @property
    def spark(self):
        if not hasattr(self, "_spark"):
            if not self.local:
                self._spark = SparkSession.builder.appName(
                    f"{self.run_id}-{self.environment}-{self.client_name}"
                ).getOrCreate()
            else:
                from databricks.connect import DatabricksSession

                databricks_host = self.config.get("databricks-config", {}).get("host")
                databricks_profile = databricks_host.replace("https://", "").split(".")[
                    0
                ]
                self._spark = DatabricksSession.builder.profile(
                    databricks_profile
                ).getOrCreate()
            conn_parts = dict(
                part.split("=", 1)
                for part in self.azure_storage_connection_string.split(";")
                if part
            )
            account_name = conn_parts.get("AccountName")
            account_key = conn_parts.get("AccountKey")
            self._spark.conf.set(
                "fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
            )
            self._spark.conf.set(
                f"fs.azure.account.key.{account_name}.blob.core.windows.net",
                account_key,
            )
        return self._spark

    @property
    def container_client(self):
        if not hasattr(self, "_container_client"):
            self._container_client = ContainerClient.from_connection_string(
                conn_str=self.azure_storage_connection_string,
                container_name=self.client_name,
            )
        return self._container_client

    @property
    def jdbc_url(self):
        return f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"

    @property
    def clickhouse_url(self):
        return f"jdbc:clickhouse://{self.clickhouse_config['host']}:{self.clickhouse_config['port']}/{self.clickhouse_config['database']}"

    def get_wasbs_path(self, path):
        conn_parts = dict(
            part.split("=", 1)
            for part in self.azure_storage_connection_string.split(";")
            if part
        )
        account_name = conn_parts.get("AccountName")
        return f"wasbs://{self.client_name}@{account_name}.blob.core.windows.net/{path}"

    def get_blob_path_from_wasbs_path(self, wasbs_path):
        prefix = self.get_wasbs_path("")
        return wasbs_path[len(prefix) :]

    @property
    def postgres_conn(self):
        if not hasattr(self, "_postgres_conn"):
            self._postgres_conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                database=self.postgres_config["database"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
            )
        return self._postgres_conn

    def close_postgres_conn(self):
        if hasattr(self, "_postgres_conn"):
            try:
                self._postgres_conn.close()
            except:
                pass

    def __del__(self):
        self.close_postgres_conn()

    @contextmanager
    def get_postgres_cursor(self):
        cursor = None
        try:
            cursor = self.postgres_conn.cursor()
            yield cursor
            self.postgres_conn.commit()
        except Exception as e:
            self.postgres_conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()

    def run_postgres_query(self, query):
        with self.get_postgres_cursor() as cursor:
            cursor.execute(query)

    def insert_to_postgres(self, df, table_name, mode="append", full_table_name=False):
        if not full_table_name:
            table_name = f"{self.client_name}.{table_name}"
        else:
            table_name = table_name
        (
            df.write.format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", table_name)
            .option("user", self.postgres_config["user"])
            .option("password", self.postgres_config["password"])
            .mode(mode)
            .save()
        )

    def read_from_postgres(self, query):
        return (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("query", query)
            .option("user", self.postgres_config["user"])
            .option("password", self.postgres_config["password"])
            .load()
        )

    def postgres_table_exists(self, table_name, schema=None):
        if schema is None:
            schema = self.client_name
        with self.get_postgres_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}' 
                    AND table_name = '{table_name}'
                )"""
            )
            return bool(cursor.fetchone()[0])

    def run_clickhouse_query(self, query):
        """Execute a query on ClickHouse using Spark JDBC"""
        return (
            self.spark.read.format("jdbc")
            .option("url", self.clickhouse_url)
            .option("query", query)
            .option("user", self.clickhouse_config["user"])
            .option("password", self.clickhouse_config["password"])
            .load()
        )

    @staticmethod
    def temp_suffix():
        return "".join(random.choices(string.ascii_lowercase, k=4))

    def sync_to_clickhouse(self, table_name, schema=None):
        if schema is None:
            schema = self.client_name

        df = self.read_from_postgres(f"SELECT * FROM {schema}.{table_name}")

        temp_table_name = f"{table_name}_temp_{self.temp_suffix()}"
        (
            df.write.format("jdbc")
            .option("url", self.clickhouse_url)
            .option("dbtable", f"{schema}__{temp_table_name}")
            .option("user", self.clickhouse_config["user"])
            .option("password", self.clickhouse_config["password"])
            .mode("overwrite")
            .save()
        )

        drop_and_rename_query = f"""
        DROP TABLE IF EXISTS {table_name};
        RENAME TABLE {schema}__{temp_table_name} TO {table_name};
        """

        self.run_clickhouse_query(drop_and_rename_query)

    def run(self):
        raise NotImplementedError("Subclasses must implement run")
