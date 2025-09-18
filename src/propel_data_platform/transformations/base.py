from propel_data_platform.tasks.base import BaseTask

AIRBYTE_METADATA_COLUMNS = [
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    "_airbyte_generation_id",
]


class BaseTransform(BaseTask):

    def write_to_db(self):
        try:
            self._write_to_db()
        except Exception as e:
            self.close_postgres_conn()
            raise e
        self.close_postgres_conn()

    @property
    def create_index_queries(self):
        return []

    @property
    def drop_table_query(self):
        return "DROP TABLE IF EXISTS {table_name};"

    def upsert(self, df):
        temp_table_name = f"{self.table_name}_temp_{self.temp_suffix()}"
        # drop temp table if it exists
        self.run_postgres_query(
            self.drop_table_query.format(table_name=temp_table_name)
        )
        # create temp table
        constraint_name = temp_table_name.split(".")[-1]
        self.run_postgres_query(
            self.create_table_query.format(
                table_name=temp_table_name, constraint_name=constraint_name
            )
        )
        # insert data into temp table
        self.insert_to_postgres(df, temp_table_name, full_table_name=True)
        # create destination table if it doesn't exist
        constraint_name = self.table_name.split(".")[-1]
        self.run_postgres_query(
            self.create_table_query.format(
                table_name=self.table_name, constraint_name=constraint_name
            )
        )
        # create indexes
        index_suffix = self.table_name.split(".")[-1]
        for index_query in self.create_index_queries:
            self.run_postgres_query(
                index_query.format(
                    table_name=self.table_name, index_suffix=index_suffix
                )
            )
        # upsert data into destination table from temp table
        self.run_postgres_query(
            self.upsert_data_query.format(
                destination_table=self.table_name, source_table=temp_table_name
            )
        )
        # drop temp table
        self.run_postgres_query(
            self.drop_table_query.format(table_name=temp_table_name)
        )

    def _write_to_db(self):
        raise NotImplementedError("Subclasses must implement write_to_db")

    def run(self):
        self.write_to_db()
