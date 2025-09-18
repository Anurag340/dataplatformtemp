from propel_data_platform.transformations.braze.base import (
    BrazeBaseTransform,
    user_data_desired_columns,
)
from propel_data_platform.utils.constants import Constants


class BrazeUserDetailsTransform(BrazeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = "transformed_user_details"
        self.table_name = f"{self.client_name}.{self.destination_table}"

    @property
    def create_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                user_id text PRIMARY KEY,
                custom_attributes text,
                purchases text,
                dob text,
                country text,
                home_city text,
                language text,
                gender text,
                push_subscribe text,
                push_opted_in_at text,
                email_subscribe text,
                email_opted_in_at text,
                devices text,
                apps text
            )
            """

    @property
    def upsert_data_query(self):
        return """
            INSERT INTO {destination_table}
            SELECT * FROM {source_table}
            ON CONFLICT (user_id) DO UPDATE 
            SET
                custom_attributes = EXCLUDED.custom_attributes,
                purchases = EXCLUDED.purchases,
                dob = EXCLUDED.dob,
                country = EXCLUDED.country,
                home_city = EXCLUDED.home_city,
                language = EXCLUDED.language,
                gender = EXCLUDED.gender,
                push_subscribe = EXCLUDED.push_subscribe,
                push_opted_in_at = EXCLUDED.push_opted_in_at,
                email_subscribe = EXCLUDED.email_subscribe,
                email_opted_in_at = EXCLUDED.email_opted_in_at,
                devices = EXCLUDED.devices,
                apps = EXCLUDED.apps
        """

    def _write_to_db(self):
        df = (
            self.spark.sql(
                Constants.fetch_user_data(self.environment, self.client_name)
            )
            .select(user_data_desired_columns)
            .dropDuplicates(["user_id"])
        )

        self.upsert(df)
