from propel_data_platform.transformations.customerio.base import (
    CustomerioBaseTransform,
    newsletter_details_desired_columns,
    target_schema_newsletter_details,
)
from propel_data_platform.utils.constants import Constants
from propel_data_platform.utils.newsletter_utils import NewsletterUtils


class CustomerioNewsletterDetailsTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = "transformed_customerio_newsletter_details"
        self.table_name = f"{self.client_name}.{self.destination_table}"

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_customerio_newsletter_details(
                self.environment, self.client_name
            )
        ).rdd.map(
            lambda row: CustomerioNewsletterDetailsTransform.row_transform(row.asDict())
        )
        df = self.spark.createDataFrame(
            df, schema=target_schema_newsletter_details
        ).select(newsletter_details_desired_columns)
        self.insert_to_postgres(df, self.destination_table, mode="overwrite")

    @staticmethod
    def row_transform(row):
        return NewsletterUtils.extract_customerio_newsletter(row, row["newsletter_id"])
