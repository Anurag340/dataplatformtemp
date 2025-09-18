from propel_data_platform.transformations.customerio.base import (
    CustomerioBaseTransform,
    target_schema_transactional_message_details,
    transactional_message_details_desired_columns,
)
from propel_data_platform.utils.constants import Constants
from propel_data_platform.utils.transactional_message_utils import (
    TransactionalMessageUtils,
)


class CustomerioTransactionalMessageDetailsTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = f"{self.client_name}.transformed_customerio_transactional_message_details"  # table to be created.
        self.destination_table = "transformed_customerio_transactional_message_details"

    def _write_to_db(self):
        df = self.spark.sql(
            Constants.fetch_customerio_transactional_message_details(
                self.environment, self.client_name
            )
        ).rdd.map(
            lambda row: CustomerioTransactionalMessageDetailsTransform.row_transform(
                row.asDict()
            )
        )
        df = self.spark.createDataFrame(
            df, schema=target_schema_transactional_message_details
        ).select(transactional_message_details_desired_columns)
        self.insert_to_postgres(df, self.destination_table, mode="overwrite")

    @staticmethod
    def row_transform(row):
        return TransactionalMessageUtils.extract_customerio_transactional_message(
            row, row["transactional_message_id"]
        )
