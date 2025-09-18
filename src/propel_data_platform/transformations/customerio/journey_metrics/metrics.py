from datetime import datetime, timedelta

from propel_data_platform.transformations.customerio.base import (
    CustomerioBaseTransform,
    journey_metrics_desired_columns,
)


class CustomerioJourneyMetricsTransform(CustomerioBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.destination_table = "transformed_customerio_journey_metrics"
        self.table_name = f"{self.client_name}.{self.destination_table}"

    def _write_to_db(self):
        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        self.run_postgres_query(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{self.client_name}' 
                    AND table_name = '{self.destination_table}'
                ) THEN
                    DELETE FROM {self.table_name}
                    WHERE date(created_at) BETWEEN '{start_date}' AND '{end_date}';
                END IF;
            END$$;
        """
        )

        try:
            df = self.spark.sql(
                f"""SELECT s.internal_customer_id, s.campaign_id playbook_id, 'campaign' type,
                    o.action_id channel, s.campaign_type playbook_type, o.explanation, o.output_type metric,
                    to_timestamp(o.delay_ends_at, 'yyyy-MM-dd HH:mm:ss') delay_ends_at, 
                    to_timestamp(s.started_campaign_at, 'yyyy-MM-dd HH:mm:ss') started_campaign_at, 
                    to_timestamp(o.created_at, 'yyyy-MM-dd HH:mm:ss') created_at
                    from {self.environment}.{self.client_name}.customerio_journey_outputs_view o
                    inner join {self.environment}.{self.client_name}.customerio_journey_subjects_view s
                    on o.subject_name = s.subject_name
                    and date(o.created_at) BETWEEN '{start_date}' AND '{end_date}'
                """
            ).select(journey_metrics_desired_columns)
            self.insert_to_postgres(df, self.destination_table)
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                return
            else:
                raise e
