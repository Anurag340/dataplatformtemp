import datetime

from propel_data_platform.tasks.base import BaseTask


class PostgresCleaner(BaseTask):

    def get_retention_period(self):
        # TODO: get retention period from sonic schema
        # retention_period = self.read_from_postgres(f"""
        #     SELECT retention_period
        #     FROM sonic.some_table s
        #     LEFT JOIN sonic.organisations o
        #     ON s.organisation_id = o.id
        #     WHERE o.domain = '{self.client_name}'
        # """)
        return 90

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.close_postgres_conn()
            raise e
        self.close_postgres_conn()

    def _run(self):
        retention_period = self.get_retention_period()
        anchor_date = (
            datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
            - datetime.timedelta(days=retention_period)
        ).strftime("%Y-%m-%d")

        if self.postgres_table_exists("mixpanel_events"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.mixpanel_events WHERE TO_CHAR(TO_TIMESTAMP(time), 'YYYY-MM-DD') < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_canvas_steps"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_canvas_steps WHERE DATE(time) < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_channels_campaign"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_channels_campaign WHERE DATE(time) < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_channels_canvas"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_channels_canvas WHERE DATE(time) < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_user_playbook_data"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_user_playbook_data WHERE DATE(last_received) < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_user_playbook_steps_data"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_user_playbook_steps_data WHERE DATE(last_received) < '{anchor_date}'"
            )

        if self.postgres_table_exists("transformed_variants"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_variants WHERE DATE(time) < '{anchor_date}'"
            )

        metrics = [
            "attempted",
            "bounced",
            "clicked",
            "converted",
            "delivered",
            "drafted",
            "dropped",
            "failed",
            "opened",
            "sent",
            "spammed",
            "undeliverable",
            "unsubscribed",
        ]
        for metric in metrics:
            if self.postgres_table_exists(f"customerio_channel_metrics_{metric}"):
                self.run_postgres_query(
                    f"DELETE FROM {self.client_name}.customerio_channel_metrics_{metric} WHERE DATE(created_at) < '{anchor_date}'"
                )

        if self.postgres_table_exists("transformed_customerio_journey_metrics"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.transformed_customerio_journey_metrics WHERE DATE(created_at) < '{anchor_date}'"
            )

        if self.postgres_table_exists("amplitude_events"):
            self.run_postgres_query(
                f"DELETE FROM {self.client_name}.amplitude_events WHERE DATE(server_upload_time) < '{anchor_date}'"
            )
