import json

from propel_data_platform.transformations.mixpanel.base import MixpanelBaseTransform
from propel_data_platform.utils.constants import Constants


class MixpanelMeteringTransform(MixpanelBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_active_events_count(self):
        if not (
            self.postgres_table_exists("mixpanel_events_list")
            and self.postgres_table_exists("mixpanel_events")
        ):
            return {"active_events": 0}
        query = Constants.fetch_mixpanel_active_events_count(
            self.environment, self.client_name
        )
        result = self.spark.sql(query)
        count = result.collect()[0]["active_events_count"]
        return {"active_events": count}

    def get_monthly_active_users(self):
        if not (
            self.postgres_table_exists("mixpanel_events_list")
            and self.postgres_table_exists("mixpanel_events")
        ):
            return {"monthly_active_users": 0}
        query = Constants.fetch_mixpanel_monthly_active_users(self.client_name)
        result_df = self.read_from_postgres(query)
        count = result_df.collect()[0]["mau_count"]
        return {"monthly_active_users": count}

    def _write_to_db(self):
        active_events_count = self.get_active_events_count()
        monthly_active_users = self.get_monthly_active_users()

        # Combine metrics
        metrics = {}
        metrics.update(active_events_count)
        metrics.update(monthly_active_users)

        org_id_result = self.read_from_postgres(
            Constants.fetch_org_id(self.client_name)
        )
        if org_id_result.count() == 0:
            raise ValueError(
                f"No organization found for client {self.client_name}. Cannot update metering information."
            )
        self.org_id = org_id_result.collect()[0]["org_id"]
        check_query = Constants.fetch_metering_info(self.org_id)

        result_df = self.read_from_postgres(check_query)
        if result_df.count() > 0:
            current_meter = result_df.collect()[0]["meter"]
            if current_meter:
                meter_json = json.loads(current_meter)
                meter_json.update(metrics)
                new_meter = json.dumps(meter_json)
            else:
                new_meter = json.dumps(metrics)

            # Update the existing record
            update_sql = f"""
                UPDATE sonic.metering
                SET meter = '{new_meter}'
                WHERE org_id = '{self.org_id}'
            """
            self.run_postgres_query(update_sql)
        else:
            # Insert new record
            insert_sql = f"""
                INSERT INTO sonic.metering (org_id, meter, active)
                VALUES ('{self.org_id}', '{json.dumps(metrics)}', true)
            """
            self.run_postgres_query(insert_sql)
