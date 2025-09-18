import json
import logging

from pyspark.sql.functions import col, countDistinct

from propel_data_platform.transformations.amplitude.base import AmplitudeBaseTransform
from propel_data_platform.utils.constants import Constants

logger = logging.getLogger(__name__)


class AmplitudeEventsMeteringTransform(AmplitudeBaseTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.validate_params(**kwargs)
        self.spark = super().initiate_spark_session()
        self.pg_spark = super().initiate_pg_spark_session()
        self.client_name = kwargs.get("config", {}).get("client")
        self.environment = kwargs.get("config", {}).get("environment")
        self.workflow_name = kwargs.get("config", {}).get("current-workflow-name")
        self.skip_metering = kwargs.get("config", {}).get("skip-metering", False)

    def validate_params(self, **kwargs):
        super().validate_params(**kwargs)

    def _table_exists(self, table_name):
        schema = self.client_name
        query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            ) AS table_exists
        """
        result = self.load_pg_data(query)
        return result.collect()[0]["table_exists"]

    def get_active_events_count(self):
        if not (self._table_exists("amplitude_events_list") and self._table_exists("amplitude_events")):
            logger.warning("Required tables amplitude_events_list or amplitude_events do not exist. Returning 0.")
            return {"active_events": 0}
        query = Constants.fetch_amplitude_active_events_count(self.environment, self.client_name)
        result = self.spark.sql(query)
        count = result.collect()[0]["active_events_count"]
        return {"active_events": count}

    def get_monthly_active_users(self):
        if not (self._table_exists("amplitude_events_list") and self._table_exists("amplitude_events")):
            logger.warning("Required tables amplitude_events_list or amplitude_events do not exist. Returning 0.")
            return {"monthly_active_users": 0}
        query = Constants.fetch_amplitude_monthly_active_users(self.client_name)
        result_df = self.load_pg_data(query)
        count = result_df.collect()[0]["mau_count"]
        return {"monthly_active_users": count}

    def update_metering_info(self):
        """
        Update the metering information in sonic.metering table
        """
        # Get counts
        active_events_count = self.get_active_events_count()
        monthly_active_users = self.get_monthly_active_users()

        # Combine metrics
        metrics = {}
        metrics.update(active_events_count)
        metrics.update(monthly_active_users)

        # Check if the metering record exists for the client
        org_id_result = self.load_pg_data(Constants.fetch_org_id(self.client_name))
        if org_id_result.count() == 0:
            logger.error(f"No organization found for client {self.client_name}. Cannot update metering information.")
            raise ValueError(f"No organization found for client {self.client_name}. Cannot update metering information.")
        self.org_id = org_id_result.collect()[0]["org_id"]
        check_query = Constants.fetch_metering_info(self.org_id)

        try:
            result_df = self.load_pg_data(check_query)
            if result_df.count() > 0:
                current_meter = result_df.collect()[0]["meter"]
                if current_meter:
                    meter_json = json.loads(current_meter)
                    # Update the metrics
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
                self.merge_in_pg(update_sql)
                logger.info(f"Updated metering information for client {self.client_name}")
            else:
                # Insert new record
                insert_sql = f"""
                    INSERT INTO sonic.metering (org_id, meter, active)
                    VALUES ('{self.org_id}', '{json.dumps(metrics)}', true)
                """
                self.merge_in_pg(insert_sql)
                logger.info(f"Inserted new metering information for client {self.client_name}")

        except Exception as e:
            logger.error(f"Error updating metering information: {e}")
            raise

    def transform(self):
        """
        Main transform method that updates the metering information
        """
        self.update_metering_info()

    def write_to_db(self):
        """
        This class doesn't need to write to DB as it's handled in update_metering_info
        """
        if self.skip_metering:
            logger.info("Skipping metering information update.")
            return
        self.transform()
        self.spark.sql(
            Constants.insert_workflow_run_history(
                self.client_name,
                self.environment,
                self.workflow_name,
                "now()"
            )
        )
        logger.info(f"Workflow run history updated for {self.workflow_name} workflow.")
        logger.info("Metering information updated successfully.")
