import logging

logger = logging.getLogger(__name__)


class Constants:

    @staticmethod
    def fetch_canvas_data_series(environment, client_name):
        logger.info("Getting canvas data series")
        return f"SELECT canvas_id, start_date, stats from {environment}.{client_name}.braze_canvas_data_series_view"

    @staticmethod
    def fetch_canvas_details(environment, client_name):
        logger.info("Getting canvas details")
        return f"SELECT canvas_id, data from {environment}.{client_name}.braze_canvas_details_view"

    @staticmethod
    def fetch_campaign_data_series(environment, client_name):
        logger.info("Getting campaign data series")
        return f"SELECT campaign_id, start_date, data from {environment}.{client_name}.braze_campaigns_data_series_view"

    @staticmethod
    def fetch_campaign_details(environment, client_name):
        logger.info("Getting campaign details")
        return f"SELECT campaign_id, data from {environment}.{client_name}.braze_campaigns_details_view"

    @staticmethod
    def read_table(table):
        return f"SELECT * from {table}"

    @staticmethod
    def fetch_user_playbook_campaign_data(environment, client_name):
        logger.info("Getting user_playbook_campaign_data")
        return f"SELECT user_id, campaigns_received from {environment}.{client_name}.braze_user_workflow_data_view"

    @staticmethod
    def fetch_user_playbook_canvas_data(environment, client_name):
        logger.info("Getting user_playbook_canvas_data")
        return f"SELECT user_id, canvases_received from {environment}.{client_name}.braze_user_workflow_data_view"

    @staticmethod
    def fetch_user_data(environment, client_name):
        logger.info("Getting user_data")
        return f"SELECT user_id, custom_attributes, purchases, dob, country, home_city, language, gender, push_subscribe, push_opted_in_at, email_subscribe, email_opted_in_at, devices, apps from {environment}.{client_name}.braze_user_workflow_data_view"

    @staticmethod
    def fetch_customerio_campaign_details(environment, client_name):
        logger.info("Getting customerio_campaign_details")
        return f"SELECT id campaign_id, name, tags, type, state, active, actions, created, updated, first_started, deduplicate_id, trigger_segment_ids from {environment}.{client_name}.customerio_campaigns"

    @staticmethod
    def fetch_customerio_newsletter_details(environment, client_name):
        logger.info("Getting customerio_newsletter_details")
        return f"SELECT id newsletter_id, name, tags, type, created, sent_at, updated, content_ids, deduplicate_id, recipient_segment_ids from {environment}.{client_name}.customerio_newsletters"

    @staticmethod
    def fetch_customerio_transactional_message_details(environment, client_name):
        logger.info("Getting customerio_transactional_message_details")
        return f"SELECT id transactional_message_id, name, description, created_at, updated_at, queue_drafts, link_tracking, open_tracking, hide_message_body, send_to_unsubscribed from {environment}.{client_name}.customerio_transactional"

    @staticmethod
    def fetch_api_broadcast_details(environment, client_name):
        logger.info("Getting api_broadcast_details")
        return f"SELECT distinct id api_broadcast_id, name, deduplicate_id, type, created, updated, active, state, actions, first_started, tags from {environment}.{client_name}.api_broadcasts"

    @staticmethod
    def read_metrics_table(table, type):
        return f"SELECT * from {table} where type = '{type}'"

    @staticmethod
    def get_metrics_table_count(table, type):
        return f"SELECT count(*) count from {table} where type = '{type}'"

    @staticmethod
    def fetch_customerio_channel_metrics(
        environment, client_name, last_run_id, time_diff_query
    ):
        logger.info("Getting customerio_channel_metrics")
        return f"""select
                distinct d.internal_customer_id,
                (case
                    when campaign_id is not null then campaign_id
                    when newsletter_id is not null then newsletter_id
                    when transactional_message_id is not null then transactional_message_id
                    end
                ) playbook_id,
                (case
                    when campaign_id is not null then 'campaign'
                    when newsletter_id is not null then 'newsletter'
                    when transactional_message_id is not null then 'transactional_message'
                    end
                ) type,
                ifnull(action_id, content_id) channel,
                d.delivery_type channel_name, 
                m.metric,
                (m.created_at {time_diff_query}) created_at
                from {environment}.{client_name}.customerio_channel_deliveries d, {environment}.{client_name}.customerio_channel_metrics m
                where d.delivery_id = m.delivery_id
                and m.id > '{last_run_id}'
                group by 
                    internal_customer_id,
                    playbook_id,
                    type,
                    channel,
                    channel_name, 
                    m.metric, 
                    (m.created_at {time_diff_query});"""

    @staticmethod
    def fetch_customerio_processed_files(environment, client_name, type):
        logger.info("Getting processed_files")
        return f"SELECT file_name from {environment}.{client_name}.customerio_processed_files where type = '{type}'"

    @staticmethod
    def fetch_client_id(enviornment, client_name):
        return f"SELECT client_id from {enviornment}.public.client where client_name = '{client_name}'"

    @staticmethod
    def fetch_last_workflow_run(client_name, environment, workflow_name):
        return f"SELECT last_run_at, last_run_reference from {environment}.public.workflow_run_history where workflow = '{workflow_name}' and client_name = '{client_name}' order by last_run_at desc limit 1"

    @staticmethod
    def fetch_all_workflows(client_name, environment, workflow_names):
        """
        Fetch last run history for multiple workflows for a given client.

        Args:
            client_name (str): Name of the client
            environment (str): Environment name (dev/prod)
            workflow_names (list): List of workflow names to fetch

        Returns:
            str: SQL query to fetch workflow run history
        """
        workflow_list = "', '".join(workflow_names)
        return f"""
            SELECT workflow, last_run_at, last_run_reference, status 
            FROM {environment}.public.workflow_run_history 
            WHERE workflow IN ('{workflow_list}') 
            AND client_name = '{client_name}' 
            ORDER BY last_run_at DESC
        """

    @staticmethod
    def insert_workflow_run_history(
        client_name, environment, workflow_name, last_run_at, last_run_reference=None
    ):
        return f"INSERT INTO {environment}.public.workflow_run_history (client_name, workflow, last_run_at, last_run_reference, status) VALUES ('{client_name}', '{workflow_name}', {last_run_at}, '{last_run_reference}', 'success')"

    @staticmethod
    def fetch_last_inserted_id(client_name, environment, table_name):
        return f"SELECT max(id) as last_inserted_id from {environment}.{client_name}.{table_name}"

    @staticmethod
    def fetch_customerio_journey_metrics(environment, client_name, last_run_id):
        logger.info("Getting customerio_channel_metrics")
        return f"""select s.internal_customer_id, s.campaign_id playbook_id, 'campaign' type,
                o.action_id channel, s.campaign_type playbook_type, o.explanation, o.output_type metric,
                to_timestamp(o.delay_ends_at, 'yyyy-MM-dd HH:mm:ss') delay_ends_at, 
                to_timestamp(s.started_campaign_at, 'yyyy-MM-dd HH:mm:ss') started_campaign_at, 
                to_timestamp(o.created_at, 'yyyy-MM-dd HH:mm:ss') created_at
                from {environment}.{client_name}.customerio_journey_outputs o, {environment}.{client_name}.customerio_journey_subjects s
                where o.subject_name = s.subject_name
                and o.id > {last_run_id};"""

    @staticmethod
    def fetch_customerio_people_details(environment, client_name):
        logger.info("Getting customerio_people_details")
        return f"""SELECT customer_id AS user_id, internal_customer_id, 
                CASE WHEN deleted = 'false' THEN FALSE ELSE TRUE END AS deleted, 
                CASE WHEN suppressed = 'false' THEN FALSE ELSE TRUE END AS suppressed, 
                CASE WHEN created_at = '1970-01-01 00:00:00.000' THEN null WHEN created_at is null THEN null ELSE to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') END AS created_at, 
                to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss') updated_at
                from {environment}.{client_name}.customerio_people_view"""

    @staticmethod
    def fetch_amplitude_event_details(environment, client_name, from_time):
        logger.info("Getting amplitude event details")
        if from_time:
            return f"SELECT ae.*, ael.event_type_id from {environment}.{client_name}.amplitude_events ae, {environment}.{client_name}.amplitude_events_list ael where ae.server_upload_time > '{from_time}' and ae.user_id is not null and ael.event_type = ae.event_type and ael.is_active is true"
        else:
            return f"SELECT ae.*, ael.event_type_id from {environment}.{client_name}.amplitude_events ae, {environment}.{client_name}.amplitude_events_list ael where ae.user_id is not null and ael.event_type = ae.event_type and ael.is_active is true"

    @staticmethod
    def fetch_amplitude_pg_event_details(client_name):
        logger.info("Getting amplitude event details")
        return f"SELECT max(server_upload_time) as event_time from {client_name}.amplitude_events"

    @staticmethod
    def fetch_amplitude_events_list(environment, client_name):
        logger.info("Getting amplitude events list")
        return f"SELECT distinct event_type_id, event_type from {environment}.{client_name}.amplitude_events_list where is_active is true"

    @staticmethod
    def fetch_amplitude_last_server_upload_time(client_name, environment):
        logger.info("Getting amplitude last server upload time")
        return f"SELECT max(server_upload_time) as server_upload_time from {environment}.{client_name}.amplitude_events"

    @staticmethod
    def fetch_campaign_list_details(environment, client_name):
        logger.info("Getting campaign list details")
        return f"SELECT distinct event_type, event_type_id from {environment}.{client_name}.mixpanel_events_list where is_active is true"

    @staticmethod
    def fetch_amplitude_active_events_count(environment, client_name):
        """Get the count of distinct event_type_id where is_active is true"""
        logger.info("Getting amplitude active events count")
        return f"""
            SELECT COUNT(event_type_id) as active_events_count 
            FROM {environment}.{client_name}.amplitude_events_list
            WHERE is_active = true
        """

    @staticmethod
    def fetch_amplitude_monthly_active_users(client_name):
        """Get the count of unique users from the last 30 days (D-2 to D-32)"""
        logger.info("Getting amplitude monthly active users")
        return f"""
            SELECT COUNT(DISTINCT user_id_id) as mau_count 
            FROM {client_name}.amplitude_events
            WHERE to_timestamp(time) >= CURRENT_DATE - INTERVAL '32 days'
            AND to_timestamp(time) < CURRENT_DATE - INTERVAL '2 days'
        """

    @staticmethod
    def fetch_metering_info(org_id):
        """Get the current metering information for a client"""
        logger.info("Getting metering information")
        return f"""
            SELECT meter
            FROM sonic.metering
            WHERE org_id = '{org_id}'
            and active = true
        """

    @staticmethod
    def fetch_mixpanel_active_events_count(environment, client_name):
        """Get the count of distinct event_type_id where is_active is true from mixpanel_events_list"""
        logger.info("Getting mixpanel active events count")
        return f"""
            SELECT COUNT(DISTINCT event_type_id) as active_events_count 
            FROM {environment}.{client_name}.mixpanel_events_list
            WHERE is_active = true
        """

    @staticmethod
    def fetch_mixpanel_monthly_active_users(client_name):
        """Get the count of unique users from the last 30 days (D-2 to D-32)"""
        logger.info("Getting mixpanel monthly active users")
        return f"""
            SELECT COUNT(DISTINCT distinct_id) as mau_count 
            FROM {client_name}.mixpanel_events
            WHERE to_timestamp(time) >= CURRENT_DATE - INTERVAL '32 days'
            AND to_timestamp(time) < CURRENT_DATE - INTERVAL '2 days'
        """
        return f"SELECT distinct event_type, event_type_id from {environment}.{client_name}.mixpanel_events_list where is_active is true"

    @staticmethod
    def fetch_onboarding_sync_status(environment):
        logger.info("Getting onboarding sync status")
        return f"SELECT client_name FROM {environment}.public.client_onboarding_status where onboarding_status = 'success' "

    @staticmethod
    def fetch_org_id(client_name):
        logger.info("Getting org_id")
        return f"SELECT id as org_id FROM sonic.organisation WHERE domain = '{client_name}' and active is true"
