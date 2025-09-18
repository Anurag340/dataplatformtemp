from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from propel_data_platform.transformations.base import BaseTransform


class CustomerioBaseTransform(BaseTransform):
    pass


# Define the schema explicitly

target_schema_campaign_details = StructType(
    [
        StructField("campaign_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("channels", StringType(), True),
        StructField("actions", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("first_started", TimestampType(), True),
        StructField("deduplicate_id", StringType(), True),
        StructField("trigger_segment_ids", StringType(), True),
        StructField("tags", StringType(), True),
    ]
)

target_schema_newsletter_details = StructType(
    [
        StructField("newsletter_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("type", StringType(), True),
        StructField("created", TimestampType(), True),
        StructField("sent_at", TimestampType(), True),
        StructField("updated", TimestampType(), True),
        StructField("content_ids", StringType(), True),
        StructField("deduplicate_id", StringType(), True),
    ]
)

target_schema_transactional_message_details = StructType(
    [
        StructField("transactional_message_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("send_to_unsubscribed", BooleanType(), True),
        StructField("link_tracking", BooleanType(), True),
        StructField("open_tracking", BooleanType(), True),
        StructField("hide_message_body", BooleanType(), True),
        StructField("queue_drafts", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


target_schema_people_details = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("internal_customer_id", StringType(), True),
        StructField("deleted", BooleanType(), True),
        StructField("suppressed", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


campaign_details_desired_columns = [
    "campaign_id",
    "name",
    "state",
    "active",
    "channels",
    "actions",
    "created_at",
    "updated_at",
    "first_started",
    "deduplicate_id",
    "trigger_segment_ids",
    "tags",
]

newsletter_details_desired_columns = [
    "newsletter_id",
    "name",
    "tags",
    "type",
    "created",
    "sent_at",
    "updated",
    "content_ids",
    "deduplicate_id",
]

transactional_message_details_desired_columns = [
    "transactional_message_id",
    "name",
    "description",
    "send_to_unsubscribed",
    "link_tracking",
    "open_tracking",
    "hide_message_body",
    "queue_drafts",
    "created_at",
    "updated_at",
]

api_broadcast_details_desired_columns = [
    "api_broadcast_id",
    "deduplicate_id",
    "name",
    "type",
    "created",
    "updated",
    "active",
    "state",
    "actions",
    "first_started",
    "tags",
    "channels",
]

journey_metrics_desired_columns = [
    "internal_customer_id",
    "playbook_id",
    "type",
    "channel",
    "playbook_type",
    "explanation",
    "metric",
    "delay_ends_at",
    "started_campaign_at",
    "created_at",
]

channel_metrics_desired_columns = [
    "internal_customer_id",
    "playbook_id",
    "type",
    "channel",
    "channel_name",
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
    "created_at",
]

channel_metrics_metric_desired_columns = [
    "internal_customer_id",
    "playbook_id",
    "type",
    "channel",
    "channel_name",
    "created_at",
]

people_details_desired_columns = [
    "user_id",
    "internal_customer_id",
    "deleted",
    "suppressed",
    "created_at",
    "updated_at",
]

channel_metrics = {
    "ios_push": {
        "sends": "sent",
        "opens": "total_opens",
        "clicks": "body_clicks",
        "delivered": "",
    },
    "android_push": {
        "sends": "sent",
        "opens": "total_opens",
        "clicks": "body_clicks",
        "delivered": "",
    },
    "trigger_in_app_message": {
        "sends": "",
        "opens": "impressions",
        "clicks": "clicks",
        "delivered": "",
    },
    "email": {
        "sends": "sent",
        "opens": "unique_opens",
        "clicks": "unique_clicks",
        "delivered": "delivered",
    },
    "sms": {"sends": "sent", "opens": "", "clicks": "clicks", "delivered": "delivered"},
    "webhook": {"sends": "sent", "opens": "", "clicks": "", "delivered": ""},
    "whats_app": {
        "sends": "sent",
        "opens": "read",
        "clicks": "",
        "delivered": "delivered",
    },
    # Add other channels as needed
}
