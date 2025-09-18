from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from propel_data_platform.transformations.base import BaseTransform


class BrazeBaseTransform(BaseTransform):
    pass


target_schema_campaign_details = StructType(
    [
        StructField("campaign_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_archived", BooleanType(), True),
        StructField("is_enabled", BooleanType(), True),
        StructField("is_draft", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("schedule_type", StringType(), True),
        StructField("channels", StringType(), True),
        StructField("first_sent", TimestampType(), True),
        StructField("last_sent", TimestampType(), True),
        StructField("messages", IntegerType(), False),
        StructField("goal", StringType(), True),
        StructField("stage", StringType(), True),
    ]
)

target_schema_variants = StructType(
    [
        StructField("variant_id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("time", TimestampType(), True),
        StructField("variant_name", StringType(), True),
        StructField("total_entries", IntegerType(), False),
        StructField("revenue", DoubleType(), False),
        StructField("unique_recipients", IntegerType(), False),
        StructField("conversions", IntegerType(), False),
        StructField("is_control_group", BooleanType(), False),
    ]
)

target_schema_canvas_details = StructType(
    [
        StructField("canvas_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_archived", BooleanType(), True),
        StructField("is_enabled", BooleanType(), True),
        StructField("is_draft", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("schedule_type", StringType(), True),
        StructField("channels", StringType(), True),
        StructField("first_sent", TimestampType(), True),
        StructField("last_sent", TimestampType(), True),
        StructField("messages", IntegerType(), False),
        StructField("goal", StringType(), True),
        StructField("stage", StringType(), True),
    ]
)

target_schema_canvas_steps = StructType(
    [
        StructField("time", TimestampType(), True),
        StructField("canvas_id", StringType(), True),
        StructField("step_id", StringType(), True),
        StructField("step_name", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("channels", StringType(), True),
        StructField("unique_recipients", IntegerType(), True),
        StructField("sent", IntegerType(), True),
        StructField("total_opens", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
    ]
)

target_schema_channels_canvas = StructType(
    [
        StructField("time", TimestampType(), True),
        StructField("channel_name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("step_id", StringType(), True),
        StructField("sends", IntegerType(), True),
        StructField("opens", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("delivered", IntegerType(), True),
    ]
)

target_schema_channels_campaign = StructType(
    [
        StructField("time", TimestampType(), True),
        StructField("channel_name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("sends", IntegerType(), True),
        StructField("opens", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("delivered", IntegerType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("revenue", DoubleType(), True),
    ]
)

target_schema_user_playbook_data = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField(
            "type", StringType(), True, {"allowedValues": ["campaign", "canvas"]}
        ),
        StructField("user_id", StringType(), False),
        StructField("last_received", TimestampType(), True),
        StructField("in_control", BooleanType(), True),
        StructField("converted", BooleanType(), True),
        StructField("conversion_events_performed", StringType(), True),
        StructField("message_status", StringType(), True),
    ]
)

target_schema_user_playbook_steps_data = StructType(
    [
        StructField("step_id", StringType(), False),
        StructField("step_name", StringType(), True),
        StructField("playbook_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("last_received", TimestampType(), True),
    ]
)

campaign_details_desired_columns = [
    "campaign_id",
    "name",
    "description",
    "is_archived",
    "is_enabled",
    "is_draft",
    "created_at",
    "updated_at",
    "schedule_type",
    "channels",
    "first_sent",
    "last_sent",
    "messages",
    "goal",
    "stage",
]

canvas_details_desired_columns = [
    "canvas_id",
    "name",
    "description",
    "is_archived",
    "is_enabled",
    "is_draft",
    "created_at",
    "updated_at",
    "schedule_type",
    "channels",
    "first_sent",
    "last_sent",
    "messages",
    "goal",
    "stage",
]

variants_desired_columns = [
    "variant_id",
    "type",
    "id",
    "time",
    "variant_name",
    "total_entries",
    "revenue",
    "unique_recipients",
    "conversions",
    "is_control_group",
]

steps_desired_columns = [
    "time",
    "canvas_id",
    "step_id",
    "step_name",
    "revenue",
    "conversions",
    "channels",
    "unique_recipients",
    "sent",
    "total_opens",
    "clicks",
]

channels_desired_columns_campaigns = [
    "time",
    "channel_name",
    "type",
    "id",
    "sends",
    "opens",
    "clicks",
    "delivered",
    "conversions",
    "revenue",
]

channels_desired_columns_canvas = [
    "time",
    "channel_name",
    "type",
    "id",
    "step_id",
    "sends",
    "opens",
    "clicks",
    "delivered",
]

user_playbook_data_desired_columns = [
    "id",
    "name",
    "type",
    "user_id",
    "last_received",
    "in_control",
    "converted",
    "conversion_events_performed",
    "message_status",
]

user_playbook_steps_data_desired_columns = [
    "step_id",
    "step_name",
    "playbook_id",
    "user_id",
    "last_received",
]

user_data_desired_columns = [
    "user_id",
    "custom_attributes",
    "purchases",
    "dob",
    "country",
    "home_city",
    "language",
    "gender",
    "push_subscribe",
    "push_opted_in_at",
    "email_subscribe",
    "email_opted_in_at",
    "devices",
    "apps",
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
    "content_cards": {
        "sends": "sent",
        "opens": "total_impressions",
        "clicks": "total_clicks",
        "delivered": "",
    },
    # Add other channels as needed
}
