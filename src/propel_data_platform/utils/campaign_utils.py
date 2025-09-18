import json
import logging

from propel_data_platform.utils.tag_utils import TagUtils
from propel_data_platform.utils.time_utils import DateUtils

get_sent_value = lambda x: int(x) if x is not None else 0

get_sent_value_float = lambda x: float(x) if x is not None else 0.0

logger = logging.getLogger(__name__)


class CampaignUtils:
    @staticmethod
    def extract_campaign_variants(campaign_data_series, campaign_id, channel_metrics):
        json_data = [json.loads(campaign_data_series)]
        variations = []
        for d in json_data:
            time = d.get("time")
            messages = d.get("messages")
            for key in messages:
                variant_list = messages.get(key)
                for variants in variant_list:
                    sent = 0
                    if channel_metrics.get(key) is not None:
                        sent = get_sent_value(
                            variants.get(
                                channel_metrics.get(key).get("sends", "Not found")
                            )
                        )
                    variant = {
                        "id": campaign_id,
                        "type": "campaign",
                        "time": DateUtils.convert_to_timestamp(time),
                        "variant_name": variants.get("variation_name", ""),
                        "variant_id": variants.get("variation_api_id"),
                        "total_entries": sent,
                        "revenue": get_sent_value_float(variants.get("revenue")),
                        "unique_recipients": get_sent_value(
                            variants.get("unique_recipients")
                        ),
                        "conversions": get_sent_value(variants.get("conversions")),
                        "is_control_group": variants.get(
                            "variation_name", ""
                        ).startswith("Control"),
                    }
                    # print(variant)
                    variations.append(variant)

        return variations

    @staticmethod
    def extract_campaigns(campaign_details_data, campaign_id):
        data = json.loads(campaign_details_data)
        messages = data.get("messages", {})
        return {
            "campaign_id": campaign_id,
            "name": data.get("name", ""),
            "description": data.get("description", ""),
            "is_archived": bool(data.get("archived", False)),
            "is_enabled": bool(data.get("enabled", False)),
            "is_draft": bool(data.get("draft", False)),
            "messages": sum(
                1
                for key, value in messages.items()
                if isinstance(value.get("name"), str)
                and not value["name"].startswith("Control")
            ),
            "created_at": DateUtils.convert_to_timestamp(data.get("created_at")),
            "updated_at": DateUtils.convert_to_timestamp(data.get("updated_at")),
            "schedule_type": data.get("schedule_type", ""),
            "channels": ",".join(data.get("channels", [])),  # comma seperated values.
            "first_sent": DateUtils.convert_to_timestamp(data.get("first_sent")),
            "last_sent": DateUtils.convert_to_timestamp(data.get("last_sent")),
            "goal": TagUtils.extract_goal(data.get("tags")),
            "stage": TagUtils.extract_stage(data.get("tags")),
        }

    @staticmethod
    def get_channel_object(time, campaign_id, messages, channel_metrics):
        time = time
        campaign_id = campaign_id
        channels = []
        if messages is not None:
            for channel, channel_details in messages.items():
                channel = channel
                sends = 0
                opens = 0
                clicks = 0
                delivered = 0
                conversions = 0
                revenue = 0
                # print("Okay")
                if channel_metrics.get(channel) is not None:
                    for i in range(len(channel_details)):
                        send = channel_details[i].get(
                            channel_metrics.get(channel).get("sends", "Not found"), 0
                        )
                        # print(
                        #     f"channel_metrics_send = {channel_metrics.get(channel).get('sends', 'Not found')}"
                        # )
                        # print(f"Step {i} and sends = {sends}")
                        open = channel_details[i].get(
                            channel_metrics.get(channel).get("opens", "Not found"), 0
                        )
                        click = channel_details[i].get(
                            channel_metrics.get(channel).get("clicks", "Not found"), 0
                        )
                        deliver = channel_details[i].get(
                            channel_metrics.get(channel).get("delivered", "Not found"),
                            0,
                        )
                        conversion = channel_details[i].get("conversions", 0)
                        rev = channel_details[i].get("revenue", 0.0)
                        sends += send
                        # print(f"total_sends at the end of {i} = {sends}")
                        opens += open
                        clicks += click
                        delivered += deliver
                        conversions += conversion
                        revenue += rev
                channel_dict = {
                    "time": DateUtils.convert_to_timestamp(time),
                    "channel_name": channel,
                    "type": "campaign",
                    "id": campaign_id,
                    "sends": get_sent_value(sends),
                    "opens": get_sent_value(opens),
                    "clicks": get_sent_value(clicks),
                    "delivered": get_sent_value(delivered),
                    "conversions": get_sent_value(conversions),
                    "revenue": get_sent_value_float(revenue),
                }
                channels.append(channel_dict)
        return channels

    @staticmethod
    def extract_channels(campaign_data_series_stats, campaign_id, channel_metrics):
        json_data = [json.loads(campaign_data_series_stats)]
        channels = []
        for d in json_data:
            time = d.get("time")
            messages = d.get("messages")
            channels.extend(
                CampaignUtils.get_channel_object(
                    time, campaign_id, messages, channel_metrics
                )
            )
        return channels

    @staticmethod
    def extract_user_campaign_details(user_id, campaigns_received):
        json_data = json.loads(campaigns_received)
        user_campaigns = []
        for data in json_data:
            name = data.get("name")
            id = data.get("api_campaign_id")
            type = "campaign"
            last_received = DateUtils.convert_to_timestamp(data.get("last_received"))
            in_control = data.get("in_control")
            converted = data.get("converted", None)
            conversion_events_performed = (
                json.dumps(data.get("multiple_converted", None))
                if data.get("multiple_converted")
                else None
            )
            message_status = (
                json.dumps(data.get("engaged", None)) if data.get("engaged") else None
            )
            user_campaign = {
                "id": id,
                "name": name,
                "type": type,
                "user_id": user_id,
                "last_received": last_received,
                "in_control": in_control,
                "converted": converted,
                "conversion_events_performed": conversion_events_performed,
                "message_status": message_status,
            }
            user_campaigns.append(user_campaign)
        return user_campaigns

    @staticmethod
    def extract_customerio_campaigns(campaign_details_data, campaign_id):
        data = campaign_details_data

        _actions = json.loads(data["actions"]) if data["actions"] is not None else []
        channels = []

        if len(_actions) > 0:
            for action in _actions:
                channels.append(action["type"])

        return {
            "campaign_id": int(campaign_id),
            "name": data["name"] or "",
            "state": data["state"],
            "active": data["active"],
            "channels": list(set(channels)),
            "actions": data["actions"],
            "created_at": DateUtils.convert_epoch_to_timestamp(int(data["created"])),
            "updated_at": DateUtils.convert_epoch_to_timestamp(int(data["updated"])),
            "first_started": DateUtils.convert_epoch_to_timestamp(
                int(data["first_started"])
            ),
            "deduplicate_id": data["deduplicate_id"],
            "trigger_segment_ids": data["trigger_segment_ids"],
            "tags": data["tags"],
        }
