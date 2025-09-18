import json
import logging

from propel_data_platform.utils.tag_utils import TagUtils
from propel_data_platform.utils.time_utils import DateUtils

get_sent_value = lambda x: int(x) if x is not None else 0

get_sent_value_float = lambda x: float(x) if x is not None else 0.0

logger = logging.getLogger(__name__)


class CanvasUtils:

    @staticmethod
    def get_messages_length(steps):
        total_messages = 0
        if steps is not None:
            for step in steps:
                if step is not None and "messages" in step:
                    if not step.get("name").startswith("Control"):
                        total_messages += len(step["messages"])
                else:
                    continue
        return total_messages

    @staticmethod
    def get_variant_object(time, canvas_id, variant_id, variant_stats):
        variant = {
            "variant_id": variant_id,
            "id": canvas_id,
            "type": "canvas",
            "time": DateUtils.convert_to_timestamp(time),
            "variant_name": variant_stats.get("name"),
            "total_entries": get_sent_value(variant_stats.get("entries")),
            "revenue": get_sent_value_float(variant_stats.get("revenue")),
            "unique_recipients": 0,  # intentionally left blank to match the schema
            "conversions": get_sent_value(variant_stats.get("conversions")),
            "is_control_group": (
                True if variant_stats.get("name", "").startswith("Control") else False
            ),
        }
        # logger.info("variant output")
        # logger.info(f"{variant}")
        return variant

    @staticmethod
    def get_steps_object(time, canvas_id, step_id, step_stat, channel_metrics):
        steps = []
        # logger.info("Inside get_steps_object")
        # logger.info(f"step_stat: {step_stat}")
        canvas_id = canvas_id
        step_id = step_id
        step_name = step_stat.get("name")
        revenue = get_sent_value_float(step_stat.get("revenue"))
        conversions = get_sent_value(step_stat.get("conversions"))
        messages = step_stat.get("messages")
        unique_recipients = step_stat.get("unique_recipients")
        channels = []
        sent = 0
        total_opens = 0
        clicks = 0
        if messages:
            for key in messages:
                channels.append(key)
                messages_list = messages.get(key)
                for m in messages_list:
                    if channel_metrics.get(key) is not None:
                        sent += get_sent_value(
                            m.get(channel_metrics.get(key).get("sends", "Not found"))
                        )
                        total_opens += get_sent_value(
                            m.get(channel_metrics.get(key).get("opens", "Not found"))
                        )
                        clicks += get_sent_value(
                            m.get(channel_metrics.get(key).get("clicks", "Not found"))
                        )

        step = {
            "time": DateUtils.convert_to_timestamp(time),
            "canvas_id": canvas_id,
            "step_id": step_id,
            "step_name": step_name,
            "revenue": revenue,
            "conversions": conversions,
            "channels": ",".join(channels),
            "sent": get_sent_value(sent),
            "total_opens": get_sent_value(total_opens),
            "clicks": get_sent_value(clicks),
            "unique_recipients": unique_recipients,
        }
        steps.append(step)
        return steps

    @staticmethod
    def get_channel_object(step_key, time, canvas_id, messages, channel_metrics):
        time = time
        canvas_id = canvas_id
        channels = []
        step_key = step_key
        # Iterate through the messages
        for channel, channel_details in messages.items():
            channel = channel
            sends = 0
            opens = 0
            clicks = 0
            delivered = 0
            if channel_metrics.get(channel) is not None:
                for i in range(len(channel_details)):
                    send = channel_details[i].get(
                        channel_metrics.get(channel).get("sends", "Not found"), 0
                    )
                    open = channel_details[i].get(
                        channel_metrics.get(channel).get("opens", "Not found"), 0
                    )
                    click = channel_details[i].get(
                        channel_metrics.get(channel).get("clicks", "Not found"), 0
                    )
                    deliver = channel_details[i].get(
                        channel_metrics.get(channel).get("delivered", "Not found"), 0
                    )
                    sends += send
                    opens += open
                    clicks += click
                    delivered += deliver
            channel_dict = {
                "time": DateUtils.convert_to_timestamp(time),
                "channel_name": channel,
                "type": "canvas",
                "id": canvas_id,
                "step_id": step_key,
                "sends": get_sent_value(sends),
                "opens": get_sent_value(opens),
                "clicks": get_sent_value(clicks),
                "delivered": get_sent_value(delivered),
            }
            channels.append(channel_dict)
        return channels

    @staticmethod
    def extract_variants(canvas_data_series_stats, canvas_id):
        stats = [json.loads(canvas_data_series_stats)]
        variations = []
        for stat in stats:
            time = stat.get("time")  # needed.
            variant_stats = stat.get("variant_stats")
            for key in variant_stats:
                variations.append(
                    CanvasUtils.get_variant_object(
                        time, canvas_id, key, variant_stats[key]
                    )
                )

        return variations

    @staticmethod
    def extract_channels(canvas_data_series_stats, canvas_id, channel_metrics):
        stats = [json.loads(canvas_data_series_stats)]
        channels = []
        for stat in stats:
            time = stat.get("time")
            step_stats = stat.get("step_stats")
            for step_key, step_data in step_stats.items():
                messages = step_data.get("messages", {})
                channels.extend(
                    CanvasUtils.get_channel_object(
                        step_key, time, canvas_id, messages, channel_metrics
                    )
                )
        return channels

    @staticmethod
    def extract_steps(canvas_data_series_stats, canvas_id, channel_metrics):
        stats = [json.loads(canvas_data_series_stats)]
        steps = []
        for stat in stats:
            time = stat.get("time")  # needed.
            step_stats = stat.get("step_stats")
            for key in step_stats:
                steps.extend(
                    CanvasUtils.get_steps_object(
                        time, canvas_id, key, step_stats[key], channel_metrics
                    )
                )
        return steps

    @staticmethod
    def extract_canvas_details(data, canvas_id):
        data = json.loads(data)
        data = data.get("data")  # This is a nested data object.
        return {
            "canvas_id": canvas_id,
            "name": data.get("name", ""),
            "description": data.get("description", ""),
            "is_archived": bool(data.get("archived", False)),
            "is_enabled": bool(data.get("enabled", False)),
            "is_draft": bool(data.get("draft", False)),
            "messages": CanvasUtils.get_messages_length(data.get("steps")),
            "created_at": DateUtils.convert_to_timestamp(data.get("created_at")),
            "updated_at": DateUtils.convert_to_timestamp(data.get("updated_at")),
            "schedule_type": data.get("schedule_type", ""),
            "channels": ",".join(data.get("channels", [])),  # comma seperated values.
            "first_sent": DateUtils.convert_to_timestamp(data.get("first_entry")),
            "last_sent": DateUtils.convert_to_timestamp(data.get("last_entry")),
            "goal": TagUtils.extract_goal(data.get("tags")),
            "stage": TagUtils.extract_stage(data.get("tags")),
        }

    @staticmethod
    def extract_user_canvas_details(user_id, canvases_received):
        json_data = json.loads(canvases_received)
        user_canvases = []
        for data in json_data:
            name = data.get("name")
            id = data.get("api_canvas_id")
            type = "canvas"
            last_received = DateUtils.convert_to_timestamp(
                data.get("last_received_message")
            )
            in_control = data.get("is_in_control")
            converted = data.get("converted", None)
            conversion_events_performed = (
                json.dumps(data.get("conversion_events_performed", None))
                if data.get("conversion_events_performed")
                else None
            )
            message_status = (
                json.dumps(data.get("engaged", None)) if data.get("engaged") else None
            )
            user_canvas = {
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
            user_canvases.append(user_canvas)
        return user_canvases

    @staticmethod
    def extract_user_canvas_steps_details(user_id, canvases_received):
        json_data = json.loads(canvases_received)
        user_canvases_steps = []
        for data in json_data:
            if len(data.get("steps_received")) > 0:
                for step in data.get("steps_received"):
                    user_canvas_step = {
                        "step_id": step.get("api_canvas_step_id"),
                        "step_name": step.get("name"),
                        "playbook_id": data.get("api_canvas_id"),
                        "user_id": user_id,
                        "last_received": DateUtils.convert_to_timestamp(
                            step.get("last_received")
                        ),
                    }
                    user_canvases_steps.append(user_canvas_step)
        return user_canvases_steps
