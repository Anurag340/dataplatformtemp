import json

from propel_data_platform.utils.time_utils import DateUtils


class MetricUtils:
    @staticmethod
    def extract_customerio_journey_metrics(playbook_details, playbook_id, type):
        data = playbook_details

        started = json.loads(data["started"])
        finished = json.loads(data["finished"])
        messaged = json.loads(data["messaged"])
        activated = json.loads(data["activated"])
        converted = json.loads(data["converted"])
        exited_early = json.loads(data["exited_early"])
        never_activated = json.loads(data["never_activated"])

        date = data["endAt"]

        data_length = len(started)

        metrics = []
        for i in range(data_length - 1, -1, -1):
            metric = {
                "playbook_id": int(playbook_id),
                "type": type,
                "date": DateUtils.subtract_days_from_date(
                    date, (data_length - (i + 1))
                ),
                "started": started[i],
                "finished": finished[i],
                "messaged": messaged[i],
                "activated": activated[i],
                "converted": converted[i],
                "exited_early": exited_early[i],
                "never_activated": never_activated[i],
            }
            metrics.append(metric)

        return metrics

    @staticmethod
    def extract_customerio_channel_metrics(channel_metrics_data, playbook_id, type):
        data = channel_metrics_data

        bounced = json.loads(data["bounced"])
        clicked = json.loads(data["clicked"])
        converted = json.loads(data["converted"])
        created = json.loads(data["created"])
        deferred = json.loads(data["deferred"])
        drafted = json.loads(data["drafted"])
        failed = json.loads(data["failed"])
        open = json.loads(data["opened"])
        sent = json.loads(data["sent"])
        spammed = json.loads(data["spammed"])
        suppressed = json.loads(data["suppressed"])
        undeliverable = json.loads(data["undeliverable"])
        topic_unsubscribed = json.loads(data["topic_unsubscribed"])
        unsubscribed = json.loads(data["unsubscribed"])

        date = data["endAt"]
        metrics = []

        data_length = len(bounced)

        for i in range(data_length - 1, -1, -1):
            metric = {
                "playbook_id": int(playbook_id),
                "type": type,
                "date": DateUtils.subtract_days_from_date(
                    date, (data_length - (i + 1))
                ),
                "channel": int(data["channel"]) if data.get("channel") else None,
                "channel_name": (
                    data["channel_name"] if data.get("channel_name") else None
                ),
                "bounced": bounced[i],
                "clicked": clicked[i],
                "converted": converted[i],
                "created": created[i],
                "deferred": deferred[i],
                "drafted": drafted[i],
                "failed": failed[i],
                "open": open[i],
                "sent": sent[i],
                "spammed": spammed[i],
                "suppressed": suppressed[i],
                "undeliverable": undeliverable[i],
                "topic_unsubscribed": topic_unsubscribed[i],
                "unsubscribed": unsubscribed[i],
            }
            metrics.append(metric)

        return metrics
