import logging

from propel_data_platform.utils.time_utils import DateUtils

logger = logging.getLogger(__name__)


class TransactionalMessageUtils:

    @staticmethod
    def extract_customerio_transactional_message(
        transactional_message_details_data, transactional_message_id
    ):
        data = transactional_message_details_data
        # print(f"Data = {data}")

        return {
            "transactional_message_id": int(transactional_message_id),
            "name": data["name"] or "",
            "description": data["description"],
            "send_to_unsubscribed": data["send_to_unsubscribed"],
            "link_tracking": data["link_tracking"],
            "open_tracking": data["open_tracking"],
            "hide_message_body": data["hide_message_body"],
            "queue_drafts": data["queue_drafts"],
            "created_at": DateUtils.convert_epoch_to_timestamp(int(data["created_at"])),
            "updated_at": DateUtils.convert_epoch_to_timestamp(int(data["updated_at"])),
        }
