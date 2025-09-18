import logging

from propel_data_platform.utils.time_utils import DateUtils

logger = logging.getLogger(__name__)


class NewsletterUtils:

    @staticmethod
    def extract_customerio_newsletter(newsletter_details_data, newsletter_id):
        data = newsletter_details_data

        return {
            "newsletter_id": int(newsletter_id),
            "name": data["name"] or "",
            "tags": data["tags"],
            "type": data["type"],
            "created": (
                DateUtils.convert_epoch_to_timestamp(int(data["created"]))
                if data["created"] is not None
                else None
            ),
            "sent_at": (
                DateUtils.convert_epoch_to_timestamp(int(data["sent_at"]))
                if data["sent_at"] is not None
                else None
            ),
            "updated": (
                DateUtils.convert_epoch_to_timestamp(int(data["updated"]))
                if data["updated"] is not None
                else None
            ),
            "content_ids": data["content_ids"],
            "deduplicate_id": data["deduplicate_id"],
        }
