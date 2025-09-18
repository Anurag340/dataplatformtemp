from datetime import UTC, datetime, timedelta


class DateUtils:
    """
    Converts an input string to a timestamp in ISO 8601 format.
    Returns None if the input is None or empty.
    """

    @staticmethod
    def convert_to_timestamp(input_value):
        if not input_value:  # Handle None or empty string
            return None
        try:
            return datetime.fromisoformat(input_value)
        except ValueError:
            return None

    @staticmethod
    def convert_epoch_to_timestamp(epoch):
        if epoch is None:
            return None

        try:
            return datetime.fromtimestamp(epoch, UTC)
        except ValueError:
            return None

    @staticmethod
    def subtract_days_from_date(date, days):
        if not date:
            return None

        try:
            return datetime.strptime(str(date), "%Y-%m-%d").date() - timedelta(
                days=days
            )
        except ValueError:
            return None
