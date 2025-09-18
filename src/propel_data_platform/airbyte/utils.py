import os

from propel_data_platform.airbyte.manager import AirbyteClient, AirbyteConfig


def get_airbyte_client():
    config = get_airbyte_config()
    return AirbyteClient(config)


def get_airbyte_config():
    config = AirbyteConfig(
        base_url=os.getenv("AIRBYTE_BASE_URL"),
        client_id=os.getenv("AIRBYTE_CLIENT_ID"),
        client_secret=os.getenv("AIRBYTE_CLIENT_SECRET"),
    )
    return config
