import logging
import os
import time

from ..airbyte import utils
from .task_step_wrapper import track_status

# Global variable to store initialized client
airbyte_client = None
DESTINATION_ID = {
    "PROD": "660fd387-a784-4ef0-ab64-7e1c86190b1b",
    "DEV": "7224e6b1-98f3-4ba6-a6b6-13f07b42edcb",
}

logger = logging.getLogger(__name__)


def init_airbyte():
    """Initialize Airbyte client."""
    logger.info("Initializing Airbyte client")
    global airbyte_client
    airbyte_client = utils.get_airbyte_client()


def get_airbyte_client():
    """Dependency to get Airbyte client."""
    logger.info("Getting Airbyte client instance")
    return airbyte_client


@track_status(
    step_name="create_connection", step_description="Creating data sync connection..."
)
def create_connection(tool_name, config, org_key, airbyte_client):
    if tool_name == "amplitude":
        return create_amplitude_connection(config, org_key, airbyte_client)
    if tool_name == "customerio":
        return create_cio_connection(config, org_key, airbyte_client)
    else:
        return None


def create_amplitude_connection(config, org_key, airbyte_client):
    if config.get("data-region", "standard") == "standard":
        data_region = "Standard Server"
    else:
        data_region = "EU Residency Server"

    env = os.getenv("ENV", "DEV").upper()
    response = airbyte_client.create_source(
        name=f"{env}-{org_key}-amplitude-source",
        definition_id="fa9f58c6-2d03-4237-aaa4-07d75e0c1396",
        configuration={
            "data_region": data_region,
            "api_key": config["api-key"],
            "secret_key": config["secret-key"],
            "start_date": config["start-date"],
            "request_time_range": 4,
            "sourceType": "amplitude",
        },
    )
    source_id = response["sourceId"]

    response = airbyte_client.create_connection(
        name=f"{env}-{org_key}-amplitude-connection",
        source_id=source_id,
        destination_id=DESTINATION_ID[os.getenv("ENV", "DEV").upper()],
        configurations={
            "streams": [
                {
                    "name": "events",
                    "syncMode": "incremental_append",
                    "cursorField": ["server_upload_time"],
                    "primaryKey": [["uuid"]],
                },
                # {
                #     "name": "events_list",
                #     "syncMode": "full_refresh_overwrite",
                #     "primaryKey": [["id"]],
                # }
            ]
        },
        schedule={"scheduleType": "cron", "cronExpression": "0 0 0 * * ? UTC"},
        namespace_format=org_key,
        prefix="amplitude_",
    )
    return response["connectionId"]

def create_cio_connection(config, org_key, airbyte_client):
    env = os.getenv("ENV", "DEV").upper()
    response = airbyte_client.create_source(
        name=f"{env}-{org_key}-customerio-source",
        definition_id="4535293f-bb60-46ff-ba58-0237fd9601b4",
        configuration={
            "app_api_key":config["api-key"],
            "__injected_declarative_manifest": {}
        },
    )
    source_id = response["sourceId"]

    response = airbyte_client.create_connection(
        name=f"{env}-{org_key}-customerio-connection",
        source_id=source_id,
        destination_id=DESTINATION_ID[os.getenv("ENV", "DEV").upper()],
        # workspaceId="34a0c0bb-135b-4245-b87d-c2edd360ce84",
        configurations={
            "streams": [
                {
                "name": "campaigns",
                "syncMode": "full_refresh_overwrite_deduped",
                "cursorField": [],
                "primaryKey": [
                    [
                        "id"
                    ]
                ],
                "selectedFields": [],
                "mappers": []
                },
                {
                    "name": "transactional",
                    "syncMode": "full_refresh_overwrite_deduped",
                    "cursorField": [],
                    "primaryKey": [
                        [
                            "id"
                        ]
                    ],
                    "selectedFields": [],
                    "mappers": []
                },
                {
                    "name": "newsletters",
                    "syncMode": "full_refresh_overwrite_deduped",
                    "cursorField": [],
                    "primaryKey": [
                        [
                            "id"
                        ]
                    ],
                    "selectedFields": [],
                    "mappers": []
                }
            ]
        },
        schedule={"scheduleType": "cron", "cronExpression": "0 0 0 * * ? UTC"},
        namespace_format=org_key,
        prefix="customerio_",
    )
    return response["connectionId"]

@track_status(step_name="sync_connection", step_description="Syncing initial data...")
def sync_connection(connection_id, airbyte_client):
    if connection_id is None:
        return
    response = airbyte_client.sync_connection(connection_id)
    job_id = response["jobId"]

    check_interval = 15
    while True:
        job_status = airbyte_client.get_job_status(job_id)
        if job_status["status"] == "succeeded":
            return
        elif job_status["status"] in ["failed", "cancelled", "incomplete"]:
            raise Exception(f"Job {job_id} failed with status {job_status['status']}")
        elif job_status["status"] in ["pending", "running"]:
            logger.info(
                f"Airbyte sync job {job_id} is {job_status['status']}, waiting for completion..."
            )
            time.sleep(check_interval)
        else:
            raise Exception(f"Unknown job status: {job_status['status']}")


@track_status(
    step_name="delete_source",
    step_description="Deleting any existing connector configurations...",
)
def delete_source(tool_name, org_key, airbyte_client):
    env = os.getenv("ENV", "DEV").upper()
    sources = airbyte_client.list_sources()
    for source in sources:
        if source["name"] == f"{env}-{org_key}-{tool_name}-source":
            airbyte_client.delete_source(source["sourceId"])
