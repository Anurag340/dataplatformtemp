import datetime
import json
import logging

import requests

from .db_utils import get_tool_name_from_tool_id

logger = logging.getLogger(__name__)


def validate_source(tool_id, config, db, tables) -> str:
    tool_name = get_tool_name_from_tool_id(tool_id, db, tables)

    logger.info(
        f"Validating source for tool_id: {tool_id}, tool_name: {tool_name}, config: {config}"
    )

    if isinstance(config, str):
        config = json.loads(config)

    if tool_name == "amplitude":
        validate_amplitude(config)
    elif tool_name == "mixpanel":
        validate_mixpanel(config)
    elif tool_name == "braze":
        validate_braze(config)
    elif tool_name == "customerio":
        validate_cio(config)
    else:
        raise ValueError(f"Tool with ID {tool_id} not found")

    return tool_name


def validate_amplitude(credentials):
    keys = ["api-key", "secret-key", "start-date"]
    if not all(key in credentials for key in keys):
        raise ValueError("api-key, secret-key, and start-date are required")

    api_key = credentials["api-key"]
    secret_key = credentials["secret-key"]

    session = requests.Session()
    session.auth = (api_key, secret_key)

    url = {
        "standard": "https://amplitude.com/api/2/export",
        "eu": "https://analytics.eu.amplitude.com/api/2/export",
    }[credentials.get("data-region", "standard")]

    # /export
    start = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    ) + "T12"
    end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    ) + "T12"

    with session.get(url, params={"start": start, "end": end}, stream=True) as response:
        response.raise_for_status()
        next(response.iter_lines())

    return

def validate_cio(credentials):
    keys = ["api-key"]
    if not all(key in credentials for key in keys):
        raise ValueError("api-key is required")
    api_key = credentials["api-key"]
    api_url = "https://api.customer.io/v1/campaigns"
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    })

    response = session.get(api_url)
    if response.status_code != 200:
        raise ValueError(f"Customer.io API key validation failed: {response.text}")
    # Optionally, you can log success or return something
    return
    

def validate_mixpanel(credentials):
    keys = ["api-url", "username", "password", "project-id", "from_date"]
    if not all(key in credentials for key in keys):
        raise ValueError(
            "api-url, username, password, project-id, and from_date are required"
        )

    api_url = credentials["api-url"].rstrip("/")
    username = credentials["username"]
    password = credentials["password"]
    project_id = credentials["project-id"]

    session = requests.Session()
    session.auth = (username, password)
    session.headers.update(
        {
            "accept": "text/plain",
            "Accept-Encoding": "gzip",
        }
    )

    # /export
    from_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    to_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    response = session.get(
        f"{api_url}/export",
        params={
            "from_date": from_date,
            "to_date": to_date,
            "project_id": int(project_id),
            "limit": 100,
        },
    )
    response.raise_for_status()

    return


def validate_braze(credentials):
    keys = ["api-key", "api-url", "start-date"]
    if not all(key in credentials for key in keys):
        raise ValueError("api-key, api-url, and start-date are required")

    api_key = credentials["api-key"]
    api_url = credentials["api-url"].rstrip("/")

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )

    # /campaigns/list
    response = session.get(f"{api_url}/campaigns/list")
    response.raise_for_status()

    campaign_ids = [campaign["id"] for campaign in response.json()["campaigns"]]
    if len(campaign_ids) > 0:
        campaign_id = campaign_ids[0]

        # /campaigns/details
        response = session.get(
            f"{api_url}/campaigns/details", params={"campaign_id": campaign_id}
        )
        response.raise_for_status()

        # /campaigns/data_series
        response = session.get(
            f"{api_url}/campaigns/data_series",
            params={"campaign_id": campaign_id, "length": 1},
        )
        response.raise_for_status()

    # /canvas/list
    response = session.get(f"{api_url}/canvas/list")
    response.raise_for_status()
    canvas_ids = [canvas["id"] for canvas in response.json()["canvases"]]
    if len(canvas_ids) > 0:
        canvas_id = canvas_ids[0]

        # /canvas/details
        response = session.get(
            f"{api_url}/canvas/details", params={"canvas_id": canvas_id}
        )
        response.raise_for_status()

        # /canvas/data_series
        response = session.get(
            f"{api_url}/canvas/data_series",
            params={
                "canvas_id": canvas_id,
                "ending-at": datetime.datetime.now().isoformat(),
                "length": 1,
            },
        )
        response.raise_for_status()

    # /users/export/ids
    # TODO: Implement this

    return
