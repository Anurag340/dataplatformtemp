import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import requests
from databricks.sdk import WorkspaceClient

from ..workflows import deploy_one_workflow
from .db_utils import get_config
from .task_step_wrapper import track_status

logger = logging.getLogger(__name__)

DEFAULT_WAREHOUSE_ID = "79739a5cbc185e2f"


@track_status(
    step_name="deploy_and_run_workflows",
    step_description="Setting up and initialising data pipelines...",
)
def deploy_and_run_workflow(
    tool_name: str,
    config: Dict[str, Any],
    org_key: str,
    start_date: str,
    end_date: str,
):
    if tool_name == "amplitude":
        deploy_and_run_amplitude_workflows(config, org_key, start_date, end_date)
    elif tool_name == "mixpanel":
        deploy_and_run_mixpanel_workflows(config, org_key, start_date, end_date)
    elif tool_name == "braze":
        deploy_and_run_braze_workflows(config, org_key, start_date, end_date)


def await_workflow_completion(run_ids: List[str]):
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )

    check_interval = 15
    while True:
        success = []
        for run_id in run_ids:
            if run_id in success:
                continue
            run = workspace_client.jobs.get_run(run_id)
            if (
                hasattr(run, "status")
                and hasattr(run.status, "termination_details")
                and hasattr(run.status.termination_details, "type")
            ):
                termination_type = run.status.termination_details.type
                if termination_type.value == "SUCCESS":
                    logger.info(
                        f"Databricks workflow run {run_id} completed successfully"
                    )
                    success.append(run_id)
                else:
                    logger.info(
                        f"Databricks workflow run {run_id} failed with termination type {termination_type}"
                    )
                    raise Exception(
                        f"Databricks workflow run {run_id} failed with termination type {termination_type}"
                    )
            logger.info(
                f"Databricks workflow run {run_id} is still running, waiting for completion..., status: {run.as_dict().get('status')}"
            )
        if len(success) == len(run_ids):
            return
        time.sleep(check_interval)


def deploy_and_run_amplitude_workflows(
    config: Dict[str, Any], org_key: str, start_date: str, end_date: str
):

    _, _, run_id = deploy_one_workflow(
        env=os.getenv("ENV", "DEV"),
        client=org_key,
        workflow="amplitude-events",
        run_now=True,
        client_config_override={},
        runtime_config=json.dumps(
            {
                "start-date": start_date,
                "end-date": end_date,
            }
        ),
    )
    logger.info(f"Databricks workflow run ID: {run_id} for Amplitude Events")
    await_workflow_completion([run_id])


def deploy_and_run_mixpanel_workflows(
    config: Dict[str, Any], org_key: str, start_date: str, end_date: str
):
    history_length = (
        datetime.now().date()
        - datetime.strptime(config["from_date"], "%Y-%m-%d").date()
    ).days - 1
    runtime_config = json.dumps(
        {
            "start-date": start_date,
            "end-date": end_date,
        }
    )
    _, _, run_id1 = deploy_one_workflow(
        env=os.getenv("ENV", "DEV"),
        client=org_key,
        workflow="mixpanel-events",
        run_now=True,
        client_config_override={
            "mixpanel-config": {
                "api-url": config["api-url"],
                "username": config["username"],
                "password": config["password"],
                "project-id": config["project-id"],
            }
        },
        runtime_config=runtime_config,
    )
    logger.info(f"Databricks workflow run ID: {run_id1} for Mixpanel Events")
    _, _, run_id2 = deploy_one_workflow(
        env=os.getenv("ENV", "DEV"),
        client=org_key,
        workflow="mixpanel-events-run30d",
        run_now=True,
        client_config_override={
            "mixpanel-config": {
                "api-url": config["api-url"],
                "username": config["username"],
                "password": config["password"],
                "project-id": config["project-id"],
            }
        },
        runtime_config=runtime_config,
    )
    logger.info(f"Databricks workflow run ID: {run_id2} for Mixpanel Events Run 30d")
    await_workflow_completion([run_id1, run_id2])


def deploy_and_run_braze_workflows(
    config: Dict[str, Any], org_key: str, start_date: str, end_date: str
):
    runtime_config = json.dumps(
        {
            "start-date": start_date,
            "end-date": end_date,
        }
    )
    _, _, run_id1 = deploy_one_workflow(
        env=os.getenv("ENV", "DEV"),
        client=org_key,
        workflow="braze-canvas",
        run_now=True,
        client_config_override={
            "braze-config": {
                "api-url": config["api-url"],
                "api-key": config["api-key"],
            }
        },
        runtime_config=runtime_config,
    )
    logger.info(f"Databricks workflow run ID: {run_id1} for Braze Canvas")

    length = (
        datetime.now().date()
        - datetime.strptime(config["start-date"], "%Y-%m-%d").date()
    ).days
    runtime_config = json.dumps(
        {
            "start-date": start_date,
            "end-date": end_date,
        }
    )
    _, _, run_id2 = deploy_one_workflow(
        env=os.getenv("ENV", "DEV"),
        client=org_key,
        workflow="braze-campaigns",
        run_now=True,
        client_config_override={
            "braze-config": {
                "api-url": config["api-url"],
                "api-key": config["api-key"],
            }
        },
        runtime_config=runtime_config,
    )
    logger.info(f"Databricks workflow run ID: {run_id2} for Braze Campaigns")
    await_workflow_completion([run_id1, run_id2])


@track_status(
    step_name="delete_workflow",
    step_description="Deleting any existing sync schedules...",
)
def delete_workflow(tool_name: str, org_key: str):
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    if tool_name == "amplitude":
        workflows = ["amplitude-events"]
    elif tool_name == "mixpanel":
        workflows = ["mixpanel-events"]
    elif tool_name == "braze":
        workflows = ["braze-canvas", "braze-campaigns"]
    else:
        workflows = []

    env = os.getenv("ENV", "DEV").upper()
    workflows = [f"{env}.{org_key}.{workflow}" for workflow in workflows]

    for existing_job in workspace_client.jobs.list():
        if existing_job.settings.name in workflows:
            # workspace_client.jobs.delete(existing_job.job_id)
            workspace_client.jobs.cancel_all_runs(
                all_queued_runs=True, job_id=existing_job.job_id
            )

    logger.info(
        "Waiting for 60 seconds for Databricks workflow runs to be cancelled..."
    )
    time.sleep(60)
    logger.info("Waiting complete for Databricks workflow runs to be cancelled...")


@track_status(step_name="drop_tables", step_description="Removing any existing data...")
def drop_databricks_tables(tool_name: str, org_key: str):
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    if tool_name == "amplitude":
        tables = ["amplitude_events"]
    elif tool_name == "mixpanel":
        tables = []
    elif tool_name == "braze":
        tables = [
            "braze_campaigns_data_series",
            "braze_canvas_data_series",
            "braze_campaigns_details",
            "braze_canvas_details",
            "braze_campaigns_list",
            "braze_canvas_list",
        ]
    else:
        tables = []

    env = os.getenv("ENV", "DEV").upper()
    tables = [f"{env}.{org_key}.{table}" for table in tables]

    for table in tables:
        if workspace_client.tables.exists(table).table_exists:
            workspace_client.tables.delete(table)


@track_status(
    step_name="clean_workflow_run_history",
    step_description="Cleaning up any sync history...",
)
def clean_workflow_run_history(tool_name: str, org_key: str):
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    env = os.getenv("ENV", "DEV").upper()
    if tool_name == "amplitude":
        workflows = ["amplitude_events"]
    else:
        workflows = []

    for workflow in workflows:
        query = f"DELETE FROM {env}.public.workflow_run_history WHERE workflow = '{workflow}' AND client_name = '{org_key}'"
        workspace_client.statement_execution.execute_statement(
            statement=query, warehouse_id=DEFAULT_WAREHOUSE_ID
        )


@track_status(
    step_name="deploy_and_run_dependend_workflows",
    step_description="Setting up and initialising multi-connector data pipelines...",
)
def deploy_and_run_dependend_workflows(
    active_tools: Dict[str, str],
    org_key: str,
    tool_name: str,
    org_id: str,
    workspace_id: str,
    db,
    tables,
    start_date: str,
    end_date: str,
):
    runtime_config = json.dumps(
        {
            "start-date": start_date,
            "end-date": end_date,
        }
    )
    run_ids = []
    if len(set(["braze", "mixpanel"]) - set(active_tools)) == 0 and (
        tool_name in ["braze", "mixpanel"]
    ):
        braze_tool_id = active_tools["braze"]
        braze_config = get_config(braze_tool_id, org_id, workspace_id, db, tables)

        _, _, run_id = deploy_one_workflow(
            env=os.getenv("ENV", "DEV"),
            client=org_key,
            workflow="braze-user",
            run_now=True,
            client_config_override={
                "braze-config": {
                    "api-url": braze_config["api-url"],
                    "api-key": braze_config["api-key"],
                }
            },
            runtime_config=runtime_config,
        )
        logger.info(f"Databricks workflow run ID: {run_id} for Braze User")
        run_ids.append(run_id)

    if len(set(["braze", "amplitude"]) - set(active_tools)) == 0 and (
        tool_name in ["braze", "amplitude"]
    ):
        braze_tool_id = active_tools["braze"]
        braze_config = get_config(braze_tool_id, org_id, workspace_id, db, tables)

        _, _, run_id = deploy_one_workflow(
            env=os.getenv("ENV", "DEV"),
            client=org_key,
            workflow="braze-user-v2",
            run_now=True,
            client_config_override={
                "braze-config": {
                    "api-url": braze_config["api-url"],
                    "api-key": braze_config["api-key"],
                }
            },
            runtime_config=runtime_config,
        )
        logger.info(f"Databricks workflow run ID: {run_id} for Braze User V2")
        run_ids.append(run_id)

    if len(run_ids) > 0:
        await_workflow_completion(run_ids)


@track_status(
    step_name="run_onboarding_status_workflow",
    step_description="Running onboarding status workflow...",
)
def deploy_and_run_onboarding_status_workflow(org_key: str, org_id: str):
    environment = os.getenv("ENV", "DEV")
    api_url = os.getenv(f"{environment.upper()}_ONBOARDING_SYNC_STATUS_URL")
    api_token = os.getenv(f"{environment.upper()}_ONBOARDING_SYNC_STATUS_TOKEN")
    if environment == "DEV":
        api_url = f"https://api.dev.propel.build/{api_url}"
    elif environment == "PROD":
        api_url = f"https://api.propel.build/{api_url}"
    else:
        api_url = f"https://api.stage.propel.build/{api_url}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }
    payload = {"schema": org_key, "orgId": org_id}
    response = requests.post(api_url, headers=headers, json=payload)
    response.raise_for_status()


@track_status(
    step_name="insert_workflow_configuration",
    step_description="Inserting workflow configuration...",
)
def insert_workflow_configuration(
    tool_name: Any, org_key: str, active_tools: Dict[str, str] = None
):
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )

    if active_tools is None:

        if tool_name == "amplitude":
            rows = [
                {
                    "workflow_name": "amplitude-events",
                    "task_name": "transformation",
                    "current_workflow_name": "amplitude_events",
                },
            ]
        elif tool_name == "mixpanel":
            rows = [
                {
                    "workflow_name": "mixpanel-events",
                    "task_name": "events-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "mixpanel-events",
                    "task_name": "events-ingestor",
                    "current_workflow_name": "mixpanel_events_ingestor",
                },
            ]
        elif tool_name == "braze":
            rows = [
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "list-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "variations-transformation",
                    "current_workflow_name": "braze_variants_campaign_transformation",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "data-series-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "details-transformation",
                    "current_workflow_name": "braze_campaign_details_transformation",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "details-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "details-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "channels-campaign-transformation",
                    "current_workflow_name": "braze_channels_campaign_transformation",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "data-series-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-campaigns",
                    "task_name": "list-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "details-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "details-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "steps-transformation",
                    "current_workflow_name": "braze_canvas_steps_transformation",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "data-series-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "list-connector",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "channels-transformation",
                    "current_workflow_name": "braze_channels_canvas_transformation",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "list-ingestor",
                    "current_workflow_name": "",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "variations-transformation",
                    "current_workflow_name": "braze_variants_canvas_transformation",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "details-transformation",
                    "current_workflow_name": "braze_canvas_details_transformation",
                },
                {
                    "workflow_name": "braze-canvas",
                    "task_name": "data-series-connector",
                    "current_workflow_name": "",
                },
            ]
        else:
            rows = []
    else:
        rows = []
        if len(set(["braze", "mixpanel"]) - set(active_tools)) == 0 and (
            tool_name in ["braze", "mixpanel"]
        ):
            rows.extend(
                [
                    {
                        "workflow_name": "braze-user",
                        "task_name": "details-transformation",
                        "current_workflow_name": "braze_user_details_transformation",
                    },
                    {
                        "workflow_name": "braze-user",
                        "task_name": "export-ids-connector",
                        "current_workflow_name": "",
                    },
                    {
                        "workflow_name": "braze-user",
                        "task_name": "canvas-steps-transformation",
                        "current_workflow_name": "braze_user_canvas_steps_transformation",
                    },
                    {
                        "workflow_name": "braze-user",
                        "task_name": "canvas-transformation",
                        "current_workflow_name": "braze_user_canvas_transformation",
                    },
                    {
                        "workflow_name": "braze-user",
                        "task_name": "export-ids-ingestor",
                        "current_workflow_name": "",
                    },
                    {
                        "workflow_name": "braze-user",
                        "task_name": "campaign-transformation",
                        "current_workflow_name": "braze_user_campaign_transformation",
                    },
                ]
            )
        if len(set(["braze", "amplitude"]) - set(active_tools)) == 0 and (
            tool_name in ["braze", "amplitude"]
        ):
            rows.extend(
                [
                    {
                        "workflow_name": "braze-user-v2",
                        "task_name": "amplitude-ingestor",
                        "current_workflow_name": "amplitude_events",
                    },
                    {
                        "workflow_name": "braze-user-v2",
                        "task_name": "export-ids-connector",
                        "current_workflow_name": "",
                    },
                ]
            )

    env = os.getenv("ENV", "DEV").upper()
    table_name = f"{env}.public.workflow_configurations"

    values_tuples = []
    for row in rows:
        values_tuples.append(
            (
                f"'{org_key}'",
                f"'{row['workflow_name']}'",
                f"'{row['task_name']}'",
                f"'{row['current_workflow_name']}'",
                "true",
                f"'{datetime.now().isoformat()}'",
                f"'{datetime.now().isoformat()}'",
            )
        )

    values_string = ", ".join(
        [f"({', '.join(value_tuple)})" for value_tuple in values_tuples]
    )

    query = f"INSERT INTO {table_name} (client_name, workflow_name, task_name, current_workflow_name, is_active, created_at, updated_at) VALUES {values_string};"
    workspace_client.statement_execution.execute_statement(
        statement=query, warehouse_id=DEFAULT_WAREHOUSE_ID
    )
