# this script should be run on a cron job daily to run workflows
import argparse
import datetime
import json
import logging
import os
import time
from typing import Any, Dict, List

import yaml
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..workflows import PROJECT_ROOT, deploy_one_workflow
from .database import get_db, init_db

MAX_CONCURRENT_TASKS = 10

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# get all active orgs from sonic database

# for each org, get all active connectors

# get all workflows to be run for each org based on all active connectors

# resolve dependency order of the workflows and run them

# set max concurrent compute budget and throttle


def get_active_connectors() -> List[Dict[str, Any]]:
    """
    Get all active connectors with their organization and tool details.
    """
    init_db()

    # Get database session and tables
    db_generator = get_db()
    db = next(db_generator)
    # TODO: get the tool configs from secret.propel.build later on
    query = """
    WITH latest_connectors AS (
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY org_id, tool_id ORDER BY updated_at DESC) AS rn
            FROM sonic.connector
        ) sub
        WHERE rn = 1 AND active = TRUE
    )
    SELECT c.id, c.tool_id, c.org_id, c.config, o.domain, t.name as tool_name
    FROM latest_connectors c
    JOIN sonic.organisation o
    ON c.org_id = o.id
    JOIN sonic.tool t
    ON c.tool_id = t.id
    WHERE o.active = TRUE AND t.active = TRUE;
    """

    try:
        # Execute the query using SQLAlchemy text()
        sql_query = text(query)
        result = db.execute(sql_query)
        connectors = result.fetchall()

        # Convert to list of dictionaries
        connectors_list = []
        for connector in connectors:
            connector_dict = {
                # "id": connector.id,
                # "tool_id": connector.tool_id,
                # "org_id": connector.org_id,
                "config": connector.config,
                "domain": connector.domain,
                "tool_name": connector.tool_name,
            }
            connectors_list.append(connector_dict)
        start_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        end_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        logger.info(f"Found {len(connectors_list)} active connectors")
        return start_date, end_date, connectors_list
    except Exception as e:
        logger.error(f"Error fetching active connectors: {e}")
        raise e
    finally:
        # Close database session
        db.close()


def get_active_connectors_from_file(config_filename: str):
    """
    Get all active connectors from a file.
    """
    with open(
        os.path.join(PROJECT_ROOT, "configs", "runner", config_filename), "r"
    ) as f:
        config = yaml.safe_load(f)
    active_connectors = []
    for connector in config.get("connectors", []):
        if connector.get("active", True):
            active_connectors.append(connector)
    start_date = config.get("start-date", "today")
    end_date = config.get("end-date", "today")
    if start_date.strip().lower() == "today":
        start_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    if end_date.strip().lower() == "today":
        end_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    return start_date, end_date, active_connectors


def daily_run(config_filename: str = "cron.yaml"):
    """
    Main function to run daily workflows for all active organizations.
    This function should be called by a cron job.
    """
    try:
        logger.info(
            f"Starting daily run at {datetime.datetime.now(datetime.timezone.utc)}"
        )

        # active_connectors = get_active_connectors()
        # start_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        # end_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

        # start_date, end_date, active_connectors = get_active_connectors_from_file(
        #     config_filename
        # )
        start_date, end_date, active_connectors = get_active_connectors()

        active_tools = {}
        for connector in active_connectors:
            domain_name = connector["domain"].lower()
            tool_name = connector["tool_name"].lower()
            if domain_name not in active_tools:
                active_tools[domain_name] = {}
            active_tools[domain_name][tool_name] = connector.get("config", {})

        # load dependencies
        with open(
            os.path.join(PROJECT_ROOT, "configs", "base", "dependencies.yaml"), "r"
        ) as f:
            dependencies = yaml.safe_load(f)

        all_workflows = {}
        for domain_name, tool_info in active_tools.items():
            while True:
                len_all_workflows = len(all_workflows)
                for workflow_name, workflow_info in dependencies.items():
                    if workflow_name in all_workflows:
                        continue
                    tool_deps = set(workflow_info.get("tools", []))
                    active_tools_set = set(tool_info.keys())
                    workflow_deps = set(
                        [(wf, domain_name) for wf in workflow_info.get("workflows", [])]
                    )
                    all_workflows_set = set(all_workflows.keys())
                    if tool_deps.issubset(active_tools_set) and workflow_deps.issubset(
                        all_workflows_set
                    ):
                        all_workflows[(workflow_name, domain_name)] = {
                            "max_concurrent_tasks": workflow_info.get(
                                "max_concurrent_tasks", 1
                            ),
                            "workflows": workflow_deps,
                        }

                if len_all_workflows == len(all_workflows):
                    break

        logger.info(
            f"Details of the workflows: {all_workflows}"
        )

        pending_workflows = all_workflows.copy()
        inprogress_workflows = {}
        completed_workflows = {}
        failed_workflows = {}
        remaining_budget = MAX_CONCURRENT_TASKS
        deploy_id, wheel_path = None, None

        workspace_client = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN"),
        )

        while len(pending_workflows) > 0 or len(inprogress_workflows) > 0:
            # loop through pending workflows and run if dependencies are met and budget is available
            # add to inprogress_workflows
            # remove from pending_workflows
            logger.info(f"Pending workflows: {pending_workflows}, In-progress workflows: {inprogress_workflows}, Completed workflows: {completed_workflows}, Failed workflows: {failed_workflows}, Remaining budget: {remaining_budget}")
            completed_workflows_set = set(completed_workflows.keys())
            for workflow_name, domain_name in list(pending_workflows):
                workflow_info = pending_workflows[(workflow_name, domain_name)]
                if (
                    workflow_info["workflows"].issubset(completed_workflows_set)
                    and remaining_budget >= workflow_info["max_concurrent_tasks"]
                ):
                    # run workflow
                    logger.info(f"Running workflow {workflow_name} for {domain_name}")
                    client_config = {}
                    for tool_name, tool_info in active_tools[domain_name].items():
                        client_config[f"{tool_name}-config"] = tool_info
                    deploy_id, wheel_path, run_id = deploy_one_workflow(
                        env=os.getenv("ENV", "DEV"),
                        client=domain_name,
                        workflow=workflow_name,
                        deploy_id=deploy_id,
                        wheel_path=wheel_path,
                        run_now=True,
                        client_config_override=client_config,
                        runtime_config=json.dumps(
                            {
                                "start-date": start_date,
                                "end-date": end_date,
                            }
                        ),
                    )

                    inprogress_workflows[(workflow_name, domain_name)] = workflow_info
                    inprogress_workflows[(workflow_name, domain_name)][
                        "run_id"
                    ] = run_id
                    pending_workflows.pop((workflow_name, domain_name))
                    remaining_budget -= workflow_info["max_concurrent_tasks"]

            for workflow_name, domain_name in list(inprogress_workflows):
                workflow_info = inprogress_workflows[(workflow_name, domain_name)]
                run_id = workflow_info["run_id"]
                run = workspace_client.jobs.get_run(run_id)
                if (
                    hasattr(run, "status")
                    and hasattr(run.status, "termination_details")
                    and hasattr(run.status.termination_details, "type")
                ):
                    termination_type = run.status.termination_details.type
                    if termination_type.value == "SUCCESS":
                        # add to completed_workflows and remove from inprogress_workflows, update remaining_budget
                        completed_workflows[(workflow_name, domain_name)] = (
                            workflow_info
                        )
                        inprogress_workflows.pop((workflow_name, domain_name))
                        remaining_budget += workflow_info["max_concurrent_tasks"]
                        logger.info(
                            f"Completed workflow {workflow_name} for {domain_name} at {datetime.datetime.now(datetime.timezone.utc)}"
                        )
                    else:
                        # add to failed_workflows and remove from inprogress_workflows, update remaining_budget
                        failed_workflows[(workflow_name, domain_name)] = workflow_info
                        inprogress_workflows.pop((workflow_name, domain_name))
                        remaining_budget += workflow_info["max_concurrent_tasks"]

                        logger.info(
                            f"Failed workflow {workflow_name} for {domain_name} at {datetime.datetime.now(datetime.timezone.utc)}"
                        )

                        for pending_workflow_name, pending_workflow_domain_name in list(
                            pending_workflows
                        ):
                            pending_workflow_info = pending_workflows[
                                (pending_workflow_name, pending_workflow_domain_name)
                            ]
                            if (workflow_name, domain_name) in pending_workflow_info[
                                "workflows"
                            ]:
                                failed_workflows[
                                    (
                                        pending_workflow_name,
                                        pending_workflow_domain_name,
                                    )
                                ] = pending_workflow_info
                                pending_workflows.pop(
                                    (
                                        pending_workflow_name,
                                        pending_workflow_domain_name,
                                    )
                                )
                                logger.info(
                                    f"Removed workflow {pending_workflow_name} for {pending_workflow_domain_name} from pending_workflows because it's {workflow_name} failed"
                                )

            time.sleep(15)

            # loop through inprogress workflows and check if they are completed or failed,
            # if completed, add to completed_workflows and remove from inprogress_workflows, update remaining_budget
            # if failed, add to failed_workflows and remove from inprogress_workflows, update remaining_budget, remove all dependend workflows from pending_workflows

        logger.info(
            f"Completed daily run at {datetime.datetime.now(datetime.timezone.utc)}"
        )

    except Exception as e:
        logger.error(f"Error in daily run: {e}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run the workflow(s) with a specified config file."
    )
    parser.add_argument(
        "--config",
        type=str,
        # default="cron.yaml",
        help="config file name (default: cron.yaml)",
    )
    args = parser.parse_args()
    config_filename = args.config

    if config_filename == "cron.yaml":
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if not (now_utc.hour == 2 and 0 <= now_utc.minute < 10):
            logger.info(
                f"Exiting: Current UTC time is {now_utc.strftime('%H:%M')}, not between 02:00 and 02:10."
            )
        else:
            # Run the daily workflow
            daily_run(config_filename=config_filename)
    else:
        daily_run(config_filename=config_filename)
