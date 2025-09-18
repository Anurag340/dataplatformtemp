"""
Module for handling background tasks.
"""

import logging
from typing import Any, Dict

from sqlalchemy import Table
from sqlalchemy.orm import Session

from ..airbyte.manager import AirbyteClient
from .airbyte import create_connection, delete_source, sync_connection
from .databricks import (
    clean_workflow_run_history,
    delete_workflow,
    deploy_and_run_dependend_workflows,
    deploy_and_run_onboarding_status_workflow,
    deploy_and_run_workflow,
    drop_databricks_tables,
    insert_workflow_configuration,
)
from .db_utils import get_active_tools, get_org_name, insert_connector, update_connector
from .task_step_wrapper import update_task_row

logger = logging.getLogger(__name__)


async def process_source_creation(
    task_id: str,
    org_id: str,
    workspace_id: str,
    tool_id: str,
    config: Dict[str, Any],
    tool_name: str,
    db: Session,
    airbyte_client: AirbyteClient,
    tables: Dict[str, Table],
    request_start_date: str,
    request_end_date: str,
):
    """
    Background task for processing source creation.

    Args:
        task_id: Task ID
        org_id: Organization ID
        workspace_id: Workspace ID
        tool_id: Tool ID
        config: Configuration dictionary
        db: Database session
        airbyte_client: Airbyte client instance
        tables: Dictionary of database tables
    """
    try:
        logger.info(f"Processing source creation for task_id: {task_id}")

        org_name = get_org_name(org_id, db, tables)
        org_key = org_name
        # org_key = f"c{org_id}{org_name}".lower()
        # org_key = "".join(c for c in org_key if c.isalnum())
        logger.info(f"Org name: {org_name}, org_key: {org_key}")

        connector_name = f"{org_key}-{tool_name}"
        connector_id = insert_connector(
            tool_id,
            connector_name,
            config,
            org_id,
            workspace_id,
            db,
            tables,
            task_id=task_id,
        )
        logger.info(f"Connector ID: {connector_id}")

        delete_source(tool_name, org_key, airbyte_client, task_id=task_id)

        delete_workflow(tool_name, org_key, task_id=task_id)
        drop_databricks_tables(tool_name, org_key, task_id=task_id)

        # clean_workflow_run_history(tool_name, org_key, task_id=task_id)
        connection_id = create_connection(
            tool_name, config, org_key, airbyte_client, task_id=task_id
        )
        # logger.info(f"Connection ID: {connection_id}")

        sync_connection(connection_id, airbyte_client, task_id=task_id)
        # logger.info(f"Synced connection {connection_id}")

        # insert_workflow_configuration(tool_name, org_key, task_id=task_id)

        deploy_and_run_workflow(
            tool_name,
            config,
            org_key,
            start_date=request_start_date,
            end_date=request_end_date,
            task_id=task_id,
        )
        # logger.info(f"Workflow run completed successfully")

        update_connector(connector_id, db, tables, task_id=task_id)

        active_tools = get_active_tools(
            org_id, workspace_id, db, tables, task_id=task_id
        )

        # insert_workflow_configuration(
        #     tool_name, org_key, active_tools=active_tools, task_id=task_id
        # )

        deploy_and_run_dependend_workflows(
            active_tools,
            org_key,
            tool_name,
            org_id,
            workspace_id,
            db,
            tables,
            start_date=request_start_date,
            end_date=request_end_date,
            task_id=task_id,
        )

        deploy_and_run_onboarding_status_workflow(org_key, org_id, task_id=task_id)

    except Exception as e:
        logger.error(f"Error in source creation for task_id: {task_id}")
        update_task_row(task_id, "failed")
        raise e
    update_task_row(task_id, "succeeded")
