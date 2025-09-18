import json
import uuid

from sqlalchemy import func, select

from .task_step_wrapper import track_status


def new_task(org_id, workspace_id, tool_id, db, tables):
    """Create a new task and its initial status update.

    Args:
        org_id: Organization ID
        workspace_id: Workspace ID
        tool_id: Tool ID
        db: Database session
        tables: Dictionary of database tables

    Returns:
        str: The created task_id
    """
    # Generate a new UUID for the task
    task_id = str(uuid.uuid4())

    # Insert into tasks table
    task_insert = (
        tables["sonic.tasks"]
        .insert()
        .values(
            task_id=task_id,
            org_id=org_id,
            workspace_id=workspace_id,
            tool_id=tool_id,
            task_name="source_creation",
            task_description=f"Creating new data source for org_id: {org_id}, workspace_id: {workspace_id}, tool_id: {tool_id}",
            task_state="in_progress",
            created_by="data-platform",
        )
    )
    db.execute(task_insert)

    # Commit the transaction
    db.commit()

    return task_id


def get_org_name(org_id, db, tables):
    org_query = (
        tables["sonic.organisation"]
        .select()
        .where(tables["sonic.organisation"].c.id == int(org_id))
    )
    org_result = db.execute(org_query).first()
    return org_result.domain.lower()


@track_status(
    step_name="insert_connector",
    step_description="Saving connector configurations securely...",
)
def insert_connector(tool_id, connector_name, config, org_id, workspace_id, db, tables):
    connector_insert = (
        tables["sonic.connector"]
        .insert()
        .values(
            tool_id=tool_id,
            name=connector_name,
            type="incremental",
            config=config if not isinstance(config, str) else json.loads(config),
            active=False,
            org_id=org_id,
            workspace_id=workspace_id,
        )
    ).returning(tables["sonic.connector"].c.id)
    result = db.execute(connector_insert)
    connector_id = result.scalar_one()
    db.commit()
    return connector_id


@track_status(
    step_name="update_connector", step_description="Marking connector as active..."
)
def update_connector(connector_id, db, tables):
    update_query = (
        tables["sonic.connector"]
        .update()
        .where(tables["sonic.connector"].c.id == connector_id)
        .values(active=True)
    )
    db.execute(update_query)
    db.commit()


@track_status(step_name="get_active_tools", step_description="Fetching active tools...")
def get_active_tools(org_id, workspace_id, db, tables):
    # Query to get the latest connector for each tool_id using window function
    connector = tables["sonic.connector"]

    rn_expr = (
        func.row_number()
        .over(partition_by=connector.c.tool_id, order_by=connector.c.updated_at.desc())
        .label("rn")
    )

    ranked_connectors = (
        select(connector, rn_expr)
        .where(connector.c.org_id == org_id)
        .where(connector.c.workspace_id == workspace_id)
        .cte(name="ranked_connectors")
    )

    stmt = select(ranked_connectors.c.tool_id).where(
        ranked_connectors.c.rn == 1, ranked_connectors.c.active == True
    )
    result = db.execute(stmt).fetchall()

    tool_names = {}
    for (tool_id,) in result:
        tool_names[get_tool_name_from_tool_id(tool_id, db, tables)] = tool_id
    return tool_names


def get_tool_name_from_tool_id(tool_id, db, tables):
    tool_query = (
        tables["sonic.tool"].select().where(tables["sonic.tool"].c.id == int(tool_id))
    )
    tool_result = db.execute(tool_query).first()
    return tool_result.name.lower()


def get_config(tool_id, org_id, workspace_id, db, tables):
    connector = tables["sonic.connector"]

    stmt = (
        select(connector.c.config)
        .where(
            connector.c.tool_id == tool_id,
            connector.c.org_id == org_id,
            connector.c.workspace_id == workspace_id,
            connector.c.active == True,
        )
        .order_by(connector.c.updated_at.desc())
        .limit(1)
    )
    result = db.execute(stmt).first()
    return result[0] if result else None
