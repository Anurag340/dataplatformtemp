import datetime
import json
import logging
from contextlib import asynccontextmanager
from typing import Dict

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Table
from sqlalchemy.orm import Session

from ..airbyte.manager import AirbyteClient
from .airbyte import init_airbyte
from .background_tasks import process_source_creation
from .database import cleanup_db, init_db
from .db_utils import new_task
from .dependencies import get_airbyte, get_db_session, get_tables
from .models import PingResponse, SourceCreateRequest, SourceCreateResponse
from .startup import custom_startup
from .validate_source import validate_source

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application lifecycle")
    # Load environment variables
    load_dotenv()

    # Initialize database
    init_db()

    # Initialize Airbyte client
    init_airbyte()

    # Your custom startup function
    await custom_startup()

    yield
    logger.info("Shutting down application")

    # Cleanup
    cleanup_db()


# Initialize FastAPI app with lifespan
app = FastAPI(title="Propel Data Platform API", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        # "http://localhost:3000"
        "*"
    ],  # Adjust this based on your Node.js backend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ping", response_model=PingResponse)
async def ping():
    logger.info("Received ping request")
    return PingResponse(message="pong")


@app.post("/source/new/", response_model=SourceCreateResponse)
async def create_source(
    request: SourceCreateRequest,
    background_tasks: BackgroundTasks,
    db: Session = get_db_session(),
    airbyte_client: AirbyteClient = get_airbyte(),
    tables: Dict[str, Table] = get_tables(),
):
    if request.start_date is None:
        request_start_date = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=6)
        ).strftime("%Y-%m-%d")
    else:
        request_start_date = request.start_date
    if request.end_date is None:
        request_end_date = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d"
        )
    else:
        request_end_date = request.end_date
    logger.info(
        f"Creating new source for org_id: {request.org_id}, workspace_id: {request.workspace_id}, tool_id: {request.tool_id}, config: {request.config}"
    )

    config = request.config
    if isinstance(config, str):
        config = json.loads(config)

    try:
        tool_name = validate_source(request.tool_id, config, db, tables)
        logger.info(f"Tool name: {tool_name}")
    except Exception as e:
        logger.error(f"Error validating source: {e}")
        return SourceCreateResponse(valid=False)

    task_id = new_task(
        request.org_id, request.workspace_id, request.tool_id, db, tables
    )

    background_tasks.add_task(
        process_source_creation,
        task_id=task_id,
        org_id=request.org_id,
        workspace_id=request.workspace_id,
        tool_id=request.tool_id,
        config=config,
        tool_name=tool_name,
        db=db,
        airbyte_client=airbyte_client,
        tables=tables,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
    )

    return SourceCreateResponse(valid=True, task_id=task_id)
