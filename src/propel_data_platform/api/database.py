import logging
import os
import time

from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Global variables to store initialized resources
engine = None
SessionLocal = None
metadata = None


def get_database_url():
    env = os.getenv("ENV", "DEV").upper()
    pg_host = os.getenv(f"{env}_PG_HOST")
    pg_port = os.getenv(f"{env}_PG_PORT")
    pg_user = os.getenv(f"{env}_PG_USER")
    pg_password = os.getenv(f"{env}_PG_PASSWORD")
    pg_db = os.getenv(f"{env}_PG_DATABASE")
    return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"


def init_db():
    """Initialize database connection and reflect metadata."""
    logger.info("Initializing database connection")
    global engine, SessionLocal, metadata
    DATABASE_URL = get_database_url()

    # PostgreSQL-specific connection settings
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,  # Enable connection health checks
        pool_recycle=3600,  # Recycle connections after 1 hour
        pool_timeout=30,  # Wait up to 30 seconds for a connection from the pool
        connect_args={
            "connect_timeout": 10,  # Wait up to 10 seconds for initial connection
            "options": "-c statement_timeout=30000",  # 30 seconds in milliseconds
        },
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    metadata = MetaData(schema="sonic")
    metadata.reflect(bind=engine)
    # Log all tables in the database
    logger.info("Reflecting database tables:")
    # for table_name in sorted(metadata.tables.keys()):
    #     logger.info(f"Found table: {table_name}")


def get_db(max_retries=3, retry_delay=1):
    """Dependency to get database session with retry logic."""
    logger.info("Creating new database session")
    retries = 0
    while retries < max_retries:
        try:
            db = SessionLocal()
            yield db
            break
        except OperationalError as e:
            retries += 1
            if retries == max_retries:
                logger.error(
                    f"Failed to connect to database after {max_retries} attempts"
                )
                raise
            logger.warning(
                f"Database connection failed, retrying in {retry_delay} seconds..."
            )
            time.sleep(retry_delay)
        finally:
            logger.info("Closing database session")
            if "db" in locals():
                db.close()


def get_postgres_tables():
    """Dependency to get reflected metadata."""
    metadata.reflect(bind=engine)
    return getattr(metadata, "tables", {})


def cleanup_db():
    """Cleanup database connection."""
    logger.info("Cleaning up database connection")
    if engine:
        engine.dispose()
