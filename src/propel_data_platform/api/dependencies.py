from fastapi import Depends

from .airbyte import get_airbyte_client
from .database import get_db, get_postgres_tables


def get_db_session():
    """Get database session dependency."""
    return Depends(get_db)


def get_airbyte():
    """Get Airbyte client dependency."""
    return Depends(get_airbyte_client)


def get_tables():
    """Get database tables dependency."""
    return Depends(get_postgres_tables)
