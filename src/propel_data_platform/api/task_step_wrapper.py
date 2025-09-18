import datetime
import functools
import logging
import traceback

from .database import get_db, get_postgres_tables

logger = logging.getLogger(__name__)


def insert_step_update_row(task_id, step_name, step_description, step_status):
    db, tables = next(get_db()), get_postgres_tables()
    insert_query = (
        tables["sonic.task_step_updates"]
        .insert()
        .values(
            task_id=task_id,
            step_name=step_name,
            step_description=step_description,
            step_status=step_status,
        )
    )
    db.execute(insert_query)
    db.commit()
    db.close()


def update_task_row(task_id, task_status):
    db, tables = next(get_db()), get_postgres_tables()
    update_query = (
        tables["sonic.tasks"]
        .update()
        .where(tables["sonic.tasks"].c.task_id == task_id)
        .values(
            task_state=task_status,
            updated_at=datetime.datetime.now(),
        )
    )
    db.execute(update_query)
    db.commit()
    db.close()


def track_status(step_name, step_description):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task_id = kwargs.get("task_id")
            del kwargs["task_id"]
            insert_step_update_row(task_id, step_name, step_description, "started")
            logger.info(f"Starting step {step_name}")
            try:
                result = func(*args, **kwargs)
                insert_step_update_row(
                    task_id, step_name, step_description, "succeeded"
                )
                logger.info(f"Step {step_name} completed successfully")
                return result
            except Exception as e:
                error_msg = traceback.format_exc()
                logger.error(f"Error in step {step_name}: {error_msg}")
                insert_step_update_row(task_id, step_name, step_description, "failed")
                update_task_row(task_id, "failed")
                logger.error(f"Step {step_name} failed: {error_msg}")
                raise e

        return wrapper

    return decorator
