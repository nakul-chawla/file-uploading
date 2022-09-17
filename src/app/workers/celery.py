import asyncio
import os
import logging

from celery import Celery, Task
from celery.utils.log import get_task_logger
from sqlalchemy.orm import Session

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# load database after the event loop is set in case of async DB drivers
from app.database import db_instance  # noqa: E402

logger = get_task_logger(__name__)

rabbitmq_url = os.getenv("RABBITMQ_URL", "pyamqp://user:bitnami@rabbitmq:5672//")

celery_app = Celery(
    "workers",
    broker=rabbitmq_url,
    include=[
        "app.workers.tasks",
    ],
)

"""The configuration of the task, 
tells us that one worker takes one task at a time. 
"""
celery_app.config_from_object(
    {
        "worker_prefetch_multiplier": 1,
        "worker_send_task_events": True,
        "task_send_sent_event": True,
        "timezone": "UTC",
        "task_routes": {},
        "task_default_priority": 1,
    }
)


class BaseDbTask(Task):
    _db_session: Session = None

    @property
    def session(self):
        if self._db_session is None:
            self._db_session = db_instance.initialize_session()

        return self._db_session

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.exception(f"task {task_id} failed args:{args} {exc}")
        try:
            self.session.rollback()
        except Exception:
            raise Exception("Failed to rollback session on retry")

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
        finally:
            logging.info(f"Closing db session {id(self.session)}")
            self.session.close()


if __name__ == "__main__":
    celery_app.start()
