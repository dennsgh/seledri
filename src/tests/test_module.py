import os
from pathlib import Path

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(name="print_2")
def print_2(message):
    print(message)
    Path(os.getenv("LOGS"), f"{message}").touch()
    logger.info(f"Printing message: {message}")  # This will be logged
