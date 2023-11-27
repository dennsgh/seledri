from celery.utils.log import get_task_logger
from celery import shared_task

logger = get_task_logger(__name__)

@shared_task(name='print_2')
def print_2(message):
    print(message)  # This will print to stdout of the worker process
    logger.info(f"Printing message: {message}")  # This will be logged