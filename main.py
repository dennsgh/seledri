from datetime import datetime, timedelta
from pathlib import Path

from celery import shared_task
from celery.utils.log import get_task_logger

from scheduler.timekeeper import Timekeeper
from tests.test_module import print_2
from worker.worker import Worker

logger = get_task_logger(__name__)


@shared_task(name="print")
def print_task(message):
    print(message)  # This will print to stdout of the worker process
    logger.info(f"Printing message: {message}")  # This will be logged


def main():
    # Initialize Worker and Scheduler
    worker = Worker(
        function_map_file=Path(".function_map.json"),
        broker="memory://",
        backend="cache+memory://",
    )

    # Registering the functions!
    worker.register_task(print_task, "print")  # Pass the function reference 'print'
    worker.register_task(print_2, "print_2")  # Pass the function reference 'print'

    # Start the worker processes
    scheduler = Timekeeper(Path("jobs.json"), worker)

    # Schedule a task
    schedule_time = datetime.now() + timedelta(seconds=6)
    formatted_timestamp = schedule_time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # Format the timestamp
    scheduler.add_job(
        "print", schedule_time, args=(formatted_timestamp,)
    )  # Pass the formatted timestamp as an argument
    scheduler.add_job(
        "print_2", schedule_time, args=(formatted_timestamp,)
    )  # Pass the formatted timestamp as an argument

    worker.start_worker()


if __name__ == "__main__":
    main()
