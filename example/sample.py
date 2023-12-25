import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from seledri.timekeeper import Timekeeper
from seledri.worker import Worker


def print_task(message):
    print(message)  # This will print to stdout of the worker process


def main():
    # Initialize Worker and Scheduler
    worker = Worker(
        function_map_file=Path(os.getenv("CONFIG"), "function_map.json"),
    )
    worker.start_worker()
    time.sleep(1)
    # Registering the functions!
    worker.register_task(print_task, "print")  # Pass the function reference 'print'

    # Start the worker processes
    scheduler = Timekeeper(Path(os.getenv("CONFIG"), "jobs.json"), worker)
    # Schedule a task
    schedule_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(
        task_name="print", schedule_time=schedule_time, kwargs={"message": "hello"}
    )

    worker.execute_task(
        "print", args=("Timekeeper and Worker initialized.",), kwargs={}
    )  # comma otherwise it won't be treated as tuple
    time.sleep(5)
    worker.stop_worker()


if __name__ == "__main__":
    main()
