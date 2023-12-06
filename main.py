import os
from datetime import datetime, timedelta
from pathlib import Path


from scheduler.timekeeper import Timekeeper
from tests.test_module import print_2
from worker.worker_aps import Worker
import time


def print_task(message):
    print(message)  # This will print to stdout of the worker process

def main():
    # Initialize Worker and Scheduler
    worker = Worker(
        function_map_file=Path(os.getenv("CONFIG"),"function_map.json"),
    )
    worker.start_worker()
    time.sleep(3)
    # Registering the functions!
    worker.register_task(print_task, "print")  # Pass the function reference 'print'
    worker.register_task(print_2, "print_2")  # Pass the function reference 'print'

    # Start the worker processes
    scheduler = Timekeeper(Path(os.getenv("CONFIG"), "jobs.json"), worker)
    # Schedule a task
    schedule_time = datetime.now() + timedelta(seconds=6)
    scheduler.add_job(
        task_name="print", schedule_time=schedule_time, kwargs={"message":"hello"}
    )

    scheduler.add_job(
        task_name="print_2", schedule_time=schedule_time, kwargs={"message":"world"}
    )
    worker.execute_task("print",("Timekeeper and Worker initialized.",),{}) # comma otherwise it won't be treated as tuple
    time.sleep(10)

if __name__ == "__main__":
    main()
