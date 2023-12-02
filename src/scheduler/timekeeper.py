import hashlib
import json
import logging  # Import the logging module
from datetime import datetime
from pathlib import Path

from worker.worker import Worker


class Timekeeper:
    def __init__(self, persistence_file: Path, worker_instance: Worker):
        self.persistence_file = persistence_file
        self.worker = worker_instance
        self.jobs = self.load_jobs()

        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self._configure_logging()

        # Re-schedule jobs that were persisted RUN ONCE
        self.__reschedule_jobs__()

    def _configure_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler and set its level to DEBUG
        file_handler = logging.FileHandler(".scheduler.log")
        file_handler.setLevel(logging.DEBUG)

        # Create a console handler and set its level to DEBUG
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Create a formatter and add it to both handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add both handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def load_jobs(self):
        try:
            with open(self.persistence_file, "r") as file:
                data = json.load(file)
                return data
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_jobs(self):
        with open(self.persistence_file, "w") as file:
            json.dump(self.jobs, file, indent=4)

    def add_job(self, task_name: str, schedule_time: datetime, *args, **kwargs):
        task_info = f"{task_name}{args}{kwargs}"

        # Compute a hash of the concatenated string
        task_hash = hashlib.sha256(task_info.encode()).hexdigest()

        job_id = str(datetime.now().timestamp())
        self.jobs[job_id] = {
            "task": task_name,
            "schedule_time": schedule_time.isoformat(),
            "task_identifier": task_hash,
            "args": args,
            "kwargs": kwargs,
        }
        self.save_jobs()
        self.schedule_job_with_celery(job_id)
        return job_id

    def schedule_job_with_celery(self, job_id):
        job_info = self.jobs[job_id]
        schedule_time = datetime.fromisoformat(job_info["schedule_time"])
        delay = (schedule_time - datetime.now()).total_seconds()
        delay = max(delay, 0)  # Ensure delay is not negative

        self.worker.__schedule_task__(
            job_info["task"], schedule_time, *job_info["args"], **job_info["kwargs"]
        )
        self.logger.debug(f"Scheduled job {job_id} to run at {schedule_time}")

    def __reschedule_jobs__(self):
        # Deserialize and re-register the functions from the FunctionMap
        for func_identifier, func_data in self.worker.function_map.function_map.items():
            print(f"{func_identifier} {func_data}")
            func = self.worker.function_map.deserialize_func(func_data)
            self.worker.register_task(func, func_identifier)
        self.logger.debug(f"Found {len(self.jobs)} scheduled.")

        # Re-schedule jobs from the persistent file
        for job_id, job_info in self.jobs.items():
            schedule_time = datetime.fromisoformat(job_info["schedule_time"])
            self.logger.debug(f"Scheduled job {job_id} to run at {schedule_time}")
            self.worker.__schedule_task__(
                job_info["task"], schedule_time, *job_info["args"], **job_info["kwargs"]
            )

    def remove_job(self, job_id):
        if job_id in self.jobs:
            del self.jobs[job_id]
            self.save_jobs()
            self.logger.debug("Removed job %s", job_id)  # Log when a job is removed

    def get_jobs(self):
        return self.jobs
