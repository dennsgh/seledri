import hashlib
import json
import logging  # Import the logging module
import os
from datetime import datetime, timedelta
from pathlib import Path

from worker.worker import Worker


class Timekeeper:
    def __init__(self, persistence_file: Path, worker_instance: Worker):
        self.persistence_file = persistence_file
        self.worker = worker_instance
        self.jobs = self.load_jobs()
        self.logfile = Path(os.getenv("LOGS"), "schedule.logs")
        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self._configure_logging()
        # Has to run before reregistering jobs ( to have celery know the actual tasks in the jobs )
        self.reload_function_map()
        # Re-schedule jobs that were persisted
        self.__reschedule_jobs__()

    def _configure_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler and set its level to DEBUG
        file_handler = logging.FileHandler(self.logfile)
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

    def compute_hash(self, task_name, *args, **kwargs):
        return hashlib.sha256(str(f"{task_name}{args}{kwargs}").encode()).hexdigest()

    def add_job(self, task_name: str, schedule_time: datetime, **kwargs):
        # Compute a hash of the concatenated string
        job_id = self.compute_hash(task_name, kwargs)

        self.jobs[job_id] = {
            "task": task_name,
            "created": datetime.now().isoformat(),
            "schedule_time": schedule_time.isoformat(),
            "kwargs": kwargs,
        }
        self.save_jobs()
        self.logger.debug(
            f"Received job {job_id} with task {task_name} to run at {schedule_time}"
        )
        self.schedule_job_with_celery(job_id)
        return job_id

    def schedule_job_with_celery(self, job_id):
        job_info = self.jobs[job_id]
        schedule_time = datetime.fromisoformat(job_info["schedule_time"])
        delay = (schedule_time - datetime.now()).total_seconds()
        delay = max(delay, 0)

        # Pass args and kwargs as expected by apply_async
        self.worker.__schedule_task__(
            job_info["task"],
            schedule_time,
            job_info.get("args", []),
            job_info.get("kwargs", {}),
        )

    def reload_function_map(self):
        for func_identifier, func_data in self.worker.function_map.function_map.items():
            func = self.worker.function_map.deserialize_func(func_data)
            self.worker.register_task(func, func_identifier)
        self.logger.debug(f"Function map {self.worker.function_map.map_file} reloaded.")

    def __reschedule_jobs__(self):
        self.logger.debug(f"Found {len(self.jobs)} scheduled.")

        # Get the current time
        now = datetime.now()
        # Remove invalid and past jobs in the persistent file
        self.prune()
        # Re-schedule jobs from the persistent file
        for job_id, job_info in self.jobs.items():
            schedule_time = datetime.fromisoformat(job_info["schedule_time"])

            # Check if the scheduled time is in the past
            if schedule_time < now:
                # Reschedule for a later time, for example, 10 seconds from now
                schedule_time = now + timedelta(seconds=10)
                self.logger.debug(
                    f"Rescheduling job {job_id} to run at {schedule_time}"
                )

            self.logger.debug(f"Restored job {job_id} to run at {schedule_time}")
            self.worker.__schedule_task__(
                job_info["task"], schedule_time, **job_info["kwargs"]
            )

    def prune(self):
        # Get the current time
        now = datetime.now()

        # List to hold job_ids to be removed
        jobs_to_remove = []

        for job_id, job_info in self.jobs.items():
            schedule_time = datetime.fromisoformat(job_info["schedule_time"])

            # Check if the job is invalid or its schedule time is in the past
            if schedule_time < now:
                jobs_to_remove.append(job_id)

        # Remove identified jobs
        for job_id in jobs_to_remove:
            del self.jobs[job_id]
            self.logger.debug(f"Pruned job {job_id}")
        if not len(jobs_to_remove):
            self.logger.debug(f"All jobs valid with {len(self.jobs)} remaining.")
        # Save the updated jobs list
        self.save_jobs()

    def remove_job(self, job_id):
        if job_id in self.jobs:
            del self.jobs[job_id]
            self.save_jobs()
            self.logger.debug("Removed job %s", job_id)  # Log when a job is removed

    def get_jobs(self):
        return self.jobs
