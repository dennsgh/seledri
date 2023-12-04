import logging
from datetime import datetime
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from scheduler.functionmap import FunctionMap
import os
class Worker:
    def __init__(self, function_map_file: Path):
        self.logger = logging.getLogger(__name__)
        self.scheduler = BackgroundScheduler()
        self.function_map = FunctionMap(function_map_file)
        self.logfile = Path(os.getenv("LOGS"), "worker.log")
        self.configure_logging()
        self.logger.info("Function Map OK")

    def configure_logging(self):
        self.logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

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

    def register_task(self, func, func_identifier):
        self.function_map.add_function(func_identifier, func)
        self.logger.info(f"Added function {func_identifier} {func}.")

    def schedule_task(self, task_name: str, run_time: datetime, *args, **kwargs):
        task_func = self.function_map.get_function(task_name)
        if task_func:
            self.scheduler.add_job(task_func, 'date', run_date=run_time, args=args, kwargs=kwargs)
            self.logger.debug(f"Scheduled task '{task_name}' to run at {run_time}")
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")

    def start_worker(self):
        self.scheduler.start()
        self.logger.debug("APScheduler started.")

    def stop_worker(self):
        self.scheduler.shutdown()
        self.logger.debug("APScheduler stopped.")

    def execute_task(self, task_name, *args, **kwargs):
        task_func = self.function_map.get_function(task_name)
        if task_func:
            self.logger.debug(f"Executing task '{task_name}': {args} {kwargs}")
            return task_func(*args, **kwargs)
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
