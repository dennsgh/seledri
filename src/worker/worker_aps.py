import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Any
from apscheduler.schedulers.background import BackgroundScheduler
from scheduler.functionmap import FunctionMap
import os

class Worker:
    def __init__(self, function_map_file: Path, daemon:bool = False, logfile:Path=None ,logger :logging.Logger = None):
        """
        Initializes the Worker class.

        Args:
            function_map_file (Path): Path to the file containing the function map.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.scheduler = BackgroundScheduler(daemon=daemon)
        self.function_map = FunctionMap(function_map_file)
        self.logfile = logfile or Path(os.getenv("LOGS"), "worker.log")
        self.configure_logging()
        self.logger.info("Function Map OK")
        self.logfile = logfile or Path(os.getenv("LOGS"), "worker.log")
        
    def configure_logging(self) -> None:
        """
        Configures logging for the Worker class.
        """
        self.logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(self.logfile)
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def register_task(self, func: Callable, task_name: str) -> None:
        """
        Registers a function under a task name for the scheduler to recognize.

        Args:
            func (Callable): The function to be registered.
            task_name (str): The name associated with the function.
        """
        self.function_map.add_function(task_name, func)
        self.logger.info(f"Added function {task_name} {func}.")

    def __schedule_task__(self, task_name: str, run_time: datetime, job_id: str, _callback: Callable, *args: Any, **kwargs: Any) -> None:
        """
        Schedules a task to be run at a specified time.

        Args:
            task_name (str): The name of the task to schedule.
            run_time (datetime): The time at which the task should run.
            job_id (str): The unique identifier of the job.
            _callback(Callable): Callback function for remove job method under Timekeeper
            *args (Any): Positional arguments to pass to the task.
            **kwargs (Any): Keyword arguments to pass to the task.
        """
        self.scheduler.add_job(
            self.execute_task,
            'date',
            run_date=run_time,
            args=(task_name, job_id, _callback,args,kwargs)
        )
        self.logger.debug(f"Scheduled task '{task_name}' to run at {run_time}")

    def start_worker(self) -> None:
        """
        Starts the worker.
        """
        self.scheduler.start()
        self.logger.debug("APScheduler worker started.")

    def stop_worker(self) -> None:
        """
        Stops the worker.
        """
        self.scheduler.shutdown()
        self.logger.debug("APScheduler worker stopped.")

    def execute_task(self, task_name: str, job_id: str = None, _callback: Callable = None, *args: Any, **kwargs: Any) -> Any:
        """
        Executes a registered task and removes it from the schedule after execution.

        Args:
            task_name (str): The name of the task to execute.
            job_id (str, optional): The unique identifier of the job.
            _callback (Callable, optional): The callback function to remove the job.
            *args (Any): Positional arguments to pass to the task.
            **kwargs (Any): Keyword arguments to pass to the task.

        Returns:
            Any: The result of the task function, if any.
        """
        task_func = self.function_map.get_function(task_name)
        result = "Task failed to execute"
        if task_func:
            # We need to ensure that 'args' is passed as a tuple and 'kwargs' as a dictionary
            self.logger.info(f"Execute task '{task_name}'(id:{job_id}).")
            result = self.function_map.parse_and_call(task_func, *args, **kwargs)
            
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
        if _callback and job_id:
            _callback(job_id)  # Call the callback to remove the job
        return result
        