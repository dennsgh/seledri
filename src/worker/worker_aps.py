import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Any
from apscheduler.schedulers.background import BackgroundScheduler
from scheduler.functionmap import FunctionMap
import os

class Worker:
    def __init__(self, function_map_file: Path,daemon=False):
        """
        Initializes the Worker class.

        Args:
            function_map_file (Path): Path to the file containing the function map.
        """
        self.logger = logging.getLogger(__name__)
        self.scheduler = BackgroundScheduler(daemon=daemon)
        self.function_map = FunctionMap(function_map_file)
        self.logfile = Path(os.getenv("LOGS"), "worker.log")
        self.configure_logging()
        self.logger.info("Function Map OK")

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

    def __schedule_task__(self, task_name: str, run_time: datetime, *args: Any, **kwargs: Any) -> None:
        """
        Schedules a task to be run at a specified time.

        Args:
            task_name (str): The name of the task to schedule.
            run_time (datetime): The time at which the task should run.
            *args (Any): Positional arguments to pass to the task.
            **kwargs (Any): Keyword arguments to pass to the task.
        """
        args = args if args is not None else ()
        kwargs = kwargs if kwargs is not None else {}
        task_func = self.function_map.get_function(task_name)
        if task_func:
            self.scheduler.add_job(
                self.function_map.parse_and_call,
                'date',
                run_date=run_time,
                kwargs={
                    "func": task_func,
                    "args": args,
                    "kwargs": kwargs
                }
            )
            self.logger.debug(f"Scheduled task '{task_name}' to run at {run_time}")
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")

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

    def execute_task(self, task_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Executes a registered task.

        Args:
            task_name (str): The name of the task to execute.
            *args (Any): Positional arguments to pass to the task.
            **kwargs (Any): Keyword arguments to pass to the task.

        Returns:
            Any: The result of the task function, if any.
        """
        task_func = self.function_map.get_function(task_name)
        if task_func:
            args = args if args is not None else ()
            kwargs = kwargs if kwargs is not None else {}
            return self.function_map.parse_and_call(task_func, *args, **kwargs)
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
