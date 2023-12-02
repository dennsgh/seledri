import inspect
import logging
import os
import signal
import subprocess
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

from celery import Celery

from scheduler.functionmap import FunctionMap


def start_celery_worker(app, num_workers):
    argv = ["worker", "--loglevel=INFO", f"--concurrency={num_workers}"]
    app.worker_main(argv)


def start_flower(app, port=5555):
    """
    Starts Flower for Celery monitoring on the specified port.
    """
    broker_url = app.conf.broker_url
    flower_command = ["flower", f"--broker={broker_url}", f"--port={port}"]
    subprocess.Popen(flower_command)


class Worker:
    def __init__(self, function_map_file: Path, broker: str, backend: str):
        self.app = Celery("scheduler", broker=broker, backend=backend)
        self.logger = logging.getLogger(__name__)
        self.function_map = FunctionMap(function_map_file)
        self.logfile = Path(os.getenv("LOGS"), "worker.log")
        self.logger.info("Function Map OK")
        self.worker_processes = []
        # Configure logging
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

        # Set up Celery logging to use the same logger
        self.app.log.get_default_logger = lambda: self.logger

    def signal_handler(self, signum, frame):
        """Gracefully shut down the worker and Flower processes."""
        self.logger.info("Received shutdown signal, terminating processes...")
        for process in self.worker_processes:
            process.terminate()
            process.join()
        if self.flower_process:
            self.flower_process.terminate()
            self.flower_process.wait()
        self.logger.info("All processes terminated.")

    def register_task(self, func, func_identifier, *task_args, **task_kwargs):
        def task_wrapper(*args, **kwargs):
            try:
                # Use the passed function reference directly
                return func(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Error registering task '{func_identifier}': {e}")

        # Add the function reference to the FunctionMap
        self.function_map.add_function(func_identifier, func)
        self.logger.info(f"Added function {func_identifier} {func}.")
        self.app.task(name=func_identifier)(task_wrapper)

    def __schedule_task__(self, task_name: str, run_time: datetime, args, kwargs: dict):
        task = self.app.tasks.get(task_name)
        if task:
            delay = (run_time - datetime.now()).total_seconds()
            delay = max(delay, 0)

            # Inspect the task function and get its parameters
            sig = inspect.signature(task.run)
            param_names = [p.name for p in sig.parameters.values()]

            # Convert args to kwargs based on the order in the task signature
            args_to_kwargs = dict(zip(param_names, args))
            all_kwargs = {**args_to_kwargs, **kwargs}

            # Apply the task with keyword arguments
            result = task.apply_async(kwargs=all_kwargs, countdown=delay)
            self.logger.debug(
                f"Scheduling task '{task_name}' to run at {run_time} with celery id {result.id}"
            )
            return result
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")

    def start_worker(self, num_workers=1):
        # Register the signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        self.logger.debug(f"Starting {num_workers} Celery worker process(es)...")
        start_flower(self.app, port=5555)
        # Create and start a process that runs the worker
        for _ in range(num_workers):
            # Pass the Celery app and the number of workers to the top-level function
            process = Process(target=start_celery_worker, args=(self.app, num_workers))
            process.start()
            self.worker_processes.append(process)
            self.logger.debug(f"Celery worker started with PID {process.pid}")

    def stop_worker(self):
        self.logger.debug("Gracefully stopping Celery worker processes...")
        for process in self.worker_processes:
            process.terminate()  # Ask processes to terminate
            process.join()  # Wait for the process to terminate
        self.logger.debug("Celery worker processes have been stopped.")

    def execute_task(self, task_name, *args, **kwargs):
        self.logger.debug(f"Executing task '{task_name}': {args} {kwargs}")
        task = self.app.tasks.get(task_name)
        if task:
            # Execute the task immediately and asynchronously
            result = task.apply_async(args=args, kwargs=kwargs)
            self.logger.debug(f"Task '{task_name}' is registered with ID {result.id}")
            return result  # Now you can use this result object elsewhere to check status or get results
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
