import logging
from datetime import datetime
from multiprocessing import Process

from celery import Celery

from scheduler.functionmap import FunctionMap

def start_celery_worker(app, num_workers):
    argv = [
        'worker',
        '--loglevel=INFO',
        f'--concurrency={num_workers}'
    ]
    app.worker_main(argv)
class Worker:
    def __init__(self, function_map_file, broker, backend):
        self.app = Celery("scheduler", broker=broker, backend=backend)
        self.function_map = FunctionMap(function_map_file)
        self.worker_processes = []
        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        # Create a file handler and set its level to DEBUG
        file_handler = logging.FileHandler("worker.log")
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

    def register_task(self, func, func_identifier, *task_args, **task_kwargs):
        def task_wrapper(*args, **kwargs):
            try:
                # Use the passed function reference directly
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Error executing task '{func_identifier}': {e}")

        # Add the function reference to the FunctionMap
        self.function_map.add_function(func_identifier, func)

        self.app.task(name=func_identifier)(task_wrapper)
        
    def __schedule_task__(self, task_name:str, run_time:datetime, *args, **kwargs):
        """
        Schedules a task to be executed at a specific `run_time`.
        """
        task = self.app.tasks.get(task_name)
        if task:
            # Calculate the number of seconds from now until the run_time
            delay = (run_time - datetime.now()).total_seconds()
            # Ensure we're not scheduling tasks in the past
            delay = max(delay, 0)

            # Schedule the task
            result = task.apply_async(args=args, kwargs=kwargs, countdown=delay)
            self.logger.debug(f"Scheduled task '{task_name}' to run at {run_time} with ID {result.id}")
            return result
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
            
    def start_worker(self, num_workers=1):
        self.logger.debug(f"Starting {num_workers} Celery worker process(es)...")

        # Create and start a process that runs the worker
        for _ in range(num_workers):
            # Pass the Celery app and the number of workers to the top-level function
            process = Process(target=start_celery_worker, args=(self.app, num_workers))
            process.start()
            self.worker_processes.append(process)
            self.logger.debug(f"Celery worker started with PID {process.pid}")

    def stop_worker(self):
        self.logger.debug("Stopping Celery worker processes...")
        for process in self.worker_processes:
            process.terminate()  # Terminate the process
            process.join()       # Wait for the process to terminate
            self.logger.debug(f"Celery worker with PID {process.pid} stopped.")
        self.worker_processes = []  # Clear the list of worker processes


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

