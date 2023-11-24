from celery import Celery
import logging
from multiprocessing import Process
from scheduler.functionmap import FunctionMap
import subprocess
class Worker:
    def __init__(self, function_map_file, broker, backend):
        self.app = Celery('scheduler', broker=broker, backend=backend)
        self.function_map = FunctionMap(function_map_file)
        self.worker_processes = []
        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        # Create a file handler and set its level to DEBUG
        file_handler = logging.FileHandler('worker.log')
        file_handler.setLevel(logging.DEBUG)

        # Create a console handler and set its level to DEBUG
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Create a formatter and add it to both handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

    def start_worker(self, num_workers=1):
        self.logger.debug(f"Starting {num_workers} Celery worker processes...")
        cmd = ['celery', 'worker', '--loglevel=INFO', f'--concurrency={num_workers}']
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop_worker(self):
        # You can use Celery's command to stop the workers gracefully
        cmd = ['celery', 'control', 'shutdown']
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def execute_task(self, task_name, *args, **kwargs):
        self.logger.debug(f"Executing task '{task_name}': {args} {kwargs}")
        task = self.app.tasks.get(task_name)
        if task:
            # Execute the task immediately
            task.apply_async(args=args, kwargs=kwargs)
        else:
            self.logger.error(f"Task '{task_name}' is not registered.")
