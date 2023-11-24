from celery import Celery
import logging
from scheduler.functionmap import FunctionMap

# Set up logging
logging.basicConfig(filename='worker_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class Worker:
    def __init__(self, broker_url, function_map_file):
        self.app = Celery('scheduler', broker=broker_url)
        self.function_map = FunctionMap(function_map_file)

    def register_task(self, identifier, *task_args, **task_kwargs):
        def task_wrapper():
            try:
                func = self.function_map.get_function(identifier)
                if func:
                    return func(*task_args, **task_kwargs)
                else:
                    logging.error(f"Function with identifier '{identifier}' not found")
            except Exception as e:
                logging.error(f"Error executing task '{identifier}': {e}")

        self.app.task(name=identifier)(task_wrapper)

    def start_worker(self):
        argv = ['worker', '--loglevel=INFO']
        self.app.worker_main(argv)