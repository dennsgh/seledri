import json
import os
from datetime import datetime, timedelta
from celery import signature
import threading
import heapq
import time
from worker.worker import Worker
from pathlib import Path
import logging  # Import the logging module
import hashlib
class PersistentScheduler:
    def __init__(self, persistence_file: Path, worker_instance: Worker):
        self.persistence_file = persistence_file
        self.jobs = self.load_jobs()
        self.worker = worker_instance
        self.job_queue = []
        self._rebuild_heap()
        self._scheduler_thread = threading.Thread(target=self._run_scheduler)
        self._scheduler_thread.daemon = True

        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler and set its level to DEBUG
        file_handler = logging.FileHandler('scheduler.log')
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
        
    def start_scheduler(self):
        self._scheduler_thread.start()
    
    def _rebuild_heap(self):
        for job_id, job_info in self.jobs.items():
            job_time = datetime.fromisoformat(job_info['schedule_time'])
            heapq.heappush(self.job_queue, (job_time, job_id))

    def _run_scheduler(self):
        while True:
            if not self.job_queue:
                time.sleep(1)
                continue

            next_run_time, job_id = self.job_queue[0]  # Peek at the next job
            now = datetime.now()
            self.logger.debug("Scheduler queue: %s", self.job_queue)  # Log the queue
            if now >= next_run_time:
                heapq.heappop(self.job_queue)  # Remove the job
                job_info = self.jobs[job_id]
                # Trigger the task using Worker
                self.worker.execute_task(job_info['task'], *job_info['args'], **job_info['kwargs'])
                self.remove_job(job_id)
            else:
                time.sleep((next_run_time - now).total_seconds())
                
    def load_jobs(self):
        if not os.path.exists(self.persistence_file):
            return {}

        with open(self.persistence_file, 'r') as file:
            return json.load(file)

    def save_jobs(self):
        with open(self.persistence_file, 'w') as file:
            json.dump(self.jobs, file, indent=4)

    def add_job(self, task, schedule_time: datetime,args=(), kwargs={}):
        task_info = f"{task}{args}{kwargs}"
    
        # Compute a hash of the concatenated string
        task_hash = hashlib.sha256(task_info.encode()).hexdigest()
    
        job_id = str(datetime.now().timestamp())
        self.jobs[job_id] = {
            'task': task,
            'schedule_time': schedule_time.isoformat(),
            'task_identifier': task_hash,
            'args': args,
            'kwargs': kwargs
        }
        self.save_jobs()
        self.logger.debug("Added job %s", job_id)  # Log when a job is added
        return job_id

    def remove_job(self, job_id):
        if job_id in self.jobs:
            del self.jobs[job_id]
            self.save_jobs()
            self.logger.debug("Removed job %s", job_id)  # Log when a job is removed

    def get_jobs(self):
        return self.jobs
