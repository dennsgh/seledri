import json
from datetime import datetime
from celery import signature
import os

class PersistentScheduler:
    def __init__(self, persistence_file):
        self.persistence_file = persistence_file
        self.jobs = self.load_jobs()

    def load_jobs(self):
        if not os.path.exists(self.persistence_file):
            return {}

        with open(self.persistence_file, 'r') as file:
            return json.load(file)

    def save_jobs(self):
        with open(self.persistence_file, 'w') as file:
            json.dump(self.jobs, file, indent=4)

    def add_job(self, task, schedule_time, args=(), kwargs={}):
        job_id = str(datetime.now().timestamp())  # Simple unique ID
        self.jobs[job_id] = {
            'task': task,
            'schedule_time': schedule_time.isoformat(),
            'args': args,
            'kwargs': kwargs
        }
        self.save_jobs()
        return job_id

    def remove_job(self, job_id):
        if job_id in self.jobs:
            del self.jobs[job_id]
            self.save_jobs()

    def get_jobs(self):
        return self.jobs
