import os
from pathlib import Path
from celery import Celery

# Define a simple Celery task
app = Celery('simple_task', broker='memory://', backend='file:///' + str(Path(os.getenv("DATA", "C:\\path\\to\\your\\data")).resolve()).replace("\\", "/"))

@app.task
def add(x, y):
    return x + y

def start_celery_worker(app: Celery, num_workers):
    argv = ["worker", "--loglevel=INFO", f"--concurrency={num_workers}", "-E"]
    app.conf.worker_concurrency = num_workers
    app.worker_main(argv)

if __name__ == '__main__':
    # Start the Celery worker
    start_celery_worker(app, num_workers=1)
