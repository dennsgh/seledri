import os
from pathlib import Path
from celery import Celery
import signal
from multiprocessing import Process
# Define a simple Celery task
app = Celery('simple_task', broker='memory://', backend='file:///' + str(Path(os.getenv("DATA", "C:\\path\\to\\your\\data")).resolve()).replace("\\", "/"))

@app.task
def add(x, y):
    return x + y

def start_celery_worker(app: Celery, num_workers):
    argv = ["worker", "--loglevel=INFO", f"--concurrency={num_workers}","-E"]
    app.conf.worker_concurrency = num_workers
    # Convert the path to a string and replace backslashes with forward slashes
    app.conf.result_backend = "file:///" + str(Path(os.getenv("DATA"))).replace("\\","/")
    app.conf.worker_redirect_stdouts = True
    app.conf.worker_redirect_stdouts_level = "DEBUG"
    app.worker_main(argv)
    
process = Process(target=start_celery_worker, args=(app,1),daemon=True)

def signal_handler(signum, frame):
    """Gracefully shut down the worker and Flower processes."""
    process.terminate()

def start_worker():
    signal.signal(signal.SIGINT, signal_handler)
    process.start()

if __name__ == '__main__':
    # Start the Celery worker
    start_worker()

    #result = add.delay(args=(4, 4),countdown=1)
    result = add.delay(4, 4)
    print(result.get(timeout=5))  # Should print 8 after the task is processed by the worker
