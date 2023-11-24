from worker.worker import Worker
from scheduler.timekeeper import PersistentScheduler
from datetime import datetime, timedelta

# Initialize Worker and Scheduler
worker = Worker(function_map_file='function_map.json', broker='memory://', backend='cache+memory://')

# Registering the functions!
worker.register_task(print, 'print', 'arg1')  # Pass the function reference 'print'

# Schedule a task
schedule_time = datetime.now() + timedelta(seconds=6)
scheduler = PersistentScheduler('jobs.json', worker)
scheduler.add_job('print_args', schedule_time, args=(str(datetime.now())))

# Start Scheduler (in a separate thread or as a background task)
worker.start_worker()
scheduler.start_scheduler()
