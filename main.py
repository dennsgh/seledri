from worker.worker import Worker
from scheduler.timekeeper import PersistentScheduler
from datetime import datetime, timedelta
# Initialize Worker
worker = Worker(function_map_file='function_map.json',broker='memory://',
                          backend='cache+memory://')

worker.function_map.add_function('custom_task', print)

# Register task with arguments
worker.register_task('custom_task', 'arg1')

# Schedule a task
scheduler = PersistentScheduler('jobs.json')
tomorrow = datetime.now() + timedelta(days=1)
schedule_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 10, 30, 0)
scheduler.add_job('test_task', schedule_time, args=('arg1'))

worker.start_worker()