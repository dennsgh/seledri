# Seledri

## Overview
This project provides a robust task scheduling system, utilizing a `Worker` class for task execution and a `Timekeeper` class for managing task scheduling and lifecycle. It is designed to handle tasks efficiently, ensuring they are executed and cleaned up properly.

## Features
- **Task Execution**: Execute tasks at specified times with the ability to pass arguments and keyword arguments.
- **Task Scheduling**: Schedule tasks with precision and handle their lifecycle.
- **Logging**: Comprehensive logging for monitoring task execution and job management.
- **Modularity**: Clear separation of concerns between task execution (`Worker`) and scheduling (`Timekeeper`).

## Installation
To set up the project environment, follow these steps:

1. Clone the repository:
```
git clone https://your-repository-url.git
```
2. Navigate to the project directory:
```
cd seledri
```
3. Install dependencies
```
pip install apscheduler
```


## Usage

...

### Starting the Scheduler
```python
from worker.worker_aps import Worker
from scheduler.timekeeper import Timekeeper
from pathlib import Path

# Initialize the Worker
worker = Worker(function_map_file=Path("path_to_function_map.json"))

# Start the worker
worker.start_worker()

# Initialize the Timekeeper with the worker instance
timekeeper = Timekeeper(persistence_file=Path("path_to_jobs.json"), worker_instance=worker)
```