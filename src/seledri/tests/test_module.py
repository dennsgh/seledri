import os
from pathlib import Path

def print_2(message):
    print(message)
    Path(os.getenv("LOGS"), f"{message}").touch()
