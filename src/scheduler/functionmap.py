import json
from pathlib import Path


class FunctionMap:
    def __init__(self, map_file: Path = ".function_map.json"):
        self.map_file = map_file
        self.function_map = self.load_function_map()

    def load_function_map(self):
        try:
            with open(self.map_file, "r") as file:
                data = json.load(file)
                return {k: (v["module"], v["name"]) for k, v in data.items()}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_function_map(self):
        with open(self.map_file, "w") as file:
            serialized_map = {
                k: {"module": v[0], "name": v[1]} for k, v in self.function_map.items()
            }
            json.dump(serialized_map, file)

    def get_function(self, identifier):
        if identifier in self.function_map:
            module_name, func_name = self.function_map[identifier]
            module = __import__(module_name, fromlist=[func_name])
            return getattr(module, func_name)
        return None

    def add_function(self, identifier, func):
        # Assume func is a function object
        self.function_map[identifier] = (func.__module__, func.__name__)
        self.save_function_map()

    @staticmethod
    def serialize_func(func_data):
        # Serialize function data into a JSON serializable format
        module_name, func_name = func_data
        return {"module": module_name, "name": func_name}

    @staticmethod
    def deserialize_func(func_data):
        # Check if func_data is a dictionary with 'module' and 'name' keys
        if (
            isinstance(func_data, dict)
            and "module" in func_data
            and "name" in func_data
        ):
            module_name = func_data["module"]
            func_name = func_data["name"]
            module = __import__(module_name, globals(), locals(), [func_name], 0)
            return getattr(module, func_name)
        else:
            raise ValueError(f"Invalid function data format {func_data}")
