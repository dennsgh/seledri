import json

class FunctionMap:
    def __init__(self, map_file='function_map.json'):
        self.map_file = map_file
        self.function_map = self.load_function_map()
        
    def load_function_map(self):
        try:
            with open(self.map_file, 'r') as file:
                data = json.load(file)
                return {k: self.deserialize_func(v) for k, v in data.items()}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_function_map(self):
        with open(self.map_file, 'w') as file:
            serialized_map = {k: self.serialize_func(v) for k, v in self.function_map.items()}
            json.dump(serialized_map, file)

    def add_function(self, identifier, func):
        self.function_map[identifier] = (func.__module__, func.__name__)
        self.save_function_map()

    def get_function(self, identifier):
        if identifier in self.function_map:
            module_name, func_name = self.function_map[identifier]
            module = __import__(module_name, fromlist=[func_name])
            return getattr(module, func_name)
        return None

    @staticmethod
    def serialize_func(func_data):
        return func_data

    @staticmethod
    def deserialize_func(func_data):
        module_name, func_name = func_data
        module = __import__(module_name, globals(), locals(), [func_name], 0)
        return getattr(module, func_name)