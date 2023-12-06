import json
import os
from pathlib import Path
from typing import Callable, Any, Dict, Tuple, Optional

class FunctionMap:
    def __init__(self, map_file: Optional[Path] = None):
        """
        Initializes the FunctionMap class.

        Args:
            map_file (Optional[Path]): Path to the JSON file that contains the function mapping. 
                                       Defaults to 'function_map.json' in the CONFIG directory.
        """
        self.map_file = map_file or Path(os.getenv("CONFIG", "function_map.json"))
        self.function_map = self.load_function_map()

    def load_function_map(self) -> Dict[str, Tuple[str, str]]:
        """
        Loads the function map from the specified JSON file.

        Returns:
            Dict[str, Tuple[str, str]]: A dictionary mapping identifiers to (module, function) tuples.
        """
        try:
            with open(self.map_file, "r") as file:
                data = json.load(file)
                return {k: (v["module"], v["name"]) for k, v in data.items()}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_function_map(self) -> None:
        """
        Saves the current function map to the JSON file.
        """
        with open(self.map_file, "w") as file:
            serialized_map = {
                k: {"module": v[0], "name": v[1]} for k, v in self.function_map.items()
            }
            json.dump(serialized_map, file)

    def get_function(self, identifier: str) -> Optional[Callable]:
        """
        Retrieves a function by its identifier.

        Args:
            identifier (str): The identifier of the function.

        Returns:
            Optional[Callable]: The function object if found, otherwise None.
        """
        if identifier in self.function_map:
            module_name, func_name = self.function_map[identifier]
            module = __import__(module_name, globals(), locals(), [func_name], 0)
            return getattr(module, func_name)
        return None

    def add_function(self, identifier: str, func: Callable) -> None:
        """
        Adds a new function to the function map.

        Args:
            identifier (str): The identifier for the function.
            func (Callable): The function object to add.
        """
        self.function_map[identifier] = (func.__module__, func.__name__)
        self.save_function_map()

    @staticmethod
    def serialize_func(func_data: Tuple[str, str]) -> Dict[str, str]:
        """
        Serializes function data into a JSON serializable format.

        Args:
            func_data (Tuple[str, str]): The module and function name tuple.

        Returns:
            Dict[str, str]: A dictionary representation of the function data.
        """
        module_name, func_name = func_data
        return {"module": module_name, "name": func_name}

    @staticmethod
    def deserialize_func(func_data: Tuple[str, str]) -> Callable:
        """
        Deserializes function data from a tuple format.

        Args:
            func_data (Tuple[str, str]): The module and function name tuple.

        Returns:
            Callable: The deserialized function object.

        Raises:
            ValueError: If the function data format is invalid.
        """
        if isinstance(func_data, tuple) and len(func_data) == 2:
            module_name, func_name = func_data
            module = __import__(module_name, globals(), locals(), [func_name], 0)
            return getattr(module, func_name)
        else:
            raise ValueError(f"Invalid function data format {func_data}")
        
    def parse_and_call(self, func: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        """
        Parses arguments and keyword arguments, and calls the given function.

        Args:
            func (Callable): The function to call.
            args (Tuple[Any, ...]): The positional arguments to pass to the function.
            kwargs (Dict[str, Any]): The keyword arguments to pass to the function.

        Returns:
            Any: The result of the function call.
        """
        args = args if args is not None else ()
        kwargs = kwargs if kwargs is not None else {}

        arg_names = func.__code__.co_varnames[:func.__code__.co_argcount]
        args_dict = dict(zip(arg_names, args))
        combined_kwargs = {**args_dict, **kwargs}
        return func(**combined_kwargs)
