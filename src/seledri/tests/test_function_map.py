from pathlib import Path

from seledri.functionmap import FunctionMap


# Renamed test functions
def function_1(arg):
    print(f"Function 1: {arg}")


def function_2(arg):
    print(f"Function 2: {arg}")


def test_run_tests():
    filepath = Path("test_function_map.json")
    if filepath.exists():
        filepath.unlink()

    # Step 1: Add and save functions (Simulate first run)
    function_map = FunctionMap(filepath)
    function_map.add_function("func1", function_1)
    function_map.add_function("func2", function_2)
    function_map.save_function_map()

    # Step 2: Simulate application restart and load functions
    function_map_reloaded = FunctionMap(filepath)

    # Step 3: Test if functions are correctly loaded
    func1_loaded = function_map_reloaded.get_function("func1")
    func2_loaded = function_map_reloaded.get_function("func2")

    assert (
        func1_loaded is not None and func2_loaded is not None
    ), "Functions were not loaded correctly"
    assert callable(func1_loaded) and callable(
        func2_loaded
    ), "Loaded objects are not callable functions"

    # Step 4: Optionally invoke the loaded functions to test functionality
    func1_loaded("Hello")
    func2_loaded("World")

    print("All tests passed.")


if __name__ == "__main__":
    test_run_tests()
