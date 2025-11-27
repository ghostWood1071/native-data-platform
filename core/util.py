import importlib
import json


def import_module(module_name):
    module_path = ".".join(module_name.split(".")[:-1])
    class_name = module_name.split(".")[-1]
    module = importlib.import_module(module_path)
    class_name_obj = getattr(module, class_name)
    return class_name_obj


def read_json(json_path):
    with open(json_path, mode="r") as f:
        data = json.loads(f.read())
        print(data)
        return data
