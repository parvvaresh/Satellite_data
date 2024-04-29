import numpy as np
import json

def save_json(path: str,
              data: dict) -> None:
    def convert_to_json_serializable(obj):
        if isinstance(obj, np.uint16):
            return int(obj)
        else:
            return obj
    
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4, default=convert_to_json_serializable)
