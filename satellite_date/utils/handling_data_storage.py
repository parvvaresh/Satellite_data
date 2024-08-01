import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from collections import Counter
from datetime import datetime, timedelta
import shutil
from typing import Union
import pickle


from datetime import datetime, timedelta


def create_folder(path : str) -> None:
    """
        this founction for make a folder for save and organzie data
    """
    try:
        os.makedirs(path)
    except FileExistsError:
        print(f"Folder already exists at {path}")



def convert_to_json_serializable(obj):
    """
        Custom converter function to handle non-serializable objects.
    """
    try:
        return json.JSONEncoder().default(obj)
    except TypeError:
        return str(obj)  # Convert to string as a fallback



def save_json(data : dict,
              path : str) -> None:
    """
        Storage of band information and... as a Jason file
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4, default=convert_to_json_serializable)



def sequence_date(start_date: datetime, step: int, finish_date: datetime) -> list:
    """
        
    """

    dates = []
    current_date = start_date

    # Ensure that start_date and finish_date are datetime objects
    if isinstance(start_date, str):
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(finish_date, str):
        finish_date = datetime.strptime(finish_date, "%Y-%m-%d")

    while True:
        current_date += timedelta(days=step)
        if current_date > finish_date:
            break
        else:
            dates.append(current_date.strftime("%Y-%m-%d"))

    result = {}
    for index, element in enumerate(dates):
        result[index] = element
        
    return result


def class_encoder(class_dict : dict) -> list:
    """
        Encoding the classes numerically so that it is understandable for the model
    """
    list_class = list(set(class_dict.values()))
    class_index = {}
    for index, _class in enumerate(list_class):
        class_index[_class] = index
    
    for num_file, _class in class_dict.items():
        class_dict[num_file] = class_index[_class]
    

    return class_dict, class_index



def save_pkl(data : Union[tuple, np.ndarray], path : str) -> None: 
    """
        for save pkl file 
    """
    with open(path, 'wb') as file:
        pickle.dump(data, file)