import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from collections import Counter
from datetime import datetime, timedelta
import shutil



from data.parser import get_bannds

##########################################################################
# Handling data storage
##########################################################################

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

def remove_circular_references(obj, seen=None):
    """
    Recursively remove circular references from the data.
    """
    if seen is None:
        seen = set()
    if id(obj) in seen:
        return None  # Circular reference found; replace with None or a placeholder
    seen.add(id(obj))

    if isinstance(obj, dict):
        return {k: remove_circular_references(v, seen) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_circular_references(i, seen) for i in obj]
    elif isinstance(obj, tuple):
        return tuple(remove_circular_references(i, seen) for i in obj)
    elif isinstance(obj, set):
        return {remove_circular_references(i, seen) for i in obj}
    
    return obj

def save_json(data : dict,
              path : str) -> None:
    """
        Storage of band information and... as a Jason file
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = remove_circular_references(data)
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4, default=convert_to_json_serializable)



def save_meta_data(path : str,
                    df : pd.DataFrame,
                    geomfeat_columns : list,
                    class_column : str,
                    start_date : datetime,
                    step : int,
                    iter : int) -> None:
    pass

    # geomfeat = fix_geomfeat(df , geomfeat_columns)
    # label = fix_label(df, class_column)
    # date = fix_date(start_date , step , iter)
    
        
    # save_json(path + "/geomfeat.json", geomfeat)
    # print("geomfeat METADATA saved successfully")
        
    # save_json(path + "/label.json", label)
    # print("label METADATA saved successfully")

    # save_json(path + "/date.json", date)
    # print("date METADATA saved successfully")
    


def copy_paste(source_file : str,
               destination_dir : str) -> None:    
    shutil.copy2(source_file, destination_dir)





##########################################################################
# Diagram for better illustration
##########################################################################

def point_plot(data : dict,
               x_lable : str,
               y_lable : str,
               title : str,
               path_to_csv : str,
               show  : bool = True) -> None:
    """
        To illustrate the data: a dot chart that displays the numbers
    """
    plt.figure(figsize=(15, 10))

    x = list(data.keys())
    y = list(data.values())
    x = list(map(lambda x : str(x), x))

    for _x, _y in zip(x, y):
        plt.text(_x, _y, f'{_y:.0f}', fontsize=9, ha='center', va='bottom')

    plt.plot(x, y, marker='o', linestyle='--')
    plt.xlabel(x_lable)
    plt.ylabel(y_lable)
    plt.title(title)

    plt.grid(True)

    if show:
        plt.show()

    if path_to_csv is not None:
        plt.savefig(path_to_csv)

    plt.close()



##########################################################################
# EDA on the input dataset (.csv or .xlsx format)
##########################################################################

def find_date_band(text : str) -> list:
    """
        this founction for find date and band 
        for example : 
            input : 11_vv
            output : [11, vv]  ----> 11 is date | vv is band



                or


            input : 5_vh_1
            output : [6, vv_1]  ----> 9 is date | vv_1 is band
    """
    index = None
    for _index, char in enumerate(text):
        if char == "_":
            index = _index
            break
            
    return [
        text[ : index], # date
        text[index + 1 : ] # band
    ]                           


def bands_per_date(df : pd.DataFrame) -> dict:
    """
        this founction for extract date and bands for .csv file format
            output :

                    {
                        0 : ["B2, B3],
                        1 : ["VV", "VH"]
                    }

    """

        
    columns= list(df.columns)
    bands = get_bannds()["s1"] + get_bannds()["s2"] + get_bannds()["geo_features"]

    columns = [col for col in columns if check_column(bands, col)]
    
    _bands_per_date = dict()
    info = list(map(find_date_band, columns))
        
    for _info in info:
        date, band = int(_info[0]) , _info[1] 
        if date in _bands_per_date:
            _bands_per_date[date].add(band)
            
        elif date not in _bands_per_date:
            _bands_per_date[date] = set()
            _bands_per_date[date].add(band)  
                
    _bands_per_date = dict(sorted(_bands_per_date.items(), key=lambda item : item[0]))
    return _bands_per_date


def bands_count_per_date(_bands_per_date : dict) -> dict:
    _bands_count_per_date = dict()
    for date , bands in _bands_per_date.items():
        _bands_count_per_date[date] = len(bands)
    return _bands_count_per_date

def check_column(bands : list, 
          col : str):
    for band in bands:
        if band in col.upper():
            return True
    return False


def get_all_dates(bands_per_date : dict) -> list:
    """
        return all unique dates in data frame
    """
    
    all_date  = list()
    for day, _ in bands_per_date.items():
        all_date.append(day)
    return all_date


def get_all_bands(bands_per_date : dict) -> list:
    """
        return all unique bands in data frame
    """
    
    get_all_bands  = list()
    for _, band in bands_per_date.items():
        get_all_bands.extend(list(band))    
    return list(set(get_all_bands))




def counter_class(df : pd.DataFrame,
                  class_columns : str) -> dict:
    """
        How many of each label are available
    """
    classes = df[class_columns]
    Class_Counts = Counter(classes)
    return dict(Class_Counts)



##########################################################################
# Clean data 
##########################################################################

def clean_data(df : pd.DataFrame,
               Class_columns : str,
               LatLong_column : str,
               block_id : str) -> pd.DataFrame:
    
    """
        remove unvalid columns in dataframe 
    """
    
    valid_column = get_bannds["s1"] + get_bannds["s1"] + get_bannds["geo_features"] + [Class_columns, LatLong_column, block_id]

    for column in df.columns:
        if column not in valid_column:
            df = df.drop(column, axis=1)

    return df