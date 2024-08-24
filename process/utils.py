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
from .data.parser import get_bannds

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
        pass


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
    




def fix_date(start_date: datetime, step: int, finish_date: datetime) -> list:

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


def fix_class(class_dict : dict) -> list:
    class_index = {'WR': 0,
                    'ot': 1,
                    'S': 2,
                    'VS': 3,
                    'BE': 4,
                    'PO': 5,
                    'o': 6,
                    'SB': 7,
                    'BR': 8,
                    'po': 9,
                    'BI': 10,
                    'HN': 11,
                    'PT_a': 12,
                    'R': 13,
                    'PT_p': 14,
                    'NK': 15,
                    'V': 16,
                    'C': 17,
                    's': 18,
                    'M': 19,
                    'nk': 20,
                    'Wr': 21,
                    'G': 22,
                    'PT_rod': 23,
                    'p': 24,
                    'joob': 25,
                    'WI': 26,
                    'B': 27,
                    'bi': 28,
                    'PT_stakhar': 29,
                    'PTWr': 30,
                    'z': 31,
                    'P': 32,
                    'CA': 33,
                    'OF': 34,
                    'br': 35,
                    'wi': 36,
                    'b': 37,
                    'A': 38,
                    'wr': 39,
                    'c': 40,
                    'Z': 41,
                    'Water': 42,
                    'O': 43,
                    'TO': 44,
                    'G-h': 45,
                    'm': 46,
                    'OT': 47,
                    'f': 48,
                    'FO': 49,
                    'F': 50}
    
    for num_file, _class in class_dict.items():
        class_dict[num_file] = class_index[_class]
    

    return class_dict, class_index



def save_pkl(data : Union[tuple, np.ndarray], path : str) -> None:
    with open(path, 'wb') as file:
        pickle.dump(data, file)



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
    bands = get_bannds()["s1"] + get_bannds()["s2"] + get_bannds()["Subscription"]

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
    
    valid_column = get_bannds()["s1"] + get_bannds()["s1"] + get_bannds()["geo_features"] + [Class_columns, block_id] + LatLong_column

    for column in df.columns:
        if not check_column(valid_column, column):
            df = df.drop(column, axis=1)

    return df



def check_column(bands : list, 
          col : str):
    for band in bands:
        if band.upper() in col.upper():
            return True
    return False



def mean_std(df : pd.DataFrame) -> None:
    _bands_per_date = bands_per_date(df)
    _all_bands = get_all_bands(_bands_per_date)
    _all_dates = get_all_dates(_bands_per_date)
    print(_all_dates)
    mean = []
    std = []

    for date in _all_dates:
        sample_bands_mean = []
        sample_bands_std = []
        for band in _all_bands:
            name_column = f"{date}_{band}"
            data = df[name_column]
            _mean = data.dropna().mean()
            sample_bands_mean.append(_mean)


            _std = data.dropna().std()
            sample_bands_std.append(_std)
        mean.append(np.array(sample_bands_mean))
        std.append(np.array(sample_bands_std))
    

    mean = np.array(mean)
    std =  np.array(std)
    
    result = (mean, std)
    return result





def fillna(df: pd.DataFrame) -> pd.DataFrame:
    _bands_per_date = bands_per_date(df)
    _all_date = get_all_dates(_bands_per_date)
    _all_bands = get_all_bands(_bands_per_date) + ["Emis_31", "Emis_32", "BSI", "slope", "max", "min", "mea"]

    save_col = []
    list_dataframe = []

    for band in _all_bands:
        temp_cols = [f"{date}_{band}" for date in _all_date if f"{date}_{band}" in df.columns]

        if temp_cols:
            temp_cols_sorted = sorted(temp_cols, key=lambda col: int(find_date_band(col)[0]))
            save_col.extend(temp_cols_sorted)
            band_df = df[temp_cols_sorted].apply(pd.to_numeric, errors='coerce')  
            band_df = band_df.interpolate(method='linear', axis=1, limit=5, limit_direction='both') 
            list_dataframe.append(band_df)

    not_change = df.columns.difference(save_col)
    df_no_change = df[not_change]

    list_dataframe.append(df_no_change)
    
    result = pd.concat(list_dataframe, axis=1)
    
    return result



def open_json_file(file_path):
    with open(file_path, 'r') as json_file:
        data_dict = json.load(json_file)
    return data_dict
