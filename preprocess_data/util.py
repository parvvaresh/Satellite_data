import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime, timedelta

from .data.parser import get_spectrums, get_stopwords



def create_folder(path : str) -> None:
    """
        this founction for make a folder for save and organzie data
    """
    try:
        os.makedirs(path)
    except FileExistsError:
        print(f"Folder already exists at {path}")



def bar_plot(data : dict,
             path_destination : str ) -> None:
    """
        this fouction for generate bar plot
    """
    x_values = list(data.keys())
    y_values = list(data.values())

    plt.plot(x_values, y_values)
    plt.savefig(path_destination)
    plt.close()
    
    

def save_json(path: str,
              data: dict) -> None:
    """
        this foucntion for convert dictionary type to json and save it
    """
    def convert_to_json_serializable(obj):
        if isinstance(obj, np.uint16):
            return int(obj)
        else:
            return obj
    
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4, default=convert_to_json_serializable)


def find_date_and_spectrum(text : str) -> list:
    """
        this founction for find date and spectrum 
        for example  
            input : 11_vv
            output : [11, vv]  ----> 11 is day | vv is spectrum
             
    """
    index = None
    for _index, char in enumerate(text):
        if char == "_":
            index = _index
            break
            
    return [
        text[ : index],
        text[index + 1 : ]
    ]                           
    


def extract_date_and_spectrum(df : pd.DataFrame) -> dict:
    """
        this founction for extract date and spectrum for .csv file format
    """

    stopwords = get_stopwords()
        
    columns= list(df.columns)
    
    columns = [_info for _info in columns if _info not in stopwords]
    date_and_spectrum = dict()
    info = list(map(find_date_and_spectrum, columns))
        
    for _info in info:
        day, spectrum = int(_info[0]) , _info[1] 
        if day in date_and_spectrum:
            date_and_spectrum[day].add(spectrum)
            
        elif day not in date_and_spectrum:
            date_and_spectrum[day] = set()
            date_and_spectrum[day].add(spectrum)  
                
    date_and_spectrum = dict(sorted(date_and_spectrum.items(), key=lambda item : item[0]))
    return date_and_spectrum


def get_all_date(date_and_spectrum : dict) -> list:
    """

    """
    
    all_day  = list()
    for day, _ in date_and_spectrum.items():
        all_day.append(day)
    return all_day



def get_all_spectrum(date_and_spectrum : dict) -> list:
    
    all_spectrum  = list()
    for _, spectrum in date_and_spectrum.items():
        all_spectrum.extend(list(spectrum))
    return list(set(all_spectrum))








def get_all_npy(path : str) -> dict:
    files = dict()
    for root, dir , _files in os.walk(path):
        for file in _files:
            if file.endswith(".npy"):
                name = file.split(".npy")[0]
                files[name] = os.path.join(root, file) 
    return files



def track_sentinel(date_and_spectrum: dict) -> dict:
    spectrums_list = get_spectrums()
    s1 = dict()
    s2 = dict()
    combination = dict()
    
    for date, spectrums in date_and_spectrum.items():
        s1_spectrums = []
        s2_spectrums = []
        combination_spectrums = []
        
        for spectrum in spectrums:
            if spectrum in spectrums_list["s1"]:
                s1_spectrums.append(spectrum)
            elif spectrum in spectrums_list["s2"]:
                s2_spectrums.append(spectrum)
            else:
                combination_spectrums.append(spectrum)
        
        s1[date] = s1_spectrums
        s2[date] = s2_spectrums
        combination[date] = combination_spectrums
        
    return {
        "s1": s1,
        "s2": s2,
        "combination": combination
    }


"""
    
"""
def fix_label(df: pd.DataFrame, class_column: str):
    labels = dict()
    for index in range(df.shape[0]):
        labels.update({str(index): df.iloc[index][class_column]})

    result = {
        f"label_{len(df[class_column].unique())}class": labels
    }
    return result



def fix_geomfeat(df : pd.DataFrame, 
                 geomfeat_columns : tuple):
    geomfeat = dict()
    for index in range(df.shape[0]):
        geomfeat.update(
                    {str(index) : [df.iloc[index][geomfeat_columns[0]] , df.iloc[index][geomfeat_columns[1]]]}
                    )
    return geomfeat
    
    
    

def fix_date(start_date : datetime, 
                  step : int,
                  iter : int) -> list:
    dates = []
    current_date = start_date
    for _ in range(iter):
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=step)
        
    return dates


def saved_meta_data(path : str,
                    df : pd.DataFrame,
                    geomfeat_columns : list,
                    class_column : str,
                    start_date : datetime,
                    step : int,
                    iter : int) -> None:

    # step 1 - > extract data
    geomfeat = fix_geomfeat(df , geomfeat_columns)
    label = fix_label(df, class_column)
    date = fix_date(start_date , step , iter)
    
        
    # step 2 -> save it
    save_json(path + "/geomfeat.json", geomfeat)
    print("geomfeat METADATA saved successfully")
        
    save_json(path + "/label.json", label)
    print("label METADATA saved successfully")

    save_json(path + "/date.json", date)
    print("date METADATA saved successfully")