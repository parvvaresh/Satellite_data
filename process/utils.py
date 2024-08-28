"""
    A set of functions written to help convert data into the appropriate format
"""

import os
import pandas as pd
import numpy as np

import json
from collections import Counter
from datetime import datetime, timedelta
from typing import Union
import pickle
from tqdm import tqdm
from datetime import datetime, timedelta

from .data.parser import get_bannds, get_classindex






def ExtractSave_information(block_pixles : pd.DataFrame, all_dates : list, all_bands : list, date_and_bands : dict, npy_folder : str) -> None:
    """ 
        Extracting and organizing band information based on date and saving it in the appropriate format (.npy)
    """
    for name, df_group in (block_pixles):
        vector = list()

        for date in all_dates:
            spectrum_vector = create_vector_bands(all_bands , df_group.shape[0])  
            vector_day = make_vector_for_each_date(df_group, date, spectrum_vector, date_and_bands)
            vector.append(vector_day)
            
        vector = np.array(vector)
    
        path =  npy_folder + f"/{name}.npy"
        np.save(path, vector)



def get_metadata(block_pixles : pd.DataFrame, class_column : str, LatLong_column : list) -> list:
    """
        Extract metadata such as classes, geographical features, and geospatial information.
    """
    classes = {}
    geofeat = {}
    info_geo = None

    for name, df_group in (block_pixles):
        name = str(name)
        class_sub = df_group[class_column].to_list()[0]
        classes[name] = class_sub

        geo , info_geo = extract_geo(df_group, LatLong_column)
        geofeat[name] = geo

    return classes, geofeat, info_geo


def create_vector_bands(bands : list, lenght : int) -> dict:
    """
         in this function we have to create a vector for bands and defeault nan  values (list type)
    """
    return {band: np.full(lenght , np.nan) 
                    for band in bands}
    
def make_vector_for_each_date(df_group : pd.DataFrame, date : int, bands_vector : dict, date_and_bands : dict) -> np.array:  
    """
        in this function we create a vector for each date
        frist we collect all bands for each date
        step 1 -> we check spectrum in this daye is existing?
        if existing generation columns and extract data nd save it 
    """
        
    for band in bands_vector:
        if band in date_and_bands[date]:
            column = f"{date}_{band}"
            bands_vector[band] = df_group[column].values

    return np.array(
                    list(bands_vector.values())
                    )


def extract_geo(sub_df : pd.DataFrame, LatLong_column : list) -> None:
    """ 
        Extraction and organization of geographic information such as elevation gradient and geographic coordinates
    """
    bands_goe = get_bannds()["geo_meta"]
    x , y = LatLong_column

    result = {}
    for geo in bands_goe:
        matching_columns = [col for col in sub_df.columns if geo.upper() in col.upper()]
        if matching_columns:
            result[geo] = sub_df[matching_columns].mean(axis=1).mean()

        
    result["x"] = sub_df[x].mean()
    result["y"] = sub_df[x].mean()

    return list(result.values()) , list(result.keys())

def split_satellite(df : pd.DataFrame, pixle_id_column : str) -> tuple:
    """
        Split the dataset into S1 and S2 based on specific bands and pixel ID column.
    """

    date_and_band = bands_per_date(df)
    dates = get_all_dates(date_and_band)

    s1_columns_base = get_bannds()["s1"]+ get_bannds()["Subscription"]
    s2_columns_base = get_bannds()["s2"]+ get_bannds()["Subscription"]



    s1_columns = [f"{date}_{band}" for date in dates for band in s1_columns_base]
    s2_columns = [f"{date}_{band}" for date in dates for band in s2_columns_base]
    

    exisiting_columns = set(df.columns)

    s1_columns = [col for col in s1_columns if col in exisiting_columns]
    s2_columns = [col for col in s2_columns if col in exisiting_columns]

    s1_columns.append(pixle_id_column)
    s2_columns.append(pixle_id_column)

    df_s1  = df[s1_columns]
    df_s2 =  df[s2_columns]

    return  df_s1, df_s2






def get_csv(folder_path : str) -> list:
    """
        Get all CSV file names and their paths in the folder.
    """
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                full_path = os.path.join(root, file)
                csv_files.append((file, full_path))
    return csv_files[ : ]






def get_columns(csv_files: list) -> list:
    """
        It collects all the columns so that all fields have the same data frame
    """
    columns = set()
    for _, csv_file in tqdm(csv_files):
        with open(csv_file, 'r') as f:
            header = f.readline().strip().split(',')
            columns.update(header)  
    return list(columns)


def merge_csv(empty_df : pd.DataFrame, df : pd.DataFrame) -> pd.DataFrame:
    """

    """

    dfs = [empty_df, df]

    merged_df = pd.concat(dfs, ignore_index=True)
    return merged_df



def add_geometric(data : pd.DataFrame, geo_cache : dict) -> pd.DataFrame:
    """

    """
    database = []

    for id_fid, group_df in data.groupby('id_fid'):
        SA, SP, SF = geo_cache.get(id_fid, (None, None, None))
        
        group_df["SA"] = SA
        group_df["SP"] = SP
        group_df["SF"] = SF
        
        database_90 = []

        for _, group_df_90 in group_df.groupby("id_9"):
            database_90.append(group_df_90)
        try:
            database.append(pd.concat(database_90, ignore_index=True))
        except:
            pass
    
    final_database = pd.concat(database, ignore_index=True)

    return final_database



def save_metadata(root_path : str , classes : dict , gefeat : dict, date : dict, class_info : dict, geo_info : dict) -> None:
    """
        Save metadata such as labels, date, geographical features, class information, and geospatial information
    """

    root_path_meta = root_path + "/META"
    create_folder(root_path_meta)

    classes = dict(sorted(classes.items() , key=lambda item : int(item[0])))
    gefeat = dict(sorted(gefeat.items() , key=lambda item : int(item[0])))
    

    save_json(classes, root_path_meta + "/labels.json")
    save_json(gefeat, root_path_meta + "/geofeat.json")
    save_json(date, root_path_meta + "/dates.json")


    save_json(class_info, root_path + "/class_info.json")
    save_json(geo_info, root_path+ "/geoMeta_info.json")


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



def fix_date(start_date: datetime, step: int, finish_date: datetime) -> list:
    """
       Break dates using Google Earth Engine date interval and convert the date to the correct format
    """

    dates = []
    current_date = start_date

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


def encode_class(class_dict : dict) -> list:
    """
        Encoding labels into numbers to enter models (ML | DL)
    """
    class_index = get_classindex()
    
    for num_file, _class in class_dict.items():
        class_dict[num_file] = class_index[_class]
    

    return class_dict, class_index



def save_pkl(data : Union[tuple, np.ndarray], path : str) -> None:
    """
        save pkl file 
    """
    with open(path, 'wb') as file:
        pickle.dump(data, file)


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
    """
        How many bands does each date have?
    """
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



def check_column(bands : list, col : str) -> bool:
    """
        
    """
    for band in bands:
        if band.upper() in col.upper():
            return True
    return False



def _mean_std(df: pd.DataFrame) -> tuple:
    """
        Calculation of mean and standard deviation
        Each date has a band number The mean and standard deviation of the band are calculated for each date
    """
    _bands_per_date = bands_per_date(df)
    _all_bands = get_all_bands(_bands_per_date)
    _all_dates = get_all_dates(_bands_per_date)

    mean = []
    std = []

    for date in _all_dates:
        sample_data = np.array(df[[f"{date}_{band}" for band in _all_bands]])
        if sample_data.size == 0:
            continue  # Skip if no data for this date
        mean.append(np.mean(sample_data, axis=0))
        std.append(np.std(sample_data, axis=0))

    if not mean: 
        return np.array([]), np.array([])

    mean = np.array(mean)
    std = np.array(std)

    return mean, std

def mean_std(csvs: list, empty_df : pd.DataFrame) -> tuple:
    """

    """
    total_samples = 0
    mean_sum = None
    variance_sum = None

    for csv in tqdm(csvs):
        path = csv[1]

        df = pd.read_csv(path)
        df = merge_csv(df, empty_df)
        df = fillna(df)
        df_s1, df_s2 = split_satellite(df, "id_9")

        mean, std = _mean_std(df_s2)

        if mean.size == 0 or std.size == 0:
            continue 

        if mean_sum is None:
            mean_sum = np.zeros_like(mean)
            variance_sum = np.zeros_like(mean)
        
        n_samples = mean.shape[0]  

        mean_sum += mean * n_samples
        variance_sum += np.power(std, 2) * n_samples

        total_samples += n_samples

    if total_samples == 0:
        raise ValueError("No valid data available to compute mean and std.")

    overall_mean = mean_sum / total_samples

    overall_variance = variance_sum / total_samples
    overall_variance = np.maximum(overall_variance, 0)

    overall_std = np.sqrt(overall_variance)
    return overall_mean, overall_std


def fillna(df: pd.DataFrame) -> pd.DataFrame:
    """
       For each band, we fill in the missing dates with the linear interpolation method using the dates around it 
    """
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




   

def merge_csv(empty : pd.DataFrame, df : pd.DataFrame) -> pd.DataFrame:
    """
        We make an empty data frame with the columns collected from all the data frames (fields) 
        and merge the data frame with it so that the current data frame has all the columns.
    """
    dfs = [empty, df]

    merged_df = pd.concat(dfs, ignore_index=True)
    return merged_df


   



def open_json_file(file_path : str) -> dict:
    """
        To open json files
    """
    with open(file_path, 'r') as json_file:
        data_dict = json.load(json_file)
    return data_dict


