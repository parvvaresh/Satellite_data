import numpy as np
import pandas as pd
from tqdm import tqdm

from process.data.parser import get_bannds
from process.utils import get_all_dates, bands_per_date


def extract_information(block_pixles : pd.DataFrame, all_dates : list, all_bands : list, date_and_bands : dict, npy_folder : str) -> None:
    """
        update
    """


    for name, df_group in tqdm(block_pixles):
        vector = list()

        for date in all_dates:
            spectrum_vector = create_vector_bands(all_bands , df_group.shape[0])  
            vector_day = make_vector_for_each_date(df_group, date, spectrum_vector, date_and_bands)
            vector.append(vector_day)
            
        vector = np.array(vector)
            
        path =  npy_folder + f"/{name}.npy"
        np.save(path, vector)



def get_meta_data(block_pixles : pd.DataFrame, class_column : str, LatLong_column : list) -> list:
    class_json = {}
    gefeat_json = {}

    for name, df_group in tqdm(block_pixles):
        name = str(name)
        _class_sub = df_group[class_column].to_list()[0]
        class_json[name] = _class_sub

        geo , info_geo = extract_geo(df_group, LatLong_column)

        gefeat_json[name] = geo

    return class_json, gefeat_json, info_geo


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

    geo_col = {}
    for geo in bands_goe:
        geo_col[geo] = []
        for col in sub_df.columns:
            if geo.upper() in col.upper():
                geo_col[geo].append(col)
        
    result = {}

    for geo, col in geo_col.items():
        mean = sub_df[col].mean(axis=1)
        mean = sum(mean) / mean.shape[0]
        result[geo] = mean
        
    x_mean = sub_df[x].mean()
    y_mean = sub_df[y].mean()

        
    result["x"] = x_mean
    result["y"] = y_mean

    return list(result.values()) , list(result.keys())

def split_df(df : pd.DataFrame, pixle_id_columns : str) -> list:
    """
        this founction for split dataset to S1 and S2 
    """

    date_and_band = bands_per_date(df)
    dates = get_all_dates(date_and_band)

    s1_columns_temp = get_bannds()["s1"]+ get_bannds()["Subscription"]
    s2_columns_temp = get_bannds()["s2"]+ get_bannds()["Subscription"]


    def create_columns(dates : list, columns : list) -> list:
        result = []
        for date in dates:
            for s1 in columns:
                result.append(f"{date}_{s1}")
        for column in result:
            if column  not in df.columns:
                result.remove(column)

        return result
    
    s1_columns = create_columns(dates=dates, columns=s1_columns_temp) + [pixle_id_columns]
    s2_columns = create_columns(dates=dates, columns=s2_columns_temp) + [pixle_id_columns]

    
    df_s1  = df[s1_columns]
    df_s2 =  df[s2_columns]

    return  df_s1, df_s2






