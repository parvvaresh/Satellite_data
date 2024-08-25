import numpy as np
import pandas as pd
from tqdm import tqdm
import os

from process.data.parser import get_bannds
from process.utils import get_all_dates, bands_per_date, create_folder, save_json


def extract_information(block_pixles : pd.DataFrame, all_dates : list, all_bands : list, date_and_bands : dict, npy_folder : str) -> None:
    """
        update
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



def get_meta_data(block_pixles : pd.DataFrame, class_column : str, LatLong_column : list) -> list:
    class_json = {}
    gefeat_json = {}

    for name, df_group in (block_pixles):
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

def split_df(df : pd.DataFrame, pixle_id_column : str) -> tuple:
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
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                full_path = os.path.join(root, file)
                csv_files.append((file, full_path))
    return csv_files[ : ]






def get_columns(csv_files: list) -> list:
    columns = set()
    for _, csv_file in tqdm(csv_files):
        with open(csv_file, 'r') as f:
            header = f.readline().strip().split(',')
            columns.update(header)  
    return list(columns)


def merge_csv(empty : pd.DataFrame, df : pd.DataFrame) -> pd.DataFrame:

    dfs = [empty, df]

    merged_df = pd.concat(dfs, ignore_index=True)
    return merged_df



def fix_dataset(data : pd.DataFrame, geo_cache : dict) -> pd.DataFrame:

    data[['SA', 'SP', 'SF']] = data['id_fid'].map(lambda x: geo_cache.get(x, (None, None, None))).apply(pd.Series)
    final_database = pd.concat([group_df for _, group_df in data.groupby(['id_fid', 'id_9'])], ignore_index=True)
    return final_database



def save_metadata(root_path : str , classes : dict , gefeat : dict, date : dict, class_info : dict, geo_info : dict) -> None:

    root_path_meta = root_path + "/META"
    create_folder(root_path_meta)

    classes = dict(sorted(classes.items() , key=lambda item : int(item[0])))
    gefeat = dict(sorted(gefeat.items() , key=lambda item : int(item[0])))
    

    save_json(classes, root_path_meta + "/labels.json")
    save_json(gefeat, root_path_meta + "/geomfeat.json")
    save_json(date, root_path_meta + "/dates.json")


    save_json(class_info, root_path + "/class_info.json")
    save_json(geo_info, root_path+ "/geoMeta_info.json")