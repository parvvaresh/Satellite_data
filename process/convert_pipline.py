import pandas as pd
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from .to_npy import (csv_to_npy_all,
                    csv_to_npy_split)

from .utils import  (create_folder,
                     merge_csv,
                     fillna,
                     add_geometric,
                     get_columns,
                     save_metadata)



class convert_data:
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path
        self._create_folder()
        print("create folder")

    def _create_folder(self):
        self.root_path = self.root_path + "/data"
        create_folder(self.root_path)
        self.root_path_all =  self.root_path + "/all"
        create_folder(self.root_path_all)
        self.root_path_all_data = self.root_path_all + "/DATA"
        create_folder(self.root_path_all_data)
        self.root_path_split = self.root_path + "/split"
        create_folder(self.root_path_split)
        self.root_path_split_data = self.root_path_split + "/DATA"
        create_folder(self.root_path_split_data)
        print("create folder on path ")

    def fit(self, geoJSON: dict, csv_paths: list, class_column: str, latlong_columns: list, block_column: str, start_date: datetime, finish_date: datetime, interval: int) -> None:
        self.geo_cache = {element["properties"]["FID"]: (element["properties"]["SA"],
                                                        element["properties"]["SP"],
                                                        element["properties"]["SF"])
                          for element in geoJSON["features"]}
        self.csv_paths = csv_paths
        self.class_column = class_column
        self.latlong_columns = latlong_columns
        self.block_column = block_column
        self.start_date = start_date
        self.finish_date = finish_date
        self.interval = interval

        self.class_info_split = None
        self.geo_info_split = None
        self.date_split = None
        self.classes_split = dict()
        self.gefeat_split = dict()

        self.class_info_all = None
        self.geo_info_all = None
        self.date_all = None
        self.classes_all = dict()
        self.gefeat_all = dict()

    def _transform_dataframe(self, csv: tuple):
        path = csv[1]
        name = csv[0].split(".csv")[0]
        df = pd.read_csv(path)
        df = merge_csv(self.empty_df, df)
        if df.shape[0] == 0:
            return None
        df = fillna(df)
        df = add_geometric(df, self.geo_cache)
        
        # Process 'all'
        model_all = csv_to_npy_all(df, 
                                   self.class_column,
                                   self.latlong_columns,
                                   self.block_column,
                                   self.root_path_all_data,
                                   self.start_date,
                                   self.finish_date,
                                   self.interval)
        model_all.get_npy()
        date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all = model_all.get_meta()
        
        # Process 'split'
        model_split = csv_to_npy_split(df, 
                                       self.class_column,
                                       self.latlong_columns,
                                       self.block_column,
                                       self.root_path_split_data,
                                       self.start_date,
                                       self.finish_date,
                                       self.interval)
        model_split.get_npy()
        date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split = model_split.get_meta()
        
        return (date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all,
                date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split)

    def transform(self) -> None:
        print("start collect columns of data sets")
        all_columns = get_columns(self.csv_paths)
        print("finish collect columns of data sets")
        self.empty_df = pd.DataFrame(columns=all_columns)
        print("start converting data")

        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(self._transform_dataframe, csv) for csv in self.csv_paths]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    result = future.result()  
                    if result:
                        (date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all,
                         date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split) = result
                        
                        self.classes_all.update(class_json_all)
                        self.gefeat_all.update(gefeat_json_all)
                        self.class_info_all = class_info_all  
                        self.geo_info_all = geo_info_all  
                        self.date_all = date_all  
                        
                        self.classes_split.update(class_json_split)
                        self.gefeat_split.update(gefeat_json_split)
                        self.class_info_split = class_info_split  
                        self.geo_info_split = geo_info_split  
                        self.date_split = date_split  
                        
                except Exception as e:
                    print(f"An error occurred: {e}")

        print("finish converting data")
        print("start save metadata")

        # for all 
        save_metadata(self.root_path_all, self.classes_all, self.gefeat_all, self.date_all, self.class_info_all, self.geo_info_all)
        # for split
        save_metadata(self.root_path_split, self.classes_split, self.gefeat_split, self.date_split, self.class_info_split, self.geo_info_split)
        print("finish save metadata")



