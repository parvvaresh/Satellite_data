import pandas as pd
import glob
from tqdm import tqdm
import json
import os
import sys
import shutil
from datetime import datetime



from all import csv_to_npy_all
from split import csv_to_npy_split
from utils import * 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/reza/Desktop/Satellite_data')))
from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class, fillna






def open_json_file(file_path):
    with open(file_path, 'r') as json_file:
        data_dict = json.load(json_file)
    return data_dict










        

ellam = get_csv("/home/reza/Desktop/csv_temp_ellam")
hamedan = get_csv("/home/reza/Desktop/csv_temp_ham")


all_csv = ellam + hamedan


# empty_df = pd.DataFrame(columns=all_columns)

with open("/home/reza/Desktop/SEASON_A_WGS_edited_final.json", 'r') as f:
    geoJSON = json.load(f)





class convert_data:
    def __init__(self, root_path : str) -> None:


        

        self.root_path = root_path + "/data"
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

    def transform(self , geoJSON : dict, csv_paths : list, class_column : str, latlong_columns : list, block_column : str, start_date : datetime, finish_date : datetime, interval : int) -> None:


        geo_cache = {element["properties"]["FID"]: (element["properties"]["SA"],
                                                element["properties"]["SP"],
                                                element["properties"]["SF"])
                 for element in geoJSON["features"]}
        

        print("start collect columns of data sets")
        all_columns = get_columns(csv_paths)
        print("finish collect columns of data sets")


        empty_df = pd.DataFrame(columns=all_columns)

        # for save meta data

        class_info_split = None
        info_geo_split = None
        date_split = None
        classes_split = dict()
        gefeat_split = dict()


        class_info_all = None
        info_geo_all = None
        date_all = None
        classes_all = dict()
        gefeat_all = dict()


        print("start converting data")

        for csv in tqdm(csv_paths):
            path = csv[1]
            name = csv[0].split(".csv")[0]



            



            

            df = pd.read_csv(path)
            df = merge_csv(empty_df, df)
            if df.shape[0] == 0:
                continue
            df = fillna(df)

            df = fix_dataset(df, geo_cache)
            model = csv_to_npy_all(df, 
                            class_column,
                            latlong_columns,
                            block_column,
                            self.root_path_all_data,
                            start_date,
                            finish_date,
                            interval,)
            model.get_npy()


            date_all, class_json, class_info_all, gefeat_json, geo_info_all = model.get_meta()

            classes_all.update(class_json)
            gefeat_all.update(gefeat_json)

            model = csv_to_npy_split(df, 
                            class_column ,
                            latlong_columns,
                            block_column,
                            self.root_path_split_data ,
                            start_date,
                            finish_date,
                            interval,)
            model.get_npy()
            date_split, class_json, class_info_split, gefeat_json, geo_info_split = model.get_meta()

            classes_split.update(class_json)
            gefeat_split.update(gefeat_json)

        print("finish converting data")


        print("start save metadata")
        # for all 
        save_metadata(self.root_path_all, classes_all , gefeat_all, date_all, class_info_all, geo_info_all)
        # for split
        save_metadata(self.root_path_split, classes_split , gefeat_split, date_split, class_info_split, geo_info_split)
        print("finish save metadata")



model_cv = convert_data("/home/reza/Desktop")

model_cv.transform(geoJSON, all_csv , "class", ["X", "Y"] , "id_9" , "2022-11-01", "2023-11-30", 7)