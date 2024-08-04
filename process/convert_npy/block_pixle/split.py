import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm


import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/reza/Desktop/Satellite_data')))


from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class, mean_std, save_pkl

from  utils import * 

class csv_to_npy():
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 LatLong_column : tuple,
                 pixle_id_columns : str,
                 path_to_save : str,
                 start_date : datetime,
                 finish_date : datetime,
                 iter : int) -> None:
        """
            update
        """
        self.df = df
        self.class_column = class_column
        self.LatLong_column = LatLong_column
        self.pixle_id_columns = pixle_id_columns
        self.path_to_save = path_to_save
        self.start_date = start_date
        self.finish_date = finish_date
        self.iter = iter
    
    def get_npy(self) -> None:

        root_path = self.path_to_save + "/split"
        create_folder(root_path)
        
        npy_folder_s1 = root_path + "/DATA/S1"
        create_folder(npy_folder_s1)

        npy_folder_s2 = root_path + "/DATA/S2"
        create_folder(npy_folder_s2)


        df_s1, df_s2 = split_df(self.df, self.pixle_id_columns)
        block_pixles = self.df.groupby(self.pixle_id_columns)

        block_pixles_s1 = df_s1.groupby(self.pixle_id_columns)
        block_pixles_s2 = df_s2.groupby(self.pixle_id_columns)
        
        
        
        date_and_band_s1 = bands_per_date(df_s1)
        all_bands_s1= get_all_bands(date_and_band_s1)
        all_dates_s1 = get_all_dates(date_and_band_s1)

        date_and_band_s2 = bands_per_date(df_s2)
        all_bands_s2= get_all_bands(date_and_band_s2)
        all_dates_s2 = get_all_dates(date_and_band_s2)
        
        extract_information(block_pixles_s1 , all_dates_s1 , all_bands_s1 , date_and_band_s1 , npy_folder_s1)
        extract_information(block_pixles_s2 , all_dates_s2 , all_bands_s2 , date_and_band_s2 , npy_folder_s2)

        class_json, gefeat_json, info_geo = get_meta_data(block_pixles , self.class_column , self.LatLong_column)
        

        

        print("saved npy files in path  ")


        # saved info dates     
        date = fix_date(self.start_date ,self.iter, self.finish_date )
        save_json(info_geo, root_path + "/info_geo.json")



        class_json , class_info = fix_class(class_json)
        # saved info class
        save_json(class_info, root_path + "/info_class.json")

        meta_folder = root_path + "/META"
        create_folder(meta_folder)

        save_json(class_json, meta_folder + "/labels.json")
        save_json(gefeat_json, meta_folder + "/geomfeat.json")
        save_json(date, meta_folder + "/dates.json")


        print("saved  meta files in path  ")

        # saved mean std files

        mean_std_s1 = mean_std(df_s1)
        mean_std_s2 = mean_std(df_s2)

        save_pkl(mean_std_s1, root_path + "/mean_std_s1.pkl")
        save_pkl(mean_std_s2, root_path + "/mean_std_s2.pkl")
        
        print("finish convert")


        





df = pd.read_csv("/home/reza/Desktop/abyek_half.csv")

model = csv_to_npy(df, 
                 "class" ,
                 ["X", "Y"],
                 "id_9",
                 "/home/reza/Desktop/" ,
                "2023-02-01",
                "2023-08-01",
                7,)


model.get_npy()