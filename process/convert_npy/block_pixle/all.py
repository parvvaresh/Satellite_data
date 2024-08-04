import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm


import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/reza/Desktop/Satellite_data')))


from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class

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

        root_path = self.path_to_save + "/npy_data_for_all_sentinel_block_pixle"
        create_folder(root_path)
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        block_pixles = self.df.groupby(self.pixle_id_columns)
        
        
        
        
        date_and_band = bands_per_date(self.df)
        all_bands= get_all_bands(date_and_band)
        all_dates = get_all_dates(date_and_band)
        
        
        extract_information(block_pixles , all_dates , all_bands , date_and_band , npy_folder)
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

        save_json(class_json, meta_folder + "/class.json")
        save_json(gefeat_json, meta_folder + "/geo.json")
        save_json(date, meta_folder + "/date.json")


        print("saved  meta files in path  ")

        
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