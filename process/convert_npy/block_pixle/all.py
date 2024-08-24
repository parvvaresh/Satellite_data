import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm






from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class

from  utils import * 

class csv_to_npy_all():
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


        
        block_pixles = self.df.groupby(self.pixle_id_columns)
        
        
        
        
        date_and_band = bands_per_date(self.df)
        all_bands= get_all_bands(date_and_band)
        all_dates = get_all_dates(date_and_band)
        
        
        extract_information(block_pixles , all_dates , all_bands , date_and_band , self.path_to_save)
        class_json, self.gefeat_json, self.info_geo = get_meta_data(block_pixles , self.class_column , self.LatLong_column)
        



        self.date = fix_date(self.start_date ,self.iter, self.finish_date )



        self.class_json , self.class_info = fix_class(class_json)



    def get_meta(self):
        return self.date, self.class_json, self.class_info, self.gefeat_json, self.info_geo







