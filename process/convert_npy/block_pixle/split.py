import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm




from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class, mean_std, save_pkl

from  utils import * 

class csv_to_npy_split():
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


        
        npy_folder_s1 = self.path_to_save + "/S1"
        create_folder(npy_folder_s1)

        npy_folder_s2 = self.path_to_save + "/S2"
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

        class_json, self.gefeat_json, self.info_geo = get_meta_data(block_pixles , self.class_column , self.LatLong_column)
        

    
        self.date = fix_date(self.start_date ,self.iter, self.finish_date )



        self.class_json , self.class_info = fix_class(class_json)



    def get_meta(self):
        return self.date, self.class_json, self.class_info, self.gefeat_json, self.info_geo



