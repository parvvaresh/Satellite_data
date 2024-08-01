import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm


import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/reza/Desktop/Satellite_data')))


from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class
from process.data.parser import get_bannds


class convert_csv_to_npy():
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 LatLong_column : tuple,
                 pixle_id_columns : str,
                 path_to_save : str,
                 start_date : datetime,
                 finish_date : datetime,
                 iter : int) -> None:
        self.df = df
        self.class_column = class_column
        self.LatLong_column = LatLong_column
        self.pixle_id_columns = pixle_id_columns
        self.path_to_save = path_to_save
        self.start_date = start_date
        self.finish_date = finish_date
        self.iter = iter
    
    def get_npy_for_all_sentinel(self) -> None:


        print("start clean data")
        #self.df = clean_data(self.df, self.class_column , self.LatLong_column, self.pixle_id_columns) 
        print("finish data")
        root_path = self.path_to_save + "/npy_data_for_all_sentinel_block_pixle"
        create_folder(root_path)
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        block_pixles = self.df.groupby(self.pixle_id_columns)
        
        
        
        
        date_and_band = bands_per_date(self.df)
        all_bands= get_all_bands(date_and_band)
        all_dates = get_all_dates(date_and_band)
        
        
        class_json = {}
        gefeat_json = {}
        date_json = {}
        
        print("start convert")
        for name, df_group in tqdm(block_pixles):
            name = str(name)
            vector = list()
            _class_sub = df_group[self.class_column].to_list()[0]
            class_json[name] = _class_sub

            geo , info_geo = self.get_geo(df_group)

            gefeat_json[name] = geo


            for date in all_dates:
                spectrum_vector = self._create_vector_spectrums(all_bands , df_group.shape[0]) # this is a lenght of vector 
                vector_day = self._make_vector_for_each_date(df_group, date, spectrum_vector, date_and_band)
                vector.append(vector_day)
            
            vector = np.array(vector)
            
            path =  npy_folder + f"/{name}.npy"
            np.save(path, vector)
        

        print("saved npy files in path")
            
        date = fix_date(self.start_date ,self.iter, self.finish_date )
        # step 5 -> save META data
        # step 6 -> create folder for save META data

        save_json(info_geo, root_path + "/info_geo.json")

        class_json , class_info = fix_class(class_json)

        save_json(class_info, root_path + "/info_class.json")

        meta_folder = root_path + "/META"
        create_folder(meta_folder)
        save_json(class_json, meta_folder + "/class.json")
        
        save_json(gefeat_json, meta_folder + "/geo.json")
        
        save_json(date, meta_folder + "/date.json")
        
        print("finish convert")


        
    def _create_vector_for_espcial_sentinel(self,
                                            df : pd.DataFrame,
                                            all_date_point : list,
                                            all_spectrums : list,
                                            date_and_spectrums : dict,) -> np.array:
        
        
        vector = list()
        for date in all_date_point:
            spectrum_vector = self._create_vector_spectrums(all_spectrums, df.shape[0])
            vector_day = self._make_vector_for_each_date(df ,date, spectrum_vector, date_and_spectrums)
            vector.append(vector_day)
            
        vector = np.array(vector)
        return vector
        
    def _create_vector_spectrums(self, 
                                 spectrums : list, 
                                 lenght : int) -> dict:
        
        """
        
         in this function we have to create a vector for spectrum and defeault nan  values (list type)
         
        """
        return {spectrum: np.full(lenght , np.nan) 
                    for spectrum in spectrums}
    
    def _make_vector_for_each_date(self,
                                   df : pd.DataFrame,
                                   date : int, 
                                   spectrum_vector : dict, 
                                   date_and_spectrum : dict) -> np.array:
        

        """
            in this function we create a vector for each date
            frist we collect all spectrum for each date
            step 1 -> we check spectrum in this daye is existing?
            if existing generation columns and extract data nd save it 
        """
        
        for spectrum in spectrum_vector:
            if spectrum in date_and_spectrum[date]:
                column = f"{date}_{spectrum}"
                spectrum_vector[spectrum] = df[column].values

        return np.array(
                        list(spectrum_vector.values())
                           )



    def get_geo(self, sub_df) -> None:
        bands_goe = get_bannds()["geo_meta"]
        x , y = self.LatLong_column


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

df = pd.read_csv("/home/reza/Desktop/abyek_half.csv")

model = convert_csv_to_npy(df, 
                 "class" ,
                 ["X", "Y"],
                 "id_9",
                 "/home/reza/Desktop/" ,
                "2023-02-01",
                "2023-08-01",
                7,)


model.get_npy_for_all_sentinel()