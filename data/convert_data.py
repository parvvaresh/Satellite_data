import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


from tool.extract_date_and_spectrum import extract_date_and_spectrum
from tool.get_all_spectrum import get_all_spectrum
from tool.get_all_day import get_all_day
from tool.create_folder import create_folder_for_save_npy
from tool.save_json import save_json

from tool.fix_Meta.fix_date import fix_date
from tool.fix_Meta.fix_geomfeat import fix_geomfeat
from tool.fix_Meta.fix_label import fix_label



class convert_data:
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 geomfeat_columns : tuple,
                 path : str,
                 start_date : datetime,
                 step : int,
                 iter : int) -> None:
        
        self.df = df
        self.class_column = class_column
        self.start_date = start_date
        self.geomfeat_column = geomfeat_columns
        
        self.start_date = start_date
        self.step = step
        self.iter = iter
        
        self.date_and_spectrum = extract_date_and_spectrum(df)
        self.all_spectrum = get_all_spectrum(self.date_and_spectrum)
        self.all_day = get_all_day(self.date_and_spectrum)
        
        
    def fit(self) -> np.array:
        for index in range(self.df.shape[0]):
            vector = list()
            for day in self.all_day:
                spectrum_vector = self._create_vector_spectrum()
                vector_day = self._make_vector_day(index, day, spectrum_vector)
                vector.append(vector_day)
                
            print(f"{index} --> saved ")
            vector = np.array(vector)
            path =  self.folder_path_data + f"/{index}.npy"
            np.save(path, vector)

        
        geomfeat = fix_geomfeat(self.df , self.geomfeat_column)
        label = fix_label(self.df, self.class_column)
        date = fix_date(self.start_date , self.step , self.iter)
        
        save_json(self.folder_path_META + "/geomfeat.json", geomfeat)
        print("geomfeat METADATA saved successfully")
        
        save_json(self.folder_path_META + "/label.json", label)
        print("label METADATA saved successfully")

        save_json(self.folder_path_META + "/date.json", date)
        print("date METADATA saved successfully")


        
                    
    def _make_vector_day(self, 
                        index : int,
                        day : int,
                        spectrum_vector : dict) -> np.array:
        
        for spectrum in spectrum_vector:
            if spectrum in self.date_and_spectrum[day]:
                column = f"{day}_{spectrum}"
                spectrum_vector[spectrum] = self.df.iloc[index][column]

        return np.array(
                        list(spectrum_vector.values())
                           )
                
    def _create_vector_spectrum(self) -> dict:
        return {spectrum: np.nan for spectrum in self.all_spectrum}

    

