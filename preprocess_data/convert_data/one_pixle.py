import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import sys


current_dir = os.path.dirname(os.path.realpath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../.."))
sys.path.append(project_root)


from convert_data import convert_data
from preprocess_data.util import extract_date_and_spectrum
from preprocess_data.util import get_all_spectrum
from preprocess_data.util import save_json
from preprocess_data.util import create_folder
from preprocess_data.util import fix_date
from preprocess_data.util import fix_geomfeat
from preprocess_data.util import fix_label
from preprocess_data.util import track_sentinel
from preprocess_data.util import saved_meta_data



        
        



class convert_csv_to_npy(convert_data):
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 geomfeat_columns : tuple,
                 path_to_save : str,
                 start_date : datetime,
                 step : int,
                 iter : int) -> None:
        
        super().__init__(df, 
                       class_column ,
                       geomfeat_columns,
                       path_to_save,
                       start_date, 
                       step,
                       iter)
        
        

                
        
         
    def get_npy_for_all_sentinel(self) -> None:
        
        """
        
            Here, regardless of which satellite the data is for, 
            pre-processing the data and converting it to .npy format data
            
            
            We explain this step by step 

        """
        
        
        #----------------------------------------------------------------
        
        """
            step 0 -> 
                        create folder for save .npy format and meta data
        """
        root_path = self.path_to_save + "/npy_data_for_all_sentinel"
        create_folder(root_path)

        """
            step 0.1 -> 
                        create folder for save .npy format especially
        """       
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        """
            step 1 -> 
                        iteration in dataframe for get row(point)
                        Each line of the dataframe represents a point 
                        with characteristics related to spectrum and date
        """
        for index in tqdm(range(self.df.shape[0])):
            """
                step 2 -> 
                            start convert each point to .npy format file
                            For this, we create an empty list to store our data
            """
            vector = list()     
                   
            """
                step 2.1 -> 
                            We have one-pixel data in .npy format
                            (24, 10)
                            This means that we have 24 dates or periods of history and each date point has 10 spectrums
                            This is how the data should be
                
            """
            for date in self.all_date_point:
                
                """
                    step 3 -> 
                            We know that every date includes taking spectrum characteristics, so we have to move on the date from the beginning

                            We create a vector of spectra for each date and set its default value to np.nan
                            And then we will see if the desired spectrum is available on that date
                            If there is, we replace its value, and if not, the default value remains
                            Then, for each date, the obtained vector is stored in the same vector that we created at the beginning
                            
                """
                spectrum_vector = self._create_vector_spectrums(self.all_spectrums)
                vector_day = self._make_vector_for_each_date(index, date, spectrum_vector, self.date_and_spectrum)
                vector.append(vector_day)
            
            
            
            """
                step 4 ->
                        We save the extracted data in the presentation format of the pie name and save the index that is in the data frame.
                        For example : 
                                    1.npy
                                    The first row is in our data frame
                        
            """            
            vector = np.array(vector)
            path =  self.npy_folder + f"/{index}.npy"
            np.save(path, vector)
        print("---> saved npy files in path")



        
        meta_folder = root_path + "/META"
        create_folder(meta_folder)
        
        
        
        """
            step 5 -> 
                    Metadata includes the information included : 
                                                                1. lable
                                                                2. Longitude latitude
                                                                3. **Time

                    ** The meaning of time means the first presentation in the NPY file, exactly what date does it mean
        """
        
        saved_meta_data(meta_folder,
                        self.df, 
                        self.geomfeat_column,
                        self.class_column,
                        self.start_date,
                        self.step ,
                        self.iter)
        print("---> saved meta data")


    
    def get_npy_track_sentinel(self) -> None:
        
        """
                Here, we pay attention to which satellite each spectrum is for and separate them, 
                pre-process the data and convert it to npy format data.
                We explain this step by step

        """
        

        #----------------------------------------------------------------
        
        """
            step 0 -> 
                        create folder for save .npy format and meta data
        """
        
        root_path = self.path_to_save + "/npy_data_for_track_sentinel"
        create_folder(root_path)
        
        """
            step 0.1 -> 
                        create folder for save .npy format especially
        """      
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        
        """
            step 1 ->
                The spectra of each satellite are separated
                Combined spectra that do not belong to any satellite are stored in a separate file under the name of combined
         
        """
        
        track_sentinel_date_and_spectrum = track_sentinel(self.date_and_spectrum)
        
        
        
        """
            step 2 ->     
                    Here we see what spectrum each date has for each satellite

        """
        s1_date_and_spectrum = track_sentinel_date_and_spectrum["s1"]
        s2_date_and_spectrum = track_sentinel_date_and_spectrum["s2"]
        combintion_date_and_spectrum = track_sentinel_date_and_spectrum["combination"]
        
        """
            step 3 -> 
                        For each satellite, we extract all its spectra
        """
        s1_all_spectrum  = get_all_spectrum(track_sentinel_date_and_spectrum["s1"])
        s2_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["s2"])
        combintion_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["combination"])
        

        """
            step 4 -> 
                        iteration in dataframe for get row(point)
                        Each line of the dataframe represents a point 
                        with characteristics related to spectrum and date
        """
        
        for index in tqdm(range(self.df.shape[0])):


            """
                    
                step 4.1 -> 
                            We have one-pixel data in .npy format
                            (24, 10)
                            This means that we have 24 dates or periods of history and each date point has 10 spectrums
                            This is how the data should be
                            We do this for each satellite 
                
        
            """
            s1 = self._create_vector_for_espcial_sentinel(
                s1_all_spectrum,
                s1_date_and_spectrum,
                index
                
            )

            s2 = self._create_vector_for_espcial_sentinel(
                s2_all_spectrum,
                s2_date_and_spectrum,
                index
                
            )

            combintion = self._create_vector_for_espcial_sentinel(
                combintion_all_spectrum,
                combintion_date_and_spectrum,
                index
                
            )
            
            
            """
                step 5 ->
                        We save the extracted data in the presentation format of the pie name and save the index that is in the data frame.
                        For example : 
                                    1.npy
                                    The first row is in our data frame
                                    We do this for each satellite 
            """   
            
            file_path_index = npy_folder + f"/{index}"
            create_folder(file_path_index)
            
            path =  file_path_index + f"/s1.npy"
            np.save(path, s1)
        
            path =  file_path_index + f"/s2.npy"
            np.save(path, s2)

            path =  file_path_index + f"/combintion.npy"
            np.save(path, combintion)
        
            
        print("saved npy files in path")
    
    
        """
            step 5 -> 
                    Metadata includes the information included : 
                                                                1. lable
                                                                2. Longitude latitude
                                                                3. **Time

                    ** The meaning of time means the first presentation in the NPY file, exactly what date does it mean
        """
    
        meta_folder = root_path + "/META"
        create_folder(meta_folder)
        
        saved_meta_data(meta_folder,
                        self.df, 
                        self.geomfeat_columns,
                        self.class_column,
                        self.start_date,
                        self.step ,
                        self.iter)        
        
        





    def _create_vector_for_espcial_sentinel(self,
                                            all_spectrums : list,
                                            date_and_spectrums : dict,
                                            index : int) -> np.array:
        
        
    
        """
                    We know that every date includes taking spectrum characteristics, so we have to move on the date from the beginning

                    We create a vector of spectra for each date and set its default value to np.nan
                    And then we will see if the desired spectrum is available on that date
                    If there is, we replace its value, and if not, the default value remains
                    Then, for each date, the obtained vector is stored in the same vector that we created at the beginning
                            
        """
        
        
        vector = list()
        for date in self.all_date_point:
            spectrum_vector = self._create_vector_spectrums(all_spectrums)
            vector_day = self._make_vector_for_each_date(index, date, spectrum_vector, date_and_spectrums)
            vector.append(vector_day)
            
        vector = np.array(vector)
        return vector
        
                    
    def _make_vector_for_each_date(self, 
                        index : int,
                        date : int,
                        spectrum_vector : dict,
                        date_and_spectrum : dict) -> np.array:

        
        
        """
            First, we can see that on the specified date, if it is the range we want, 
            it will reconstruct the column according to the format and save its information.
        """
        
        for spectrum in spectrum_vector:
            if spectrum in date_and_spectrum[date]:
                column = f"{date}_{spectrum}"
                spectrum_vector[spectrum] = self.df.iloc[index][column]

        return np.array(
                        list(spectrum_vector.values())
                           )
                
    def _create_vector_spectrums(self, 
                                 spectrums : list) -> dict:
        """
        
         in this function we have to create a vector for spectrum and defeault nan  values
         
        """
        return {spectrum: np.nan 
                    for spectrum in spectrums}