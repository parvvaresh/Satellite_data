import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm


from util import extract_date_and_spectrum
from util import get_all_spectrum
from util import get_all_date
from util import save_json
from util import create_folder
from util import fix_date
from util import fix_geomfeat
from util import fix_label
from util import track_sentinel
from util import saved_meta_data


class convert_data:
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 geomfeat_columns : tuple,
                 path_to_save : str,
                 start_date : datetime,
                 step : int,
                 iter : int) -> None:
        
        #intialize pharameter
        
        self.df = df
        self.class_column = class_column
        self.path_to_save = path_to_save
        self.start_date = start_date
        self.geomfeat_columns = geomfeat_columns
        self.start_date = start_date
        self.step = step
        self.iter = iter

        
        
        #get data from csv file
        self.date_and_spectrum = extract_date_and_spectrum(df)
        self.all_spectrums = get_all_spectrum(self.date_and_spectrum)
        self.all_date_point = get_all_date(self.date_and_spectrum)


class convert_csv_to_npy_pixle(convert_data):
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
        
        # step 0 -> create folder for save .npy format
        root_path = self.path_to_save + "/npy_data_for_all_sentinel"
        create_folder(root_path)
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        # step 1 -> iteration in dataframe for get row(point)
        for index in tqdm(range(self.df.shape[0])):
            # step 2 -> start convert each point to .npy format file
            vector = list()
            
            # step 2.1
            """
            
                in one pixle data we have data 
                and shape it is : (24, 10) 
                means we have 24 dates and for each dates have spectrum
                and we shoud itereations in dates and save data for each date
                
            """
            for date in self.all_date_point:
                # step 2.2 -> creats vecotrs for spectrum 
                spectrum_vector = self._create_vector_spectrums(self.all_spectrums)
                # step 2.3 -> make creating vecotrs for each date
                vector_day = self._make_vector_for_each_date(index, date, spectrum_vector, self.date_and_spectrum)
                # step 2.4 -> save data in list for each date
                vector.append(vector_day)
            
            # step 3 -> conver data to np.array type
            vector = np.array(vector)
            
            path =  self.npy_folder + f"/{index}.npy"
            # step 4 -> save it path 
            np.save(path, vector)
        
        print("saved npy files in path")


        # step 5 -> save META data
        # step 6 -> create folder for save META data
        
        meta_folder = root_path + "/META"
        create_folder(meta_folder)
        
        # step 7 -> extract data from dataframe
        saved_meta_data(meta_folder,
                        self.df, 
                        self.geomfeat_column,
                        self.class_column,
                        self.start_date,
                        self.step ,
                        self.iter)
    
    
    def get_npy_track_sentinel(self) -> None:
        
        # step 0 : make folder for save it 
        
        root_path = "/home/reza/Desktop" + "/npy_data_for_track_sentinel"
        create_folder(root_path)

        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        # step 1 -> track spectrum (sentinel 1 and sentinel 2)
        
        """
        
         s2 = (b1 , b2 , b3 , b4 , b5 , b6 , b7) and more
         s1 = (vv, vh , hv, hh)
         A combination of one and two
         
        """
        
        track_sentinel_date_and_spectrum = track_sentinel(self.date_and_spectrum)
        
        # step 2 -> get date and spectrum for each sentinel
        s1_date_and_spectrum = track_sentinel_date_and_spectrum["s1"]
        s2_date_and_spectrum = track_sentinel_date_and_spectrum["s2"]
        combintion_date_and_spectrum = track_sentinel_date_and_spectrum["combination"]
        
        # step 3 -> get all spectrum for each sentinel 
        s1_all_spectrum  = get_all_spectrum(track_sentinel_date_and_spectrum["s1"])
        s2_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["s2"])
        combintion_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["combination"])
        
        # step 3 -> iteration in dataframe for get row(point)
        for index in tqdm(range(self.df.shape[0])):
            # step 4 -> start convert each point to .npy format file

            
            # step 5.1
            """
            
                in one pixle data we have data 
                and shape it is : (24, 10) 
                means we have 24 dates and for each dates have spectrum
                and we shoud itereations in dates and save data for each date
                
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
            
            file_path_index = npy_folder + f"/{index}"
            create_folder(file_path_index)
            
            path =  file_path_index + f"/s1.npy"
            np.save(path, s1)
        
            path =  file_path_index + f"/s2.npy"
            np.save(path, s2)

            path =  file_path_index + f"/combintion.npy"
            np.save(path, combintion)
        
            
        print("saved npy files in path")
    
    
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
        
        
        vector = list()
        for date in self.all_date_point:
            # step 1 -> creats vecotrs for spectrum 
            spectrum_vector = self._create_vector_spectrums(all_spectrums)
            # step 2 -> make creating vecotrs for each date
            vector_day = self._make_vector_for_each_date(index, date, spectrum_vector, date_and_spectrums)
            # step 3 -> save data in list for each date
            vector.append(vector_day)
            
        # step 4 -> conver data to np.array type
        vector = np.array(vector)
        return vector
        
                    
    def _make_vector_for_each_date(self, 
                        index : int,
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
                spectrum_vector[spectrum] = self.df.iloc[index][column]

        return np.array(
                        list(spectrum_vector.values())
                           )
                
    def _create_vector_spectrums(self, spectrums : list) -> dict:
        """
        
         in this function we have to create a vector for spectrum and defeault nan  values
         
        """
        return {spectrum: np.nan 
                    for spectrum in spectrums}
    
    
    

    
class convert_csv_to_npy_block_pixle(convert_data):
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 geomfeat_columns : tuple,
                 pixle_block_columns : str,
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
        
        
        self.pixle_block_columns = pixle_block_columns
    
    def get_npy_for_all_sentinel(self) -> None:
        

        # step 0 -> create folder for save .npy format
        root_path = self.path_to_save + "/npy_data_for_all_sentinel_block_pixle"
        create_folder(root_path)
        
        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        block_pixles = self.df.groupby(self.pixle_block_columns)
        
        
        # step 0.1 -> get all day | get spectrum | get date and spectrums
        
        
        date_and_spectrum = extract_date_and_spectrum(self.df)
        all_spectrums = get_all_spectrum(date_and_spectrum)
        all_date_point = get_all_date(date_and_spectrum)
        
        
        
        # step 1 -> iteration in block pixles
        for name, df_group in tqdm(block_pixles):
            # step 2 -> make a vector for each block
            vector = list()

            for date in all_date_point:
                # step 2.2 -> creats vecotrs for spectrum 
                spectrum_vector = self._create_vector_spectrums(all_spectrums , df_group.shape[0]) # this is a lenght of vector 
                # step 2.3 -> make creating vecotrs for each date
                vector_day = self._make_vector_for_each_date(df_group, date, spectrum_vector, date_and_spectrum)
                # step 2.4 -> save data in list for each date
                vector.append(vector_day)
            
            # step 3 -> conver data to np.array type
            vector = np.array(vector)
            
            path =  npy_folder + f"/{name}.npy"
            # step 4 -> save it path 
            np.save(path, vector)
        
        print("saved npy files in path")
            
        
        # step 5 -> save META data
        # step 6 -> create folder for save META data
        
        meta_folder = root_path + "/META"
        create_folder(meta_folder)
        
        # step 7 -> extract data from dataframe
        saved_meta_data(meta_folder,
                        self.df, 
                        self.geomfeat_column,
                        self.class_column,
                        self.start_date,
                        self.step ,
                        self.iter)
        
        

  
    def get_npy_track_sentinel(self) -> None:
        
        # step 0 : make folder for save it 
        
        root_path = self.path_to_save + "/npy_data_for_track_sentinel_block_pixle"
        create_folder(root_path)

        npy_folder = root_path + "/DATA"
        create_folder(npy_folder)
        
        
        
        
        # step 0.1 -> group by pixle block column's
        block_pixles = self.df.groupby(self.pixle_block_columns)
        
        
        # step 0.2 -> get all day | get spectrum | get date and spectrums
        
        
        date_and_spectrum = extract_date_and_spectrum(self.df)
        all_spectrums = get_all_spectrum(date_and_spectrum)
        all_date_point = get_all_date(date_and_spectrum)
        
        # step 1 -> track spectrum (sentinel 1 and sentinel 2)
        
        """
        
         s2 = (b1 , b2 , b3 , b4 , b5 , b6 , b7) and more
         s1 = (vv, vh , hv, hh)
         A combination of one and two
         
        """
        
        track_sentinel_date_and_spectrum = track_sentinel(date_and_spectrum)
        
        # step 2 -> get date and spectrum for each sentinel
        s1_date_and_spectrum = track_sentinel_date_and_spectrum["s1"]
        s2_date_and_spectrum = track_sentinel_date_and_spectrum["s2"]
        combintion_date_and_spectrum = track_sentinel_date_and_spectrum["combination"]
        
        # step 3 -> get all spectrum for each sentinel 
        s1_all_spectrum  = get_all_spectrum(track_sentinel_date_and_spectrum["s1"])
        s2_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["s2"])
        combintion_all_spectrum = get_all_spectrum(track_sentinel_date_and_spectrum["combination"])
        
        
        # step 4 -> iteration in group of block pixle
        for name, df_group in tqdm(block_pixles):

            s1 = self._create_vector_for_espcial_sentinel(
                df_group,
                all_date_point,
                s1_all_spectrum,
                s1_date_and_spectrum,
                
                
            )

            s2 = self._create_vector_for_espcial_sentinel(
                df_group,
                all_date_point,
                s2_all_spectrum,
                s2_date_and_spectrum,
                
                
            )

            combintion = self._create_vector_for_espcial_sentinel(
                df_group,
                all_date_point,
                combintion_all_spectrum,
                combintion_date_and_spectrum,
                
                
            )
            
            file_path_index = npy_folder + f"/{name}"
            create_folder(file_path_index)
            
            path =  file_path_index + f"/s1.npy"
            np.save(path, s1)
        
            path =  file_path_index + f"/s2.npy"
            np.save(path, s2)

            path =  file_path_index + f"/combintion.npy"
            np.save(path, combintion)
        
            
        print("saved npy files in path")

        
    def _create_vector_for_espcial_sentinel(self,
                                            df : pd.DataFrame,
                                            all_date_point : list,
                                            all_spectrums : list,
                                            date_and_spectrums : dict,) -> np.array:
        
        
        vector = list()
        for date in all_date_point:
            # step 1 -> creats vecotrs for spectrum 
            spectrum_vector = self._create_vector_spectrums(all_spectrums, df.shape[0])
            # step 2 -> make creating vecotrs for each date
            vector_day = self._make_vector_for_each_date(df ,date, spectrum_vector, date_and_spectrums)
            # step 3 -> save data in list for each date
            vector.append(vector_day)
            
        # step 4 -> conver data to np.array type
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
        
        
        
path = "/home/reza/Desktop" + "/USA_DATA_3.xlsx"


df = pd.read_excel(path)

m = convert_csv_to_npy_block_pixle(df = df, 
                 class_column = "gridcode",
                 geomfeat_columns = ("X", "Y"),
                 pixle_block_columns = "FID_BLOCK_",
                 path_to_save = "/home/reza/Desktop",
                 start_date = datetime(2012, 2, 2),
                 step = 3,
                 iter = 20)

m.get_npy_track_sentinel()


