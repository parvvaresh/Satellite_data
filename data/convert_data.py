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
        self.start_date = start_date
        self.geomfeat_column = geomfeat_columns
        
        self.start_date = start_date
        self.step = step
        self.iter = iter

        
        
        #get data from csv file
        self.date_and_spectrum = extract_date_and_spectrum(df)
        self.all_spectrcombination_spectrumsums = get_all_spectrum(self.date_and_spectrum)
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
        self.root_path = self.path_to_save + "/npy_data_for_all_sentinel"
        create_folder(self.root_path)
        
        self.npy_folder = self.root_path + "/DATA"
        create_folder(self.npy_folder)
        
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
                spectrum_vector = self._create_vector_spectrums()
                # step 2.3 -> make creating vecotrs for each date
                vector_day = self._make_vector_for_each_date(index, date, spectrum_vector)
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
        
        self.meta_folder = self.root_path + "/META"
        create_folder(self.meta_folder)
        
        # step 7 -> extract data from dataframe
        
        geomfeat = fix_geomfeat(self.df , self.geomfeat_column)
        label = fix_label(self.df, self.class_column)
        date = fix_date(self.start_date , self.step , self.iter)
        
        
        # step 8 -> save it
        save_json(self.meta_folder + "/geomfeat.json", geomfeat)
        print("geomfeat METADATA saved successfully")
        
        save_json(self.meta_folder + "/label.json", label)
        print("label METADATA saved successfully")

        save_json(self.meta_folder + "/date.json", date)
        print("date METADATA saved successfully")
    
    
    def get_npy_track_sentinel(self) -> None:
        # step 1 -> track spectrum 
        """
        
         s2 = (b1 , b2 , b3 , b4 , b5 , b6 , b7) and more
         s1 = (vv, vh , hv, hh)
         A combination of one and two
         
        """


        
                    
    def _make_vector_for_each_date(self, 
                        index : int,
                        date : int,
                        spectrum_vector : dict) -> np.array:
        
        
        """
            in this function we create a vector for each date
            frist we collect all spectrum for each date
            step 1 -> we check spectrum in this daye is existing?
            if existing generation columns and extract data nd save it 
        """
        
        for spectrum in spectrum_vector:
            if spectrum in self.date_and_spectrum[date]:
                column = f"{date}_{spectrum}"
                spectrum_vector[spectrum] = self.df.iloc[index][column]

        return np.array(
                        list(spectrum_vector.values())
                           )
                
    def _create_vector_spectrums(self) -> dict:
        """
        
         in this function we have to create a vector for spectrum and defeault nan  values
         
        """
        return {spectrum: np.nan 
                    for spectrum in self.all_spectrums}
    
    
    

    


df = pd.read_excel("/home/reza/Desktop/BAHAR_DATA_S1_S2_0301_0630.xlsx")

m = convert_csv_to_npy_pixle(df = df, 
                 class_column = "Name",
                 geomfeat_columns = ("X", "Y"),
                 path_to_save = "/home/reza/Desktop",
                 start_date = datetime(2012, 2, 2),
                 step = 3,
                 iter = 20)

print(track_sentinel(m.date_and_spectrum))


