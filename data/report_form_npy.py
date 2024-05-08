import numpy as np
import os

from tool.get_all_npy import get_all_npy
from tool.save_json import save_json
from tool.generate_plot import plot_with_number,bar_plot
from tool.create_folder import create_folder
from tqdm import tqdm


class report_from_npy:
    def __init__(self,
                 path_folder_npy : str ) -> None:
        self.all_npy_file = get_all_npy(path_folder_npy)
        
        
     
        
    # report from number of pixle for each file
    def _extract_all_pixle(self) -> None:
        # step 1 -> initialize dictionary for save number of pixle for each file
        self.all_pixle_for_each_file = dict()
        
        # step 2 -> iterate in files and extract data
        for name , path in self.all_npy_file.items():
            pixle = np.load(path).shape[2]
            self.all_pixle_for_each_file.update({int(name) : pixle})
            
            
            # step 2.1 -> sort data based names
            self.all_pixle_for_each_file = dict(
                sorted(self.all_pixle_for_each_file.items(), key=lambda item:item[0])
            )
    
    def report_pixle_for_each_file(self,
                                   path_to_save : str) -> None:
        # step 1 -> extract number of pixle for each file
        self._extract_all_pixle()
        
        # step 2 -> create file 
        path_to_save += "/report_pixle_for_each_file"
        create_folder(path_to_save)
        
        # step 3.1 -> save json file
        path_to_save_json = path_to_save + "/pixle_for_each_file.json"
        save_json(path_to_save_json,
                 self.all_pixle_for_each_file)
        print(f"saved json file in this directory -> {path_to_save}")
        
        
        # step 3.2 -> save plot
        path_to_save_plot = path_to_save + "/pixle_for_each_file.png"
        plot_with_number(self.all_pixle_for_each_file,
             path_to_save_plot)
        print(f"saved png file in this directory -> {path_to_save}")    
    
    
    
    
    def _extract_pixle_each_spectrum(self,
                                     path  : str,
                                     index_pixle : int) -> dict:
        # step 1 -> open file and extract number of day | spectrum
        data = np.load(path)
        number_day , number_spectrum = data.shape[0] , data.shape[1]
        
        # step 1.1  initialize dictionary for data 
        result = dict()
        
        # step 2 -> extract espcial spectrum from data
        for index_spectrum in range(number_spectrum):
            # step 2 -> initialize dictionary for save data's for each spectrum
            temp = dict()
            
            for index_day in range(number_day):
                temp[index_day + 1] = data[index_day][index_spectrum][index_pixle]
            result[index_spectrum + 1] = temp
        
        return result    
    
    def report_pixle_for_each_spectrum(self, 
                                       index_pixle : int,
                                       path_to_save : str) -> None :
        
        path_to_save += "/report_pixle_for_each_spectrum"
        create_folder(path_to_save)
        
        # step 1 -> extract data froam each file
        path_to_save_json = path_to_save + "/json_data"
        path_to_save_plot = path_to_save + "/plot"
        
        create_folder(path_to_save_json)
        create_folder(path_to_save_plot)

        for name , path in tqdm(self.all_npy_file.items()):
            data = self._extract_pixle_each_spectrum(path, index_pixle) 
                       
            # step 2.1 - > save json file 
            path_json = path_to_save_json + f"/{name}.json"
            save_json(path_json,
                data)
            #print(f"saved json {name} file in this directory -> {path_to_save_json}")
            
            # step 2.2 save plot in folder
            path_plot = path_to_save_plot + f"/{name}"
            create_folder(path_plot)

            for index , _data in data.items():
                path_temp = path_plot + f"/{index}.png"
                bar_plot(_data, path_temp)           
                #print(f"saved plot {name} file in this directory -> {path_temp}")
            
        
                
            