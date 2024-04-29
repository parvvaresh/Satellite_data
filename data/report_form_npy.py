import numpy as np

from tool.get_all_npy import get_all_npy
from tool.save_json import save_json
from tool.generate_plot import plot_with_number,bar_plot
from tool.create_folder import create_folder_for_report_npy
from tool.create_folder import create_folder_for_report_npy_same_class


class report_from_npy:
    def __init__(self,
                 path_folder_npy : str) -> None:
        self.all_npy_files = get_all_npy(path_folder_npy)
        self._extract_all_pixle()
        self.pixle_each_spectrum = self._extract_pixle_each_spectrum()
    
    def _extract_all_pixle(self) -> None:
        self.all_pixle_for_each_file = dict()
        
        for name ,file in self.all_npy_files.items():
            pixle = np.load(file).shape[2]
            self.all_pixle_for_each_file.update({name : pixle})
        
        self.all_pixle_for_each_file = dict(
            sorted(self.all_pixle_for_each_file.items(), key=lambda item:item[0])
        )
    
    
    def save_pixle_for_each_file(self,
                                 path_to_save) -> None:
        path_to_save += "/pixle_for_each_file.json"
        save_json(path_to_save,
                 self.all_pixle_for_each_file)
        print(f"saved json file in this directory -> {path_to_save}")
    

    def plot_pixle_for_each_file(self,
                                 path_to_save) -> None:
        path_to_save += "/pixle_for_each_file.png"
        plot_with_number(self.all_pixle_for_each_file,
             path_to_save)
        print(f"saved png file in this directory -> {path_to_save}")
    
    
    def _extract_pixle_each_spectrum(self) -> dict:
        result_all = {}
        index_pixle = 4

        for name, file in self.all_npy_files.items():
            data = np.load(file)
            
            day, spectrum = data.shape[0] , data.shape[1]
            result = {}
            for _spectrum in range(spectrum):
                _result = dict()
                for _day in range(day):
                    _result[_day + 1] = data[_day][_spectrum][index_pixle]
                result[_spectrum] = _result
            result_all[name] = result
        return result_all
    
    def save_json_pixle_each_spectrum(self,
                                      path_to_save : str) -> None:
        path_to_save += "/pixle_for_each_file.json"
        save_json(path_to_save,
                 self.pixle_each_spectrum)
        print(f"saved json file in this directory -> {path_to_save}")
    
    def plot_pixle_each_spectrum(self, 
                                      path_to_save : str) -> None:
        folders_path = create_folder_for_report_npy(path_to_save, len(self.all_npy_files))
        for index , path in folders_path.items():
            data = self.pixle_each_spectrum[index]
            for index_spectrum in range(len(data)):
                _data = data[index_spectrum]
                print(_data.values())
                bar_plot(_data, path + f"/{index_spectrum + 1}.png")            

    
    def plot_pixle_each_spectrum_for_same_class(self,
                                                lables : dict,
                                                path_to_save) -> None:
        same_class = {}
        for  file, _class in lables.items():
            if _class in same_class:
                same_class[_class].append(file)
            else:
                same_class[_class] = []
                same_class[_class].append(file)
        
        pixles = self._extract_pixle_each_spectrum()
        
        folders_path = create_folder_for_report_npy(path_to_save, list(same_class.keys()))
        for index , path in folders_path.items():
            data = self.pixle_each_spectrum[index]
            for index_spectrum in range(len(data)):
                _data = data[index_spectrum]
                print(_data.values())
                bar_plot(_data, path + f"/{index_spectrum + 1}.png")     
        
        
    
import json
with open('/home/reza/Desktop/META/labels.json') as f:
    data = json.load(f)

classes = (dict(data["label_44class"]))

model = report_from_npy("/home/reza/Desktop/DATA")
model.plot_pixle_each_spectrum("/home/reza/Desktop")