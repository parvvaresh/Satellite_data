import os
from datetime import datetime

from gridTif_extractData.process_tif import run_process_tif
from to_npy.convert_pipline import ConvertData

def pipeline(paht_tifs : str , 
             path_save_tif : str, 
             path_save_csv : str, 
             size_crop_block : int, 
             size_crop_pixle : int,
             geoJSON : dict,
             class_column : str,
             latlong_columns : list,
             block_column : str, 
             start_date : datetime, 
             finish_date : datetime, 
             interval : int, 
             fillna_method_value : list):
    # grid tifs
    run_process_tif(
                paht_tifs="/home/reza/test_tif",
                path_save_tif="/home/reza/tifs_garbage/tifs",
                path_save_csv="/home/reza/csv",
                size_crop_terrain=9,
                size_crop_pixle=1)
    
    




pipeline(paht_tifs = "/home/reza/test_fif" , 
        path_save_tif ="/home/reza/tifs_garbage/num_", 
        path_save_csv ="/hpme/reza/csv", 
        size_crop_block = 9, 
        size_crop_pixle = 1,
        geoJSON = "gwo",
        class_column = "class",
        latlong_columns = ["X, Y"],
        block_column = "unique_block_id", 
        start_date = datetime(2022, 11, 1),
        finish_date = datetime(2023, 11, 30),
        interval = 7, 
        fillna_method_value = ["input" , -1])