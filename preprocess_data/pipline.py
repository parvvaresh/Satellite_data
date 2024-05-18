"""
    This is a converted production line that performs a series of tasks in sequence
"""


import numpy as np
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

from util import create_folder
from util import extract_date_and_spectrum
from util import get_all_spectrum
from util import save_json
from util import create_folder
from util import fix_date
from util import fix_geomfeat
from util import fix_label
from util import track_sentinel
from util import saved_meta_data
from util import get_all_date
from util import point_plot
from util import counter_class



def pipeline_one_pixle(df :pd.DataFrame,
             class_column : str,
             path_to_save : str,
             geo_columns : tuple,
             start_date : datetime,
             iter : int,
             step : int) -> None:
    
    path_to_save += "/satelilte_data"
    """
    
        step 1 -> Create a folder for storage
    
    """
    create_folder(path_to_save)
    
    
    """
        step 2-> save report of number of spectru in each date
    """

    """
        step 2.1 -> create foder and save it
    """
    date_and_spectrum = extract_date_and_spectrum(df)
    path = path_to_save + "/Distribution_spectra"
    create_folder(path)
    
    
    
    """
        step 2.2 -> save a json file
    """
    save_json(path + "/distribution.json", date_and_spectrum)    
    
    """
        step 2.3 -> save a bar plot
    """
    all_spectrum  = get_all_spectrum(date_and_spectrum)
    distribution = {}
    for date , spectrum in date_and_spectrum.items():
        distribution[str(date)] = len(all_spectrum) - len(spectrum )
        
    point_plot(distribution   ,
               path + "/distribution.png",
               "point of date",
               "distribution of spectrum" ,
               "distribution of spectrum for dates")
    
    del path
    
    print("---> save json and plot distribution spectrum")
    
    """
        step 3-> save number of each class 
    """
    data = counter_class(df , class_column)
    path = path_to_save + "/Distribution_class"
    create_folder(path)
    
    """
        step 3.1 -> save a json file
    """
    save_json(path + "/distribution.json", data)
    
    
    """
        step 3.3 -> save a bar plot
    """

    point_plot(data   ,
               path + "/distribution.png",
               "point of date",
               "distribution of class" ,
               "distribution of class for dates")
    
    print("---> save json and plot distribution class")


    from convert_data.one_pixle import  convert_csv_to_npy

    """
        step 4 -> conver data
    """
    path = path_to_save + "/converted_data"
    create_folder(path)
    model = convert_csv_to_npy(df , 
                 class_column,
                 geo_columns,
                 path,
                 start_date ,
                 step,
                 iter)   
    
    model.get_npy_for_all_sentinel()
    print("---> save .npy format files for all sentinel")

    model.get_npy_track_sentinel()
    print("---> save .npy format files for special sentinel")



def pipeline_block_pixle(df :pd.DataFrame,
             class_column : str,
             path_to_save : str,
             block_column : str,
             index_pixle_visual : str,
             geo_columns : tuple,
             start_date : datetime,
             iter : int,
             step : int) -> None:
    
    path_to_save += "/satelilte_data_block_pixle"
    """
    
        step 1 -> Create a folder for storage
    
    """
    create_folder(path_to_save)
    
    
    """
        step 2-> save report of number of spectru in each date
    """

    """
        step 2.1 -> create foder and save it
    """
    date_and_spectrum = extract_date_and_spectrum(df)
    path = path_to_save + "/Distribution_spectra"
    create_folder(path)
    
    
    
    """
        step 2.2 -> save a json file
    """
    save_json(path + "/distribution.json", date_and_spectrum)    
    
    """
        step 2.3 -> save a bar plot
    """
    all_spectrum  = get_all_spectrum(date_and_spectrum)
    distribution = {}
    for date , spectrum in date_and_spectrum.items():
        distribution[str(date)] = len(all_spectrum) - len(spectrum )
        
    point_plot(distribution   ,
               path + "/distribution.png",
               "point of date",
               "distribution of spectrum" ,
               "distribution of spectrum for dates")
    
    del path
    
    print("---> save json and plot distribution spectrum")
    
    """
        step 3-> save number of each class 
    """
    data = counter_class(df , class_column)
    path = path_to_save + "/Distribution_class"
    create_folder(path)
    
    """
        step 3.1 -> save a json file
    """
    save_json(path + "/distribution.json", data)
    
    
    """
        step 3.3 -> save a bar plot
    """

    point_plot(data   ,
               path + "/distribution.png",
               "point of date",
               "distribution of class" ,
               "distribution of class for dates")
    
    print("---> save json and plot distribution class")


    from convert_data.block_pixle import  convert_csv_to_npy as block_pixle

    """
        step 4 -> conver data
    """
    path = path_to_save + "/converted_data"
    create_folder(path)
    model = block_pixle(df , 
                 class_column,
                 geo_columns,
                 block_column,
                 path,
                 start_date ,
                 step,
                 iter)   
    
                 
    
    #model.get_npy_for_all_sentinel()
    print("---> save .npy format files for all sentinel")

    model.get_npy_track_sentinel()
    print("---> save .npy format files for special sentinel")
    
# #df =pd.read_excel("/home/reza/Desktop/BAHAR_DATA_S1_S2_0301_0630.xlsx")


# pipeline_one_pixle(df,"Name" ,"/home/reza/Desktop", ("X", "Y"), datetime(2015, 4, 4), 15, 5)



    
df =pd.read_excel("/home/reza/Desktop/USA_DATA_3.xlsx")


pipeline_block_pixle(df,"FID_BLOCK_" ,"/home/reza/Desktop" , "FID_BLOCK_", 5,  ("X", "Y"), datetime(2015, 4, 4), 15, 5)