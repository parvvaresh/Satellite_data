import numpy as np
import pandas as pd


from utils import (
    create_folder,
    point_plot,
    bands_per_date,
    bands_count_per_date,
    save_json,
    point_plot,
    get_all_bands,
    get_all_dates,
    counter_class)


class report_csv:
    def __init__(self, 
                 df : pd.DataFrame,
                 class_column : str) -> pd.DataFrame:
        self.df = df
        self.class_column = class_column
    

    def pipline_save(self,
                     path_to_save : str) -> None:
        
        path_root = path_to_save + "/report_csv"
        create_folder(path_root)

        bands_per_date_data = bands_per_date(self.df)

        save_json(bands_per_date_data, path_root + "/bands_per_date.json")

        bands_count_per_date_date = bands_count_per_date(bands_per_date_data)
        point_plot(bands_count_per_date_date,  "bands" , "number of bands" , "number of bands per date",path_root + "/bands_per_date.png", False)


        total_information = {
            "number of all bands" : len(get_all_bands(bands_per_date_data)),
            "number of all dates" : len(get_all_dates(bands_per_date_data)),
            "list of bands in all date" : list(set(bands_per_date_data)),

        }
        save_json(total_information, path_root + "/total_information.json")
        print("pkey")

        class_count = counter_class(self.df, self.class_column)
        save_json(class_count, path_root + "/class_count.json")

