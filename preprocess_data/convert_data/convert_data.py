import pandas as pd
from datetime import datetime, timedelta


class _convert_data:
    def __init__(self,
                 df : pd.DataFrame, 
                 class_column : str,
                 geomfeat_columns : tuple,
                 path_to_save : str,
                 start_date : datetime,
                 step : int,
                 iter : int) -> None:
        
        """
            Here our variables are initialized
                1. Data frame
                2. Class column
                3. Data storage location
                4. Start date for metadata storage
                5. The step of skipping data is, for example, 5 (it means data every 5 days)
                6. and repetition

        """
        
        
        
        self.df = df
        self.class_column = class_column
        self.path_to_save = path_to_save
        self.geomfeat_columns = geomfeat_columns
        self.start_date = start_date
        self.step = step
        self.iter = iter