"""
    This is a converted production line that performs a series of tasks in sequence
"""


import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm

from util import create_folder


def pipeline(df :pd.DataFrame,
             class_column : str,
             geo_column : str,
             strar_date : datetime,
             step : int,
             path_to_save) -> None:
    
    """
    
        step 1 -> Create a folder for storage
    
    """
    create_folder(path_to_save)
    
    
    """
        step 2-> 
    """