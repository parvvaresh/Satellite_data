import pandas as pd 
from datetime import datetime, timedelta

from data.convert_data import convert_data


"""
    YOU SHOULD FILL THIS
"""

path = "" # path of excel file 
df = pd.read_excel(path)
model = convert_data(df = df, 
                 class_column = "", #class columns in data sets
                 geomfeat_columns = (), #geomfeat columns in data sets (enter name)
                 path = "", #path file for save folder 
                 start_date = datetime(2015, 4, 4), # enter year month and day for start date -> change it
                 step = 5, #this is a step of day for generate date
                 iter = 25) #how many data for generate data? 

model.fit()
