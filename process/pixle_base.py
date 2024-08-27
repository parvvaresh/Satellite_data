import os
import pandas as pd
import glob
from tqdm import tqdm
import json
import os
import shutil
from all import csv_to_npy_all
from process.to_npy import csv_to_npy_split


import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/reza/Desktop/Satellite_data')))


from process.utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class, fillna
from utils import * 














        

ellam = get_csv("/home/reza/Desktop/csv_temp_ellam")
hamedan = get_csv("/home/reza/Desktop/csv_temp_ham")


all_csv = ellam + hamedan
all_columns = get_columns(all_csv)


empty_df = pd.DataFrame(columns=all_columns)

with open("/home/reza/Desktop/SEASON_A_WGS_edited_final.json", 'r') as f:
    geoJSON = json.load(f)

geo_cache = {element["properties"]["FID"]: (element["properties"]["SA"],
                                                element["properties"]["SP"],
                                                element["properties"]["SF"])
                 for element in geoJSON["features"]}



rows = []
for csv in tqdm(all_csv):
    path = csv[1]
    name = csv[0].split(".csv")[0]



    



    

    df = pd.read_csv(path)
    df = merge_csv(empty_df, df)
    if df.shape[0] == 0:
        continue
    df = fix_dataset(df, geo_cache)
    df = fillna(df)

    df = df.drop(columns=["id_3", "id_1", "id_fid"])


    block_pixles = df.groupby("id_9")

    for name, df_group in (block_pixles):
        temp_df_group = df_group.drop(columns=["class", "id_9"])
        column_means = temp_df_group.mean()

        means_df = pd.DataFrame([column_means], columns=column_means.index)
        means_df["class"] = df_group.iloc[0]["class"]
        means_df["id"] = name
        rows.append(means_df)
    



final_database = pd.concat(rows, ignore_index=True)
final_database = fillna(final_database)

final_database.to_csv("pixle.csv" , index=False)