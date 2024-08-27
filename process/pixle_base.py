import os
import pandas as pd
import glob
from tqdm import tqdm
import json
import os
import shutil

from utils import (get_csv,
                   get_columns,
                   merge_csv,
                   fillna,
                   add_geometric)

        





def pixle_base(all_csv : list, path : str, empty_df : pd.DataFrame, geo_cache : dict) -> None:
    rows = []
    for csv in tqdm(all_csv):
        path = csv[1]
        name = csv[0].split(".csv")[0]



    

        df = pd.read_csv(path)
        df = merge_csv(empty_df, df)
        if df.shape[0] == 0:
            continue
        df = add_geometric(df, geo_cache)
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

    final_database.to_csv(path , index=False)