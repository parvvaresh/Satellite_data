import concurrent.futures
import os
import pandas as pd
import glob
from tqdm import tqdm
import json
import os
import shutil
from datetime import datetime, timedelta


from .utils import (get_csv,
                   get_columns,
                   merge_csv,
                   add_geometric)


class pixle_base:
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path + "/pixle_base.csv"
        

    def fit(self, geoJSON: dict, csv_paths: list, class_column: str, block_column: str) -> None:
        self.geo_cache = {element["properties"]["FID"]: (element["properties"]["SA"],
                                                        element["properties"]["SP"],
                                                        element["properties"]["SF"])
                          for element in geoJSON["features"]}
        
        self.csv_paths = csv_paths
        self.class_column = class_column
        self.block_column = block_column
        self.all_columns = get_columns(csv_paths)
        self.empty_df = pd.DataFrame(columns=self.all_columns)
    

    def _transform_dataframe(self, csv: tuple) -> list:
        path = csv[1]
        name = csv[0].split(".csv")[0]

        rows = []

        df = pd.read_csv(path)
        df = merge_csv(self.empty_df, df)
        if df.shape[0] == 0:
            return []
        df = add_geometric(df, self.geo_cache)
        df = df.fillna(-1)

        block_pixles = df.groupby(self.block_column)

        for name, df_group in block_pixles:
            temp_df_group = df_group.drop(columns=[self.class_column, self.block_column])
            column_means = temp_df_group.mean()

            means_df = pd.DataFrame([column_means], columns=column_means.index)
            means_df[self.class_column] = df_group.iloc[0][self.class_column]
            means_df["id"] = name
            rows.append(means_df)
        
        return rows
            

    def transform(self, max_workers: int = 16) -> None:
        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._transform_dataframe, csv): csv for csv in self.csv_paths}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(self.csv_paths)):
                result = future.result()
                if result:
                    rows.extend(result)

        final_database = pd.concat(rows, ignore_index=True)
        final_database = final_database.fillna(-1)
        final_database.to_csv(self.root_path, index=False)
