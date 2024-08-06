import pandas as pd
import numpy as np
from tqdm import tqdm


def block_to_pixle(df : pd.DataFrame, class_column : str, pixle_id_columns : str) -> pd.DataFrame:

    block_pixles = df.groupby(pixle_id_columns)
    rows = list()

    for name, df_group in tqdm(block_pixles):
        temp_df_group = df_group.drop(columns=[class_column, pixle_id_columns])
        column_means = temp_df_group.mean()

        means_df = pd.DataFrame([column_means], columns=column_means.index)
        means_df["class"] = df_group.iloc[0]["class"]
        means_df["id"] = name
        rows.append(means_df)
    

    merged_df = pd.concat(rows, ignore_index=True)
    return merged_df



