import pandas as pd

def fix_geomfeat(df : pd.DataFrame, 
                 geomfeat_columns : tuple):
    geomfeat = dict()
    for index in range(df.shape[0]):
        geomfeat.update(
                    {str(index) : [df.iloc[index][geomfeat_columns[0]] , df.iloc[index][geomfeat_columns[1]]]}
                    )
    return geomfeat
    