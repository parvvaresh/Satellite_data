import pandas as pd

def fix_label(df: pd.DataFrame, class_column: str):
    labels = dict()
    for index in range(df.shape[0]):
        labels.update({str(index): df.iloc[index][class_column]})

    result = {
        f"label_{len(df[class_column].unique())}class": labels
    }
    return result
