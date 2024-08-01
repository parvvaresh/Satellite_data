import numpy as np
import pandas as pd

from utils import clean_data, create_folder, bands_per_date, get_all_bands, get_all_dates, save_json, fix_date, fix_class,save_pkl


def mean_std(df : pd.DataFrame, path : str) -> None:
    _bands_per_date = bands_per_date(df)
    _all_bands = get_all_bands(_bands_per_date)
    _all_dates = get_all_dates(_bands_per_date)
    print(_all_dates)
    mean = []
    std = []

    for date in _all_dates:
        sample_bands_mean = []
        sample_bands_std = []
        for band in _all_bands:
            name_column = f"{date}_{band}"
            data = df[name_column]
            _mean = data.dropna().mean()
            sample_bands_mean.append(_mean)


            _std = data.dropna().std()
            sample_bands_std.append(_std)
        mean.append(np.array(sample_bands_mean))
        std.append(np.array(sample_bands_std))
    

    mean = np.array(mean)
    std =  np.array(std)
    
    result = (mean, std)
    return result
    save_pkl(result, path)

