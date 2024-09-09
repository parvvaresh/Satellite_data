import os
import glob
import rasterio
from rasterio.transform import Affine

def make_folder(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)    


def get_all_tif_files(path: str) -> list:
    tif_files = glob.glob(os.path.join(path, "*.tif"))
    files_info = [(file, os.path.basename(file)) for file in tif_files]
    return files_info


def convert_pixel_value_to_geographic_coordinate(refx, refy, transform):
    lon, lat = rasterio.transform.xy(transform, refy, refx)
    return lon, lat

def count_nan(row):
    count = 0
    for x in row:
        if str(x) == "nan":
            count += 1
    return count


