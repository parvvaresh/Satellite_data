import csv
import csv
import numpy as np
import rasterio

from utils import (get_all_tif_files, 
                   convert_pixel_value_to_geographic_coordinate, 
                   count_nan)


def extract_data(path_tifs, _class, id_fid, path_csv):
    path_csv += f"/{id_fid}.csv"
    info = get_all_tif_files(path_tifs)
    with open(path_csv, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        flag = True
        for path_tif, name_tif in info:
            name_tif = name_tif.split(".tif")[0]
            id_block , id_pixle = name_tif.split("_")


            with rasterio.open(path_tif) as src:        
                band_names = [src.descriptions[band_idx] for band_idx in range(src.count)]
                data = src.read()
                
                transform = src.transform
                
                lon, lat = convert_pixel_value_to_geographic_coordinate(0, 0, transform)

                


                if flag and len(band_names) != 0:

                    headers = ["id_fid", 'id_block', 'id_pixle', "X", "Y", "class"] + band_names
                    csvwriter.writerow(headers)
                    flag = False
                    print("heades is ok !")

                row_data = [id_fid, id_block, id_pixle, lon, lat, _class]
                for band_idx in range(data.shape[0]):
                    band_data = data[band_idx, :, :]
                    row_data.append(band_data[0][0])   
                len_data = len(row_data)
                count_0 = row_data.count(0)
                count_nan_ = count_nan(row_data)
                if (count_nan_ / len_data) >= 0.4  or (count_0 / len_data) >= 0.4:
                    continue
                else:
                    csvwriter.writerow(row_data)
                print("csv saved")
                