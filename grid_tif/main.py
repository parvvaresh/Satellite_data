import concurrent.futures
import os
import shutil
from utils import get_all_tif_files
from CropImage import CropImage
from extract_data import extract_data

def process_tif(info, path_save_tif, path_save_csv, size_crop_terrain, size_crop_middle, size_crop_pixle):
    path, name = info
    name = name.split(".tif")[0]
    f_id, _class = name.split("_")
    path_save_tif_temp = path_save_tif + f"{f_id}"

    model = CropImage(path, size_crop_terrain, size_crop_middle, size_crop_pixle, path_save_tif_temp)
    model.pipeline_crop()

    extract_data(os.path.join(path_save_tif_temp, f"{size_crop_pixle}_{size_crop_pixle}"), _class, f_id, path_save_csv)
    shutil.rmtree(path_save_tif_temp)

def main(paht_tifs: str, path_save_tif: str, path_save_csv: str, size_crop_terrain: int, size_crop_middle: int, size_crop_pixle: int) -> None:
    tifs = get_all_tif_files(paht_tifs)

    # Set the number of workers to the number of CPU cores
    num_workers = os.cpu_count()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                process_tif, info, path_save_tif, path_save_csv, size_crop_terrain, size_crop_middle, size_crop_pixle
            ) for info in tifs
        ]

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")

# Example call to main function
main(
    paht_tifs="/home/reza/Desktop/abyek_fainal7",
    path_save_tif="/home/reza/Desktop/tifs/test7_",
    path_save_csv="/home/reza/Desktop/temp_csv7",
    size_crop_terrain=9,
    size_crop_middle=3,
    size_crop_pixle=1
)