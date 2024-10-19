import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from .to_npy import (csv_to_npy_all, csv_to_npy_split)

from .utils import (create_folder, merge_csv, fillna_with_input, add_geometric, get_columns, save_metadata, _mean_std, save_pkl)

class ConvertData:
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path
        self._create_folder()
        print("Folders created.")

    def _create_folder(self):
        create_folder(self.root_path)
        
        self.root_path_all = os.path.join(self.root_path, "all")
        create_folder(self.root_path_all)
        self.root_path_all_data = os.path.join(self.root_path_all, "DATA")
        create_folder(self.root_path_all_data)
        
        self.root_path_split = os.path.join(self.root_path, "split")
        create_folder(self.root_path_split)
        self.root_path_split_data = os.path.join(self.root_path_split, "DATA")
        create_folder(self.root_path_split_data)
        
        print("Folders created on paths.")

    def fit(self, geoJSON: dict, csv_paths: list, class_column: str, latlong_columns: list, block_column: str, start_date: datetime, finish_date: datetime, interval: int, fillna_method_value: tuple) -> None:
        self.geo_cache = {
            element["properties"]["FID"]: (
                element["properties"]["SA"],
                element["properties"]["SP"],
                element["properties"]["SF"]
            )
            for element in geoJSON["features"]
        }
        
        self.csv_paths = csv_paths
        self.class_column = class_column
        self.latlong_columns = latlong_columns
        self.block_column = block_column
        self.start_date = start_date
        self.finish_date = finish_date
        self.interval = interval
        self.fillna_method_value = fillna_method_value

        self.class_info_split = None
        self.geo_info_split = None
        self.date_split = None
        self.classes_split = dict()
        self.gefeat_split = dict()

        self.class_info_all = None
        self.geo_info_all = None
        self.date_all = None
        self.classes_all = dict()
        self.gefeat_all = dict()

        self.total_samples_all = 0

        self.mean_sum_all = None
        self.variance_sum_all = None

        self.mean_sum_s1 = None
        self.variance_sum_s1 = None

        self.mean_sum_s2 = None
        self.variance_sum_s2 = None

    def _transform_dataframe(self, csv: tuple):
        try:
            path = csv[1]
            name = os.path.splitext(csv[0])[0]
            df = pd.read_csv(path)
            df = merge_csv(self.empty_df, df)
            n_samples = df.shape[0]

            if df.shape[0] == 0:
                return None

            fillna_method, fillna_value = self.fillna_method_value
            df = fillna_with_input(df, fillna_method, fillna_value)
            df = add_geometric(df, self.geo_cache)

            mean_all, std_all = _mean_std(df)

            # Process 'all'
            model_all = csv_to_npy_all(df,
                                    self.class_column,
                                    self.latlong_columns,
                                    self.block_column,
                                    self.root_path_all_data,
                                    self.start_date,
                                    self.finish_date,
                                    self.interval)
            model_all.get_npy()
            date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all = model_all.get_meta()

            # Process 'split'
            model_split = csv_to_npy_split(df,
                                        self.class_column,
                                        self.latlong_columns,
                                        self.block_column,
                                        self.root_path_split_data,
                                        self.start_date,
                                        self.finish_date,
                                        self.interval)
            model_split.get_npy()
            date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split = model_split.get_meta()

            df_s1 = model_split.get_s1()
            df_s2 = model_split.get_s2()

            mean_s1, std_s1 = _mean_std(df_s1)
            mean_s2, std_s2 = _mean_std(df_s2)

            return (date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all,
                    date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split, n_samples,
                    mean_all, std_all, mean_s1, std_s1, mean_s2, std_s2)
        except Exception as e:
            return f"Error processing {csv}: {e}"
    def transform(self) -> None:
        print("Start collecting columns of data sets.")
        all_columns = get_columns(self.csv_paths)
        print("Finished collecting columns of data sets.")
        self.empty_df = pd.DataFrame(columns=all_columns)
        print("Start converting data.")

        # Initialize flag to check if any data is processed
        data_processed = False

        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(self._transform_dataframe, csv) for csv in self.csv_paths]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    result = future.result()
                    if result:
                        (date_all, class_json_all, class_info_all, gefeat_json_all, geo_info_all,
                        date_split, class_json_split, class_info_split, gefeat_json_split, geo_info_split, n_samples,
                        mean_all, std_all, mean_s1, std_s1, mean_s2, std_s2) = result

                        # Update the flag indicating data has been processed
                        data_processed = True

                        self.classes_all.update(class_json_all)
                        self.gefeat_all.update(gefeat_json_all)
                        self.class_info_all = class_info_all
                        self.geo_info_all = geo_info_all
                        self.date_all = date_all

                        self.classes_split.update(class_json_split)
                        self.gefeat_split.update(gefeat_json_split)
                        self.class_info_split = class_info_split
                        self.geo_info_split = geo_info_split
                        self.date_split = date_split

                        self.total_samples_all += n_samples

                        if mean_all.size == 0 or std_all.size == 0:
                            continue

                        if self.mean_sum_all is None:
                            self.mean_sum_all = np.zeros_like(mean_all)
                            self.variance_sum_all = np.zeros_like(mean_all)

                        if self.mean_sum_s1 is None:
                            self.mean_sum_s1 = np.zeros_like(mean_s1)
                            self.variance_sum_s1 = np.zeros_like(mean_s1)

                        if self.mean_sum_s2 is None:
                            self.mean_sum_s2 = np.zeros_like(mean_s2)
                            self.variance_sum_s2 = np.zeros_like(mean_s2)

                        # Accumulate the sums for mean and variance
                        self.mean_sum_all += mean_all * n_samples
                        self.variance_sum_all += np.power(std_all, 2) * n_samples

                        self.mean_sum_s1 += mean_s1 * n_samples
                        self.variance_sum_s1 += np.power(std_s1, 2) * n_samples

                        self.mean_sum_s2 += mean_s2 * n_samples
                        self.variance_sum_s2 += np.power(std_s2, 2) * n_samples

                except Exception as e:
                    print(f"An error occurred: {e}")

        # Check if any data was processed before performing further calculations
        if not data_processed:
            print("No data was processed, skipping mean and std calculation.")
            return

        # Perform calculations if data was processed
        overall_mean_all = self.mean_sum_all / self.total_samples_all
        overall_variance_all = self.variance_sum_all / self.total_samples_all
        overall_variance_all = np.maximum(overall_variance_all, 0)
        overall_std_all = np.sqrt(overall_variance_all)

        overall_mean_s1 = self.mean_sum_s1 / self.total_samples_all
        overall_variance_s1 = self.variance_sum_s1 / self.total_samples_all
        overall_variance_s1 = np.maximum(overall_variance_s1, 0)
        overall_std_s1 = np.sqrt(overall_variance_s1)

        overall_mean_s2 = self.mean_sum_s2 / self.total_samples_all
        overall_variance_s2 = self.variance_sum_s2 / self.total_samples_all
        overall_variance_s2 = np.maximum(overall_variance_s2, 0)
        overall_std_s2 = np.sqrt(overall_variance_s2)

        print("Finished converting data.")
        print("Start saving metadata.")

        # Save for 'all'
        save_metadata(self.root_path_all, self.classes_all, self.gefeat_all, self.date_all, self.class_info_all, self.geo_info_all)
        save_pkl((overall_mean_all, overall_std_all), f"{self.root_path_all}/mean_std.pkl")

        # Save for 'split'
        save_metadata(self.root_path_split, self.classes_split, self.gefeat_split, self.date_split, self.class_info_split, self.geo_info_split)
        print("Finished saving metadata.")

        # Save mean and std for S1
        save_pkl((overall_mean_s1, overall_std_s1), f"{self.root_path_split_data}/S1/mean_std.pkl")

        # Save mean and std for S2
        save_pkl((overall_mean_s2, overall_std_s2), f"{self.root_path_split_data}/S2/mean_std.pkl")

        # Save total sample count in a text file
        with open(f"{self.root_path}/total_samples_all.txt", "w") as file:
            file.write(str(self.total_samples_all))

        

    def MeanStd(self, mean: np.array, std: np.array, n_samples: int) -> tuple:
        """
        Calculate and return the overall mean and standard deviation.
        """
        overall_mean = mean / n_samples
        overall_variance = (std ** 2) / n_samples
        overall_variance = np.maximum(overall_variance, 0)
        overall_std = np.sqrt(overall_variance)

        return overall_mean, overall_std
