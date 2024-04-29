import os

def create_folder_for_save_npy(path: str) -> tuple:
    os.makedirs(path, exist_ok=True)
    folder_path_root = os.path.abspath(path)
    
    folder_name_data = "data"
    folder_path_data = os.path.join(folder_path_root, folder_name_data)
    os.makedirs(folder_path_data, exist_ok=True)

    folder_name_META = "META"
    folder_path_META = os.path.join(folder_path_root, folder_name_META)
    os.makedirs(folder_path_META, exist_ok=True)

    return folder_path_data, folder_path_META


def create_folder_for_report_npy(path : str,
                                 list_of_files : list) -> dict:
    path = path + "/plots"
    os.makedirs(path, exist_ok=True)
    folder_path_root = os.path.abspath(path)
    list_path = dict()
    for file in range(list_of_files):
        folder_path_root_for_file = folder_path_root + f"/{file}"
        os.makedirs(folder_path_root_for_file, exist_ok=True)
        list_path[file] = os.path.abspath(folder_path_root_for_file)
    return list_path

def create_folder_for_report_npy_same_class(path : str,
                                 list_of_lables : list) -> dict:
    path = path + "/plots_same_class"
    os.makedirs(path, exist_ok=True)
    folder_path_root = os.path.abspath(path)
    list_path = dict()
    for file in list_of_lables:
        folder_path_root_for_file = folder_path_root + f"/{file}"
        os.makedirs(folder_path_root_for_file, exist_ok=True)
        list_path[file] = os.path.abspath(folder_path_root_for_file)
    return list_path


