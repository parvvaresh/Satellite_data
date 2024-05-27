import random

from util import get_all_npy
from util import copy_paste
from util import create_folder

def split_test_train(path_file_npy : str,
                     path_to_save : str,
                     test_size : int = 0.2 ) -> None:
    
    npy_files = list(get_all_npy(path_file_npy).values())
    number_test = int(
            len(path_file_npy) * test_size)
    
    
    test_data = random.sample(npy_files, number_test)
    
    
        
    folder_split = path_to_save + "/DATA_split"
    create_folder(folder_split)
    
            
    folder_train = folder_split + "/train"
    create_folder(folder_train)
    
    for train_data in npy_files:
        if train_data not in test_data:
            copy_paste(train_data, folder_train)


    folder_test = folder_split + "/test"
    create_folder(folder_test)
    
    for _test_data in test_data:
        copy_paste(_test_data, folder_test)
    
    
    print("test / train saved in path ...")


