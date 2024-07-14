import random

from util import get_all_npy
from util import copy_paste
from util import create_folder

def split_data_set(path_file_npy : str,
                  path_to_save : str,
                  test_size : float,
                  validation_size : float) -> None:



    if True:

        def remover_data(files ,elements):
            for element in elements:
                files.remove(element)
            return files

        npy_files = list(get_all_npy(path_file_npy).values())


        number_test = int(
                len(npy_files) * test_size)

        number_validation = int(
            len(npy_files) * validation_size)


        test_data = random.sample(npy_files, number_test)
        npy_files = remover_data(npy_files ,test_data)



        validation_data = random.sample(npy_files, number_validation)
        npy_files = remover_data(npy_files ,validation_data)



        folder_split = path_to_save + "/DATA_split"
        create_folder(folder_split)





        folder_train = folder_split + "/train"
        create_folder(folder_train)

        for train_data in npy_files:
            copy_paste(train_data, folder_train)




        folder_test = folder_split + "/test"
        create_folder(folder_test)

        for _test_data in test_data:
            copy_paste(_test_data, folder_test)



        folder_validation = folder_split + "/validation"
        create_folder(folder_validation)

        for _validation_data in validation_data:
            copy_paste(_validation_data, folder_validation)



        print("test / train saved in path ...")

    else:
        print("not fix please try again")


