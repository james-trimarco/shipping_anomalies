import os
from pathlib import Path
import random
import shutil

#root = Path.cwd()
#fishing_dir = root/'fishing'
#not_fishing_dir = root/'not_fishing'
#split_dir = root/'split'

# Must have a fishing folder and not_fishing folder containing images within EDA folder
def split_data(class_1_directory, class_2_directory, split_directory, binary_name, set_seed=223):
    """
    :param class_1_directory: pathlib path object
        Where your fishing directory is
    :param class_2_directory: pathlib path object
        Where your nonfishing directory is
    :param split_directory: pathlib path object
        The location where you want the train and test directories to live
    :param binary_name: str
        Inside both /train and /split, the positive class will be named this; the
        negative class will be named this with prefix 'no_'.
    :param set_seed:
    :return:
    """
    #import pdb; pdb.set_trace()
    if class_1_directory.is_dir() and class_2_directory.is_dir():
        (split_directory/'train'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_directory/'train'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)
        (split_directory/'val'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_directory/'val'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)
        (split_directory/'test'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_directory/'test'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)

        # Obtain files from class 1 and class 2:
        class_1_files = os.listdir(class_1_directory)
        class_2_files = os.listdir(class_2_directory)

        class_1_files.sort()
        class_2_files.sort()

        random.seed(set_seed)

        random.shuffle(class_1_files)
        random.shuffle(class_2_files)

        split_1 = int(0.8 * len(class_1_files))
        #split_2 = int(0.9 * len(class_1_files))
        train_filenames = class_1_files[:split_1]
        #dev_filenames = class_1_files[split_1:split_2]
        test_filenames = class_1_files[split_1:]

        for item in train_filenames:
            s = os.path.join(class_1_directory, item)
            d = os.path.join((split_directory/'train'/str(binary_name)), item)

            shutil.copy(s, d)

        #for item in dev_filenames:
            #s = os.path.join(class_1_directory, item)
            #d = os.path.join((split_directory/'val/'/str(binary_name)), item)

            #shutil.copy(s, d)

        for item in test_filenames:
            s = os.path.join(class_1_directory, item)
            d = os.path.join((split_directory/'test'/str(binary_name)), item)

            shutil.copy(s, d)

        split_1 = int(0.8 * len(class_2_files))
        #split_2 = int(0.9 * len(class_2_files))
        train_filenames = class_2_files[:split_1]
        #dev_filenames = class_2_files[split_1:split_2]
        test_filenames = class_2_files[split_1:]

        for item in train_filenames:
            s = os.path.join(class_2_directory, item)
            d = os.path.join(split_directory/'train'/str('no_' + binary_name), item)

            shutil.copy(s, d)

        #for item in dev_filenames:
            #s = os.path.join(class_2_directory, item)
            #d = os.path.join(split_directory/'val'/str('no_' + binary_name)), item)

            #shutil.copy(s, d)

        for item in test_filenames:
            s = os.path.join(class_2_directory, item)
            d = os.path.join((split_directory/'test'/str('no_' + binary_name)), item)

            shutil.copy(s, d)

    else:
        print("ERROR: One or both of the data directories do not exist!")
        return -1
        
#split_data(fishing_dir, not_fishing_dir, split_dir, "fishing")
