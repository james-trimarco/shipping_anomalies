import os
from pathlib import Path
import random
import shutil

root = Path.cwd()
fishing_dir = root/'fishing'
not_fishing_dir = root/'not_fishing'
split_dir = root/'split'

# Must have a fishing folder and not_fishing folder containing images within EDA folder
def split_data(class_1_directory, class_2_directory, split_directory, binary_name, set_seed=223):

    if class_1_directory.is_dir() and class_2_directory:
        (split_dir/'train'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_dir/'train'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)
        (split_dir/'val'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_dir/'val'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)
        (split_dir/'test'/str(binary_name)).mkdir(parents=True, exist_ok=True)
        (split_dir/'test'/str('no_' + binary_name)).mkdir(parents=True, exist_ok=True)

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
            d = os.path.join((split_dir/'train'/str(binary_name)), item)

            shutil.copy(s, d)

        #for item in dev_filenames:
            #s = os.path.join(class_1_directory, item)
            #d = os.path.join((split_dir/'val/'/str(binary_name)), item)

            #shutil.copy(s, d)

        for item in test_filenames:
            s = os.path.join(class_1_directory, item)
            d = os.path.join((split_dir/'test'/str(binary_name)), item)

            shutil.copy(s, d)

        split_1 = int(0.8 * len(class_2_files))
        #split_2 = int(0.9 * len(class_2_files))
        train_filenames = class_2_files[:split_1]
        #dev_filenames = class_2_files[split_1:split_2]
        test_filenames = class_2_files[split_1:]

        for item in train_filenames:
            s = os.path.join(class_2_directory, item)
            d = os.path.join(split_dir/'train'/str('no_' + binary_name), item)

            shutil.copy(s, d)

        #for item in dev_filenames:
            #s = os.path.join(class_2_directory, item)
            #d = os.path.join(split_dir/'val'/str('no_' + binary_name)), item)

            #shutil.copy(s, d)

        for item in test_filenames:
            s = os.path.join(class_2_directory, item)
            d = os.path.join((split_dir/'test'/str('no_' + binary_name)), item)

            shutil.copy(s, d)

    else:
        print("ERROR: One or both of the data directories do not exist!")
        return -1
        
split_data(fishing_dir, not_fishing_dir, split_dir, "fishing")
