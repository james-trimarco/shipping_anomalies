from utils import copy_csv_to_db
import subprocess
import os

def load_csv(TEMP_DIR, engine, temp_subdir, out_table):
    """
    Load csv files to database

    Parameters:
    TEMP_DIR : Path object
        Directory where the temporary csv files are stored locally
    engine : SQLAlchemy engine object
        Connection to the target database
    subdir: ### str

    outtable : str
        name of postgres table to store data in

    Returns:
    None
    """
    sep = ','
    for csvfile in temp_subdir.glob('*.csv'):
        copy_csv_to_db(src_file=csvfile, dst_table=out_table, engine=engine, sep=sep)


def load_shp(DATA_DIR, dir_dict, credentials_dict):
    # Get DB credentials from yaml file
    psql_cfg = credentials_dict

    for key in dir_dict:

        path = os.path.join(DATA_DIR, key)
        print(path)
        table_name = dir_dict[key]

        # Must be INSIDE the same directory as the .shp file in order to pull other files
        # Go into each directory and get the shapefile
        os.chdir(path)
        full_dir = os.walk(path)

        for source, dirs, files in full_dir:
            for file_ in files:
                if file_[-3:] == 'shp':
                    print("Found Shapefile")
                    shapefile = file_

        command = 'ogr2ogr -overwrite -f "PostgreSQL" PG:"host=' + psql_cfg['host'] + ' user=' + psql_cfg[
            'user'] + ' dbname=' + psql_cfg['dbname'] + ' password=' + psql_cfg[
                      'password'] + '" ' + shapefile + ' -nln raw.' + table_name + ' -nlt PROMOTE_TO_MULTI'

        print("Uploading file {}".format(shapefile))
        subprocess.call(command, shell=True)
        print("Done")
