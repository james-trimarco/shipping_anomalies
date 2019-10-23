from utils import copy_csv_to_db
import subprocess
import os


def load_csv(dir, engine, out_table, sep=',', quote='"'):
    """
    Load csv files to database

    Parameters:
    dir : Path object
        Directory where the temporary csv files are stored locally
    engine : SQLAlchemy engine object
        Connection to the target database
    subdir: ### str

    outtable : str
        name of postgres table to store data in

    Returns:
    None
    """
    for csv_file in dir.iterdir():
        if '.crc' not in csv_file.name and 'SUCCESS' not in csv_file.name:
            print(csv_file.name)
            copy_csv_to_db(src_file=csv_file, dst_table=out_table, engine=engine, sep=sep, quote=quote)


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


def load_iuu_list(filename, engine):
    iuu_list_orig = Path.cwd().parent.parent.joinpath('aux_data').joinpath(filename)
    pd_test = pd.read_csv(iuu_list_orig, delimiter="\t", encoding="ISO-8859-1")

    pd_test.rename(columns={'IOTC': 'Date_IOTC',
                            'ICCAT': 'Date_ICCAT',
                            'IATTC': 'Date_IATTC',
                            'WCPFC': 'Date_WCPFC',
                            'CCAMLR': 'Date_CCAMLR',
                            'SEAFO': 'Date_SEAFO',
                            'INTERPOL': 'Date_INTERPOL',
                            'NEAFC': 'Date_NEAFC',
                            'NAFO': 'Date_NAFO',
                            'SPRFMO': 'Date_SPRFMO',
                            'CCSBT': 'Date_CCSBT',
                            'GFCM': 'Date_GFCM',
                            'NPFC': 'Date_NPFC',
                            'SIOFA': 'Date_SIOFA',
                            'Reason': 'Reason_IOTC',
                            'Reason.1': 'Reason_ICCAT',
                            'Reason.2': 'Reason_IATTC',
                            'Reason.3': 'Reason_WCPFC',
                            'Reason.4': 'Reason_CCAMLR',
                            'Reason.5': 'Reason_SEAFO',
                            'Reason.6': 'Reason_INTERPOL',
                            'Reason.7': 'Reason_NEAFC',
                            'Reason.8': 'Reason_NAFO',
                            'Reason.9': 'Reason_SPRFMO',
                            'Reason.10': 'Reason_CCSBT',
                            'Reason.11': 'Reason_GFCM',
                            'Reason.12': 'Reason_NPFC',
                            'Reason.13': 'Reason_SIOFA',
                            }, inplace=True)

    pd_test = pd_test.reset_index()
    long = pd.wide_to_long(pd_test, i='index', j='Violation', stubnames=['Reason', 'Date'], sep="_", suffix='\\w+')

    long = long.dropna(how='all', subset=['Reason', 'Date']).reset_index()
    long = long.drop('index', axis=1)

    copy_csv_to_db(long, iuu_list, engine, header=True, sep=',')
