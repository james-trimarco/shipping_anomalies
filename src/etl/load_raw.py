from utils import copy_csv_to_db

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