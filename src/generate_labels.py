import settings
import pandas as pd
from pathlib import Path
import shutil
from utils import create_connection_from_dict, execute_sql


def fishing_prefilter(df, turn90 = 3, turn30 = 5, direct = 2):
    
    # Filter the dataframe to fishing vessels
    fishing_df = df[df['vessel_type'] == 'Fishing'].copy()
    
    # Create a function that will label rows as fishy or not fishy
    def row_filter(row):
        if row['turn90'] >= turn90 and row['turn30'] >= turn30 and row['direct'] >= direct:
            val = 1
        else:
            val = 0
        return val
    
    # Apply the function above to filter rows
    fishing_df['prefilter'] = fishing_df.apply(row_filter, axis=1)
    
    return fishing_df


def nonfishing_dataframe_creator(df, fishing_df):
    nonfishing_vessel_df = df[df['vessel_type'] != 'Fishing'].copy()
    nonfishy_fishing_vessel_df = fishing_df[fishing_df['prefilter'] == 0].drop(['prefilter'], axis=1).copy()
    
    nonfishing_df = pd.concat([nonfishing_vessel_df, nonfishy_fishing_vessel_df], ignore_index=True)
    
    return nonfishing_df


def sampler(df_fish, df_nonfish, n = None, frac = None, dataset_frac = 0.5, seed = 223):
    
    # Filter the dataframe to fishy fishing vessels
    fishy_fishing_df = df_fish[df_fish['prefilter'] == 1].drop(['prefilter'], axis=1).copy()
    
    # Get length of fishy fishing dataframe
    axis_length_fish = fishy_fishing_df.shape[0]
    
    # If no frac or n, default to n=100.
    if n is None and frac is None:
        n = int(round(0.5 * axis_length_fish))
    elif n is not None and frac is None and n > axis_length_fish:
        print("Provided dataset size is over trajectory count. Using full data.")
        n = axis_length_fish
    elif n is not None and frac is None and n % 1 != 0:
        raise ValueError("Only integers accepted as `n` values")
    elif n is None and frac is not None:
        n = int(round(frac * axis_length_fish))
    elif n is not None and frac is not None:
        raise ValueError("Please enter a value for `frac` OR `n`, not both")
    
    # Grab a sample of the fishy fishing rows
    fishy_fishing_sample = fishy_fishing_df.sample(n = n, random_state = seed)
    fishy_fishing_sample['fishing_status'] = 'fishing'
    
    # Get length of fishy fishing sample
    axis_length_sample = fishy_fishing_sample.shape[0]
    
    # Get length of nonfishing dataframe
    axis_length_nonfish = df_nonfish.shape[0]
    
    nonfish_sample_length = int(round((1/dataset_frac)*axis_length_sample))
    
    if nonfish_sample_length > axis_length_nonfish:
        n = axis_length_nonfish
    else:
        n = nonfish_sample_length
    
    # Grab a sample of the fishy fishing rows
    nonfishing_sample = df_nonfish.sample(n = n, random_state = seed)
    nonfishing_sample['fishing_status'] = 'not_fishing'
    
    final_dataset = pd.concat([fishy_fishing_sample, nonfishing_sample], ignore_index=True)
    
    def path_creator(row):
        path = row['vessel_type'] + '/' + row['traj_id'] + '.png'
        return path
    
    final_dataset['path'] = final_dataset.apply(path_creator, axis=1)
    
    return final_dataset


def trajectory_seperator(df):
    
    if fishing_path.exists():
        shutil.rmtree(fishing_path, ignore_errors=False, onerror=None)
        
    if nonfishing_path.exists():
        shutil.rmtree(nonfishing_path, ignore_errors=False, onerror=None)

    fishing_path.mkdir(parents=True, exist_ok=True)
    nonfishing_path.mkdir(parents=True, exist_ok=True)
    
    def row_copy(row):
        if row['fishing_status'] == 'fishing':
            shutil.copy(root/row['path'], fishing_path)
        else:
            shutil.copy(root/row['path'], nonfishing_path)

    df.apply(row_copy, axis=1)


def run():
    """
    TODO: write docstring
    """
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    # Get data to process from postgres
    quants_df = execute_sql('select * from features.quants;', engine, read_file=False, return_df=True)


if __name__ == '__main__':

    root = Path.cwd()/'trajectory_folder'
    fishing_path = Path.cwd()/'fishing'
    nonfishing_path = Path.cwd()/'nonfishing'
    
    #run()
    fishy_stuff = fishing_prefilter(quants_df)
    nonfish = nonfishing_dataframe_creator(quants_df, fishy_stuff)
    dataset = sampler(fishy_stuff,nonfish)
    trajectory_seperator(dataset)
