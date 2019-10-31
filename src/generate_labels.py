import settings
from utils import create_connection_from_dict, execute_sql
from eda.traj_console_print import show_image, get_color, get_ansi_color_code

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
    img_df = execute_sql('select * from features.images;', engine, read_file=False, return_df=True)

    for i, row in img_df.iterrows():
        img = [float(pix) for pix in  row['img'].replace('{', '').replace('}', '').split(',')]
        print(f"showing image for trajectory {row['traj_id']}")
        #import pdb;pdb.set_trace()
        show_image(img)
        y = input('Is this fishing?')

if __name__ == '__main__':
    run()
