import pathlib
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, read_file
from shapely.geometry import Point, LineString, Polygon
import movingpandas as mp
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


def new_box(bbox_tuple):
    """
    Takes in trajectory boundry box of form (LAT1, LON1, LAT2, LON2),
    outputs cords for bounding trajectory in a square.
    """
    # TUPLE HAS FORM (LAT1, LON1, LAT2, LON2)
    lat_length = bbox_tuple[2] - bbox_tuple[0]
    lon_length = bbox_tuple[3] - bbox_tuple[1]
    
    difference = lat_length - lon_length
    abs_diff = abs(difference)
    extension = abs_diff/2
    
    # Lat > Lon
    if difference > 0:
        return (bbox_tuple[0], bbox_tuple[1]-extension, bbox_tuple[2], bbox_tuple[3]+extension)
        
    # Lat < Lon
    elif difference < 0:
        return (bbox_tuple[0]-extension, bbox_tuple[1], bbox_tuple[2]+extension, bbox_tuple[3])
        
    else:
        return bbox_tuple
    
def out_images(out_dir,traj1):
    """
    Converts trajectory to image and saves to out_dir
    """
    cord = new_box(traj1.get_bbox())
    ax = traj1.plot(column = 'speed', with_basemap=False,vmin =0, vmax = 15,cmap = "Greys")
    ax.set_axis_off()
    ax.set_ylim([cord[1],cord[3]])
    ax.set_xlim([cord[0],cord[2]])
    #ax.set_facecolor("black")
    fig = ax.get_figure()
    fig.savefig(out_dir,bbox_inches = 'tight', dpi=42.7, pad_inches=0)
    fig.clf()
    plt.close()
    return 
   
def add_class(traj,class_col,seq_id,day):
    """
    Adds poshness
    """
    classes     = traj.df[class_col].unique()
    mmsi_value  = traj.df['mmsi'].iloc[0]
    if 7 in classes:
        out_dir1 = f'fishing/{mmsi_value}_{day}_{seq_id}.png'
        out_images(out_dir1,traj)
        return 
    else:
        out_dir1 = f'not_fishing/{mmsi_value}_{day}_{seq_id}.png'
        out_images(out_dir1,traj)
        return 
    
def df_to_geodf(df,time_field,long,lat):
    """
    Takes in pd dataframe converts to geodf
    """
    
    df['t'] = pd.to_datetime(df[time_field],format = '%m/%d/%y %H:%M')
    df['day'] = df['t'].dt.day
    df = df.set_index('t')
    geometry = [Point(xy) for xy in zip(df[long], df[lat])]
    crs = {'init': 'epsg:4326'}  # Coordinate reference system : WGS84
    df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    return df


def get_traj(df,ID,day_n,min_length,seq_id):
    """
    Takes in one month of data and generates list of trajectories on given day
    """   
    df = df[df['day']==day_n]
    
    #Building Trajectories
    MIN_LENGTH = min_length # Threshold to remove stationary points
    t_start = datetime.now()
    count = 0
    for key, values in df.groupby([ID]):
        seq_id = seq_id + 1
        if len(values) < 2:
            continue
        trajectory = mp.Trajectory(key, values)
        if trajectory.get_length() < MIN_LENGTH:
            continue
        else:
            count +=1
            add_class(trajectory,'navstat',seq_id,day_n)
            
    print("Finished creating {} trajectories in {}".format(count,datetime.now() - t_start))
    return    


# Generating Images

curr_dir = pathlib.Path.cwd()
(curr_dir / 'fishing').mkdir(parents = True,exist_ok=True)
(curr_dir / 'not_fishing').mkdir(parents = True,exist_ok=True)

cnn = pd.read_csv('cnn_sample_3.csv')

geo_df = df_to_geodf(cnn, 'time_stamp','longitude','latitude')

for i in range(31):
    seq_id =0
    get_traj(df=geo_df,ID='mmsi',day_n=i,min_length = 2000,seq_id = 0)
