import pathlib
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, read_file
from shapely.geometry import Point, LineString, Polygon
import movingpandas as mp
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


curr_dir = pathlib.Path.cwd()
(curr_dir / 'fishing').mkdir(parents = True,exist_ok=True)
(curr_dir / 'not_fishing').mkdir(parents = True,exist_ok=True)
#cord = new_box(pd.DataFrame(test)['trajectory'].loc[2].get_bbox())
def new_box(bbox_tuple):
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
    cord = new_box(traj1.get_bbox())
    ax = traj1.plot(column = 'speed', with_basemap=False,vmin =0, vmax = 15,cmap = "Greys")
    ax.set_axis_off()
    ax.set_ylim([cord[1],cord[3]])
    ax.set_xlim([cord[0],cord[2]])
    #ax.set_facecolor("black")
    fig = ax.get_figure()
    plt.savefig(out_dir,bbox_inches = 'tight', dpi=42.7, pad_inches=0)

#out_images('fishing/test.png',pd.DataFrame(test)['trajectory'].loc[2])

#pathlib.Path(curr_dir)

def add_class(traj,class_col,seq_id,day):
    classes     = traj.df[class_col].unique()
    mmsi_value  = traj.df['mmsi'].iloc[0]
    if 7 in classes:
        out_dir1 = f'fishing/{mmsi_value}_{day}_{seq_id}.png'
        out_images(out_dir1,traj)
        return {class_col:1,'trajectory':traj,'mmsi':mmsi_value,'seq_id': seq_id} # 1 for fishing
    else:
        out_dir1 = f'not_fishing/{mmsi_value}_{day}_{seq_id}.png'
        out_images(out_dir1,traj)
        return {class_col:0,'trajectory':traj,'mmsi':mmsi_value,'seq_id' : seq_id}
    
def get_traj(df,ID,time_field,day_n,lat,long,min_length,seq_id):
    """
    takes in one month of data and generates list of trajectories on given day
    """
    # Creating a Geographic data frame
    df['t'] = pd.to_datetime(df[time_field],format = '%m/%d/%y %H:%M')
    df = df[df['t'].dt.day==day_n]
    geometry = [Point(xy) for xy in zip(df[long], df[lat])]
    crs = {'init': 'epsg:4326'}  # Coordinate reference system : WGS84
    df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    df = df.set_index('t')
    
    #Building Trajectories
    MIN_LENGTH = min_length # Threshold to remove stationary points
    t_start = datetime.now()
    trajectories = []
    generalized_trajs = []
    for key, values in df.groupby([ID]):
        seq_id = seq_id + 1
        if len(values) < 2:
            continue
        trajectory = mp.Trajectory(key, values)
        if trajectory.get_length() < MIN_LENGTH:
            continue
        else:
            trajectory = add_class(trajectory,'navstat',seq_id,day_n)
            trajectories.append(trajectory)
            #generalized_trajs.append(trajectory.generalize(mode='douglas-peucker', tolerance=0.001)) #(mode='min-time-delta', tolerance=timedelta(minutes=5))
    
    print("Finished creating {} trajectories in {}".format(len(trajectories),datetime.now() - t_start))
    return #trajectory
    
#test = get_traj(df=cnn,ID='mmsi',time_field='time_stamp',day_n=15,lat='latitude',long='longitude',min_length = 2000)

#seq_id = 0
for i in range(31):
    seq_id =0
    test = get_traj(df=cnn,ID='mmsi',time_field='time_stamp',day_n=i,lat='latitude',long='longitude',min_length = 2000,seq_id = 0)
