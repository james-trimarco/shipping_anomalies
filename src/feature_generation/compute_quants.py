# assume input is a Pandas dataframe already subsetted to be one mmsi and one time_chunk
import pandas as pd
import numpy as np
from numpy.linalg import eig, inv
from functools import reduce


# Each row will be one ping with time_stamps, lats, and lons

# Run the below cluster_metrics() function on the dataframe of a trajectory (one mmsi, one timechunk...) to get one row
# with 'MMSI','TIME','MINLON','MAXLON','MINLAT','MAXLAT','a_xx','a_xy','a_yy','a_x','a_y','a_1','ell_center_x',
# 'ell_center_y','ell_major','ell_minor','b','m','direct_lon','direct_lat','direct','lonpath',
# 'latpath','curve_len','maxspeed','meanspeed','num_turns_90','num_turns_30'.

# Here the 'a_*' coefficients are the coefficients in the equation of the fitting ellipse.
# The 'ell_center_*' is the coordinate location of the ellipse center.
# The 'ell_major' and 'ell_minor' are the major and minor axes lengths.
# The 'b' and 'm' values are obtained from a line of best fit through the lat,lon points.
# The 'direct_lon' and 'direct_lat' and 'direct' are the distance from the starting and ending points in longitude and latitude and then combined using a pythagorean metric.
# The 'lonpath', the 'latpath', and the 'curve_len' are the total summed path length along longitude, latitude, and summing the pythagorean metric.
# The 'maxspeed' gives the maximum speed and 'meanspeed', the mean speed.
# The 'num_turns_90' gives the number of turns that are greater than 90 degrees, and 'turn30' gives the number of turns that are greater than 30 degrees.


def give_angle(mydf):
    # import pdb; pdb.set_trace()
    mydf.loc[(mydf.v_lon>0),'angle'] = np.arctan(mydf.v_lat/mydf.v_lon) * np.float(180) / np.pi
    mydf.loc[(mydf.v_lon<0),'angle'] = np.float(180) + np.arctan(mydf.v_lat/mydf.v_lon) * np.float(180) / np.pi
    mydf.loc[(mydf.v_lon==0),'angle'] = np.float(270)
    mydf.loc[((mydf.v_lon==0) & (mydf.v_lat==0)),'angle'] = np.float(0)
    mydf.loc[((mydf.v_lon==0) & (mydf.v_lat>0)),'angle'] = np.float(90)
    mydf.loc[((mydf.v_lon==0) & (mydf.v_lat<0)),'angle'] = np.float(270)
    return mydf

def fit_ellipse(x, y):
    x = x[:, np.newaxis]
    y = y[:, np.newaxis]
    D = np.hstack((x * x, x * y, y * y, x, y, np.ones_like(x)))
    S = np.dot(D.T, D)
    C = np.zeros([6, 6])
    C[0, 2] = C[2, 0] = 2;
    C[1, 1] = -1
    E, V = eig(np.dot(inv(S), C))
    n = np.argmax(np.abs(E))
    a = V[:, n]
    return a


def ellipse_center(a):
    b, c, d, f, g, a = a[1] / 2, a[2], a[3] / 2, a[4] / 2, a[5], a[0]
    num = b * b - a * c
    x0 = (c * d - b * f) / num
    y0 = (a * f - b * d) / num
    return x0, y0


def ellipse_axis_length(a):
    b, c, d, f, g, a = a[1] / 2, a[2], a[3] / 2, a[4] / 2, a[5], a[0]
    up = 2 * (a * f * f + c * d * d + g * b * b - 2 * b * d * f - a * c * g)
    down1 = (b * b - a * c) * ((c - a) * np.sqrt(1 + 4 * b * b / ((a - c) * (a - c))) - (c + a))
    down2 = (b * b - a * c) * ((a - c) * np.sqrt(1 + 4 * b * b / ((a - c) * (a - c))) - (c + a))
    res1 = np.sqrt(up / down1)
    res2 = np.sqrt(up / down2)
    return res1, res2


def compute_quants(df):
    # Must write a function that creates one pandas Series for each input with 5 to 10 numerical features

    ##SHAPES
    # Bounding Box
    # Bounding Ellipse
    # Line of Best Fit

    # METRICS
    # Directness Ratio
    # Max Speed
    # Mean Speed
    # 90+ Degree Turns
    # 30+ Degree Turns
    
    #Confirm data types

    # First convert initial rows to a form that includes velocity
    df = df.sort_index()

    # Confirm we have latitude and longitude as floats
    df['time_stamp']=df.time_stamp.apply(lambda x:pd.to_datetime(x))
    df['longitude']=df.longitude.apply(lambda x:np.float(x))
    df['latitude']=df.latitude.apply(lambda x:np.float(x))
    #TODO: decide on .dropna()
    df_diff = df.diff()#.dropna()
    
    # Set NA values on first step to 0, but set first time value to 1 for division (set back to 0 later for distributions)
    #TODO: pd.Timedelta(0),etc.
    df_diff.loc[(df_diff.index==df_diff.index[0]),'longitude'] = np.float(0)
    df_diff.loc[(df_diff.index==df_diff.index[0]),'latitude'] = np.float(0)
    df_diff.loc[(df_diff.index==df_diff.index[0]),'time_stamp'] = pd.Timedelta('1 second')
    df['t_lag'] = df_diff['time_stamp'].apply(lambda x:x.total_seconds())
    df['lat_lag'] = df_diff['latitude']
    df['lon_lag'] = df_diff['longitude']
    df['v_lat'] = df['lat_lag']/df['t_lag']
    df['v_lon'] = df['lon_lag']/df['t_lag']
    df['speed'] = np.sqrt(df['v_lat'] ** 2 + df['v_lon'] ** 2)
    df['ll_maglag'] = np.sqrt(df['lat_lag'] ** 2 + df['lon_lag'] ** 2)

    # Rows are now in form ['time','lat','lon','t_lag','lat_lag','v_lat','lon_lag','v_lon','ll_magn','speed','count']
    # Add the angle of the boat at that time via tan^-1(v_lat/v_lon)
    
    #TODO: check that this TODO is complete
    #TODO: attempt to accomplish reduce syntax on give_angle for good form
    df = give_angle(df)

    # Rows now have 'angle' term at the end
    
    #TODO: check that this TODO is complete
    #TODO: rename df_diff to express that this is only for angle differencing
    angle_diff = df.diff()
    angle_diff.loc[(angle_diff.index==angle_diff.index[0]),'angle'] = np.float(0)
    
    # add angle changes (how big the turn was, in degrees) and speed to each row
    #TODO: add angular speeds as new field in df same way as before
    df['angle_chg'] = angle_diff['angle']

    # now we will add the features we are interested in to a single pandas row

    # Directness Ratio calculations
    direct_lon = df.at[df.index[-1],'longitude']-df.at[df.index[0],'longitude']
    direct_lat = df.at[df.index[-1],'latitude']-df.at[df.index[0],'latitude']

    # 90+ Degree Turns
    def turn_90(z):
        map_90 = map(lambda x: 1 if x > 90 else 0, z)
        return reduce(lambda x, y: x + y, map_90)

    # 30+ Degree Turns
    def turn_30(z):
        map_30 = map(lambda x: 1 if x > 30 else 0, z)
        return reduce(lambda x, y: x + y, map_30)

    outrow = pd.DataFrame(
        columns=['minlon', 'maxlon', 'minlat', 'maxlat', 'a_xx', 'a_xy', 'a_yy', 'a_x', 'a_y', 'a_1',
                 'ell_center_x', 'ell_center_y', 'ell_major', 'ell_minor', 'slope', 'intercept', 'count',
                 'direct_lon', 'direct_lat', 'direct', 'lonpath', 'latpath', 'curve_len', 'maxspeed', 
                 'meanspeed', 't_total', 't_lag_mean', 't_lag_sd', 't_lag_max', 'turn90', 'turn30'])
    outrow = outrow.astype(np.float)
    
    outrow['minlon'] = [min(df['longitude'])]
    outrow['maxlon'] = [max(df['longitude'])]
    outrow['minlat'] = [min(df['latitude'])]
    outrow['maxlat'] = [max(df['latitude'])]
    
    #TODO: there is work to be done here...debug
    # Bounding Ellipse
    try: 
        f = fit_ellipse(df['longitude'], df['latitude'])
    
        outrow['a_xx'] = [f[0]]
        outrow['a_xy'] = [f[1]]
        outrow['a_yy'] = [f[2]]
        outrow['a_x'] = [f[3]]
        outrow['a_y'] = [f[4]]
        outrow['a_1'] = [f[5]]
    
        c = ellipse_center(f)
    
        outrow['ell_center_x'] = [c[0]]
        outrow['ell_center_y'] = [c[1]]
    
        ab = ellipse_axis_length(f)

        outrow['ell_major'] = [max(ab)]
        outrow['ell_minor'] = [min(ab)]
     
    except np.linalg.LinAlgError as err:
        print("Singular matrix \n")
        print(df[['longitude', 'latitude']].head()) 

    # Line of Best Fit
    y = np.polyfit(df['longitude'], df['latitude'], deg=1)
    outrow['slope'] = [y[0]]
    outrow['intercept'] = [y[1]]
    
    #Path lengths
    outrow['count'] = [len(df)]
    outrow['direct_lon'] = [direct_lon]
    outrow['direct_lat'] = [direct_lat]
    outrow['direct'] = [np.sqrt(direct_lon ** 2 + direct_lat ** 2)]

    outrow['lonpath'] = [sum(abs(df['lon_lag']))]
    outrow['latpath'] = [sum(abs(df['lat_lag']))]
    
    outrow['curve_len'] = [sum(abs(df['ll_maglag']))]
    
    #Quant distributions
    outrow['maxspeed'] = [max(df['speed'])]
    outrow['meanspeed'] = [np.mean(df['speed'])]

    outrow['t_total'] = [sum(df['t_lag'])]
    outrow['t_lag_mean'] = [np.mean(df['t_lag'])]
    outrow['t_lag_sd'] = [np.std(df['t_lag'])]
    outrow['t_lag_max'] = [max(df['t_lag'])]
    #coerce types to real
    outrow = outrow.apply(lambda x: [np.real(y) for y in x])
    
    # get angle cts
    outrow['turn90'] = turn_90(df.angle_chg)
    outrow['turn30'] = turn_30(df.angle_chg)

    return outrow
