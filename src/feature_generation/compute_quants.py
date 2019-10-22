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


def give_angle(v_y, v_x):
    if v_x > 0:
        return np.arctan(v_y / v_x) * 180 / np.pi
    elif v_x < 0:
        return 180 + np.arctan(v_y / v_x) * 180 / np.pi
    else:
        if v_y == 0:
            return 0
        else:
            if v_y > 0:
                return 90
            else:
                return 270


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

    # First convert initial rows to a form that includes velocity
    df = df.sort_index()
    #TODO: decide on .dropna()
    df_diff = df.diff().dropna()
    # Set NA values on first step to 0, but set first time value to 1 for division (set back to 0 later for distributions)
    #TODO: pd.Timedelta(0),etc.
    # df_diff.loc[0] = 0
    # df_diff['time_stamp'][0] = 1
    df['t_lag'] = df_diff['time_stamp']
    df['lat_lag'] = df_diff['latitude']
    df['v_lat'] = df_diff['latitude'] / (df_diff['time_stamp'].dt.total_seconds() )
    df['lon_lag'] = df_diff['longitude']
    df['v_lon'] = df_diff['longitude'] / (df_diff['time_stamp'].dt.total_seconds() )
    
    #TODO: consider pd.Timedelta(0),etc.
    #df['t_lag'][0] = 0
    #df['lat_lag'][0] = 0
    #df['lon_lag'][0] = 0

    df['ll_magn'] = np.sqrt(df['lon_lag'] ** 2 + df['lat_lag'] ** 2)

    # Rows are now in form ['time','lat','lon','t_lag','lat_lag','v_lat','lon_lag','v_lon','ll_magn']
    # Add the angle of the boat at that time via tan^-1(v_lat/v_lon)
    
    #TODO: decide on .dropna()
    df = df.dropna()i

    #TODO: attempt to accomplish reduce syntax on give_angle for good form
    # df['angle'] = give_angle(df['v_lat'], df['v_lon'])
    df['angle'] = df.apply(lambda x: give_angle(x.v_lat, x.v_lon), axis = 1)
    # Rows now have 'angle' term at the end
    
    #TODO: rename df_diff to express that this is only for angle differencing
    df_diff = df.diff()
    df_diff.loc[0] = 0
    
    #TODO: consider moving addition of linear speed to somewhere it makes more sense
    # add angle changes (how big the turn was, in degrees) and speed to each row
    #TODO: add angular speeds as new field in df same way as before
    df['angle_chg'] = df_diff['angle']
    df['speed'] = np.sqrt(df['v_lat'] ** 2 + df['v_lon'] ** 2)

    # now we will add the features we are interested in to a single pandas row

    # Directness Ratio calculations
    direct_lon = df['longitude'][-1] - df['longitude'][0]
    direct_lat = df['latitude'][-1] - df['latitude'][0]

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
                 'ell_center_x', 'ell_center_y', 'ell_major', 'ell_minor', 'slope', 'intercept', 
                 'direct_lon', 'direct_lat', 'direct', 'lonpath', 'latpath', 'curve_len', 'maxspeed', 
                 'meanspeed', 'turn90', 'turn30'])
    outrow = outrow.astype(np.float)

    outrow.loc[0, 'minlon'] = min(df['longitude'])
    outrow.loc[0, 'maxlon'] = max(df['longitude'])
    outrow.loc[0, 'minlat'] = min(df['latitude'])
    outrow.loc[0, 'maxlat'] = max(df['latitude'])

    # Bounding Ellipse
    try: 
        f = fit_ellipse(df['longitude'], df['latitude'])

        outrow.loc[0, 'a_xx'] = f[0]
        outrow.loc[0, 'a_xy'] = f[1]
        outrow.loc[0, 'a_yy'] = f[2]
        outrow.loc[0, 'a_x'] = f[3]
        outrow.loc[0, 'a_y'] = f[4]
        outrow.loc[0, 'a_1'] = f[5]

        c = ellipse_center(f)

        outrow.loc[0, 'ell_center_x'] = c[0]
        outrow.loc[0, 'ell_center_y'] = c[1]

        ab = ellipse_axis_length(f)
        print(ab)
        outrow.loc[0, 'ell_major'] = max(ab)
        outrow.loc[0, 'ell_minor'] = min(ab)
     
    except np.linalg.LinAlgError as err:
        print("Singular matrix \n")
        print(df[['longitude', 'latitude']].head()) 

    # Line of Best Fit
    y = np.polyfit(df['longitude'], df['latitude'], deg=1)

    outrow.loc[0, 'slope'] = y[0]
    outrow.loc[0, 'intercept'] = y[1]

    outrow.loc[0, 'direct_lon'] = direct_lon
    outrow.loc[0, 'direct_lat'] = direct_lat
    outrow.loc[0, 'direct'] = np.sqrt(direct_lon ** 2 + direct_lat ** 2)

    outrow.loc[0, 'lonpath'] = sum(abs(df['lon_lag']))
    outrow.loc[0, 'latpath'] = sum(abs(df['lat_lag']))
    outrow.loc[0, 'curve_len'] = sum(df['ll_magn'])

    outrow.loc[0, 'maxspeed'] = max(df['speed'])
    outrow.loc[0, 'meanspeed'] = np.mean(df['speed'])

    outrow = outrow.apply(lambda x: np.real(x))
    # get angle cts
    #anglects = list(df.agg({'angle_chg': ['turn_90', 'turn_30']}))

    #outrow['turn90'] = anglects[0]
    #outrow['turn30'] = anglects[1]

    return outrow
