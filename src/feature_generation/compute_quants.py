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


def fitEllipse(x, y):
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


def cluster_metrics(trajpd):
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
    trajpd = trajpd.sort_values('time')
    trajpd_diff = trajpd.diff()

    # Set NA values on first step to 0, but set first time value to 1 for division (set back to 0 later for distributions)
    trajpd_diff.loc[0] = 0
    trajpd_diff['time'][0] = 1

    trajpd['t_lag'] = trajpd_diff['time']
    trajpd['lat_lag'] = trajpd_diff['lat']
    trajpd['v_lat'] = trajpd_diff['lat'] / trajpd_diff['time']
    trajpd['lon_lag'] = trajpd_diff['lon']
    trajpd['v_lon'] = trajpd_diff['lon'] / trajpd_diff['time']

    trajpd['t_lag'][0] = 0
    trajpd['lat_lag'][0] = 0
    trajpd['lon_lag'][0] = 0

    trajpd['ll_magn'] = np.sqrt(trajpd['lon_lag'] ** 2 + trajpd['lat_lag'] ** 2)

    # Rows are now in form ['time','lat','lon','t_lag','lat_lag','v_lat','lon_lag','v_lon','ll_magn']
    # Add the angle of the boat at that time via tan^-1(v_lat/v_lon)

    trajpd['angle'] = give_angle(trajpd['v_lat'], trajpd['v_lon'])

    # Rows now have 'angle' term at the end

    trajpd_diff = trajpd.diff()
    trajpd_diff.loc[0] = 0

    # add angle changes (how big the turn was, in degrees) and speed to each row
    trajpd['angle_chg'] = trajpd_diff['angle']
    trajpd['speed'] = np.sqrt(trajpd['v_lat'] ** 2 + trajpd['v_lon'] ** 2)

    # now we will add the features we are interested in to a single pandas row

    # Directness Ratio calculations
    direct_lon = trajpd['lon'][-1] - trajpd['lon'][0]
    direct_lat = trajpd['lat'][-1] - trajpd['lat'][0]

    # 90+ Degree Turns
    def turn_obtuse(z):
        obtuse_map = map(lambda x: 1 if x > 90 else 0, z)
        return reduce(lambda x, y: x + y, obtuse_map)

    # 30+ Degree Turns
    def turn_30(z):
        map_30 = map(lambda x: 1 if x > 30 else 0, z)
        return reduce(lambda x, y: x + y, map_30)

    outrow = pd.DataFrame(
        columns=['MMSI', 'TIME', 'MINLON', 'MAXLON', 'MINLAT', 'MAXLAT', 'a_xx', 'a_xy', 'a_yy', 'a_x', 'a_y', 'a_1',
                 'ell_center_x', 'ell_center_y', 'ell_major', 'ell_minor', 'b', 'm', 'direct_lon', 'direct_lat',
                 'direct', 'lonpath', 'latpath', 'curve_len', 'maxspeed', 'meanspeed', 'obtuse', 'turn30'])

    outrow['MMSI'] = trajpd['MMSI']
    outrow['TIME'] = trajpd['time_chunk']
    outrow['MINLON'] = min(trajpd['lon'])
    outrow['MAXLON'] = max(trajpd['lon'])
    outrow['MINLAT'] = min(trajpd['lat'])
    outrow['MAXLAT'] = max(trajpd['lat'])

    # Bounding Ellipse
    f = fitEllipse(trajpd['lon'], trajpd['lat'])

    outrow['a_xx'] = f[0]
    outrow['a_xy'] = f[1]
    outrow['a_yy'] = f[2]
    outrow['a_x'] = f[3]
    outrow['a_y'] = f[4]
    outrow['a_1'] = f[5]

    c = ellipse_center(f)

    outrow['ell_center_x'] = c[0]
    outrow['ell_center_y'] = c[1]

    ab = ellipse_axis_length(f)

    outrow['ell_major'] = max(ab)
    outrow['ell_minor'] = min(ab)

    # Line of Best Fit
    y = np.polyfit(trajpd['lon'], trajpd['lat'], deg=1)

    outrow['b'] = y[0]
    outrow['m'] = y[1]

    outrow['direct_lon'] = direct_lon
    outrow['direct_lat'] = direct_lat
    outrow['direct'] = np.sqrt(direct_lon ** 2 + direct_lat ** 2)

    outrow['lonpath'] = sum(abs(trajpd['lon_lag']))
    outrow['latpath'] = sum(abs(trajpd['lat_lag']))
    outrow['curve_len'] = sum(trajpd['ll_magn'])

    outrow['maxspeed'] = max(trajpd['speed'])
    outrow['meanspeed'] = np.mean(trajpd['speed'])

    # get angle cts
    anglects = list(trajpd.agg({'angle_chg': ['obtuse', 'turn30']}))

    outrow['obtuse'] = anglects[0]
    outrow['turn30'] = anglects[1]

    return outrow
