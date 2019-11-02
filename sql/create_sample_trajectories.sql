DROP TABLE IF EXISTS features.fishing_segments;
    CREATE TABLE features.fishing_segments AS (
        WITH fishing_segments AS (
            SELECT v.mmsi,
                   v.ais_vessel_type AS vessel_type,
                   a.time_stamp::DATE,
                   ST_MinimumBoundingCircle(ST_Collect(a.geom)) AS circle
            FROM cleaned.ais a
            INNER JOIN cleaned.vessels v
            ON a.mmsi = v.mmsi
            WHERE v.ais_vessel_type ~* 'Fishing'
            GROUP BY v.mmsi,
                     v.ais_vessel_type,
                     a.time_stamp::DATE
            HAVING count(*) > {min_pings} -- Removes trajectories with few pings analysis
            ) SELECT a.mmsi,
                     a.time_stamp,
                     a.latitude,
                     a.longitude,
                     ST_Length(ST_LongestLine(f.circle, f.circle)::geography) / 1000 AS span_km,
                     f.vessel_type
             FROM cleaned.ais a
        INNER JOIN fishing_segments f ON a.mmsi = f.mmsi
        AND a.time_stamp::DATE = f.time_stamp::DATE
        WHERE ST_Length(ST_LongestLine(f.circle, f.circle)::geography) / 1000 > 5.0 -- Removes short trajectories from analysis
        );
create index fishing_pings_idx on features.fishing_segments(mmsi);


DROP TABLE IF EXISTS features.nonfishing_segments;
    CREATE TABLE features.nonfishing_segments AS (
        WITH nonfishing_segments AS (
            SELECT v.mmsi,
                   v.ais_vessel_type AS vessel_type,
                   a.time_stamp::DATE,
                   ST_MinimumBoundingCircle(ST_Collect(a.geom)) AS circle
            FROM cleaned.ais a
            INNER JOIN cleaned.vessels v
            ON a.mmsi = v.mmsi
            WHERE v.ais_vessel_type != 'Fishing'
            GROUP BY v.mmsi,
                     v.ais_vessel_type,
                     a.time_stamp::DATE
            HAVING count(*) > {min_pings}
            ) SELECT a.mmsi,
                     a.time_stamp,
                     a.latitude,
                     a.longitude,
                     ST_Length(ST_LongestLine(f.circle, f.circle)::geography) / 1000 AS span_km,
                     f.vessel_type
            FROM cleaned.ais a
        INNER JOIN nonfishing_segments f ON a.mmsi = f.mmsi
        AND a.time_stamp::DATE = f.time_stamp::DATE
        WHERE ST_Length(ST_LongestLine(f.circle, f.circle)::geography) / 1000 > 5.0
        );
create index nonfishing_pings_idx on features.nonfishing_segments(mmsi);


DROP TABLE IF EXISTS features.cnn_sample;
CREATE TABLE features.cnn_sample AS (
    SELECT mmsi,
    time_stamp,
    latitude,
    longitude,
    vessel_type
    FROM features.fishing_segments
UNION
    SELECT mmsi,
    time_stamp,
    latitude,
    longitude,
    vessel_type
    FROM features.nonfishing_segments
);


/*
with segments as (select count(*)
from features.fishing_segments
group by mmsi, time_stamp::date)

select count(*) from segments;

drop table if exists features.images;
create table features.images(
    traj_id varchar primary key,
    mmsi varchar,
    day varchar,
    img int [],
    img_dimensions int[]
)
*/