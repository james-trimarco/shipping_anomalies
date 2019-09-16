WITH time_ordered AS (
    SELECT
        mmsi,
        time_stamp,
        location::geography AS geom
    FROM
        cleaned.ais
    ORDER BY
        mmsi,
        time_stamp
),
adds_previous AS (
    SELECT
        mmsi,
        time_stamp,
        geom,
        lag(geom) OVER (PARTITION BY mmsi ORDER BY time_stamp) AS prev_geom
    FROM
        time_ordered
)
SELECT
    mmsi,
    time_stamp,
    st_distance (geom, prev_geom)
FROM
    adds_previous
WHERE
    mmsi='204206760'
LIMIT 10;


-- Finds ping count by length for each boat
DROP TABLE if exists eda.ping_count_by_length;
CREATE TABLE eda.ping_count_by_length AS
  (SELECT mmsi,
          length,
          Count(*)
   FROM   cleaned.ais
   WHERE  length != 0  --  removes rows with no value here
   GROUP  BY mmsi,
             length);

with pings_by_ship as (
SELECT
    mmsi,
    navstat,
    count(*) as ping_count
    from cleaned.ais
    group by mmsi, navstat),
quantiles as (
SELECT navstat,
       max(ping_count) as max_ping,
       ntile(4) over (order by ping_count) as quantile
       from pings_by_ship
       group by navstat, max(ping_count))
select max_ping,
       quantile,
       navstat
       from quantiles
       group by quantile;

-- Creates boxplot data for pings by navstat category
DROP TABLE if exists eda.mean_pings_by_navstat;
create table eda.mean_pings_by_navstat as (
with pings_by_ship as
( SELECT
    mmsi,
    navstat,
    count(*) as ping_count
    from cleaned.ais
    group by mmsi, navstat
    ),
pings2 as (
SELECT navstat,
       ping_count,
       NTILE(4) OVER (PARTITION BY navstat ORDER BY ping_count) AS ntile4
       FROM pings_by_ship),
pings3 AS (
    SELECT navstat,
        -- Row count for navstat:
        COUNT(*) AS count,
        -- Highest value of tile 1:
        MAX((CASE WHEN ntile4=2 THEN ping_count END)) AS max_ntile2,
        -- Lowest value of tile 2:
        MIN((CASE WHEN ntile4=3 THEN ping_count END)) AS min_ntile3,
        -- Highest value
        MAX((CASE WHEN ntile4=1 THEN ping_count END)) AS max_ntile1,
        -- Lowest value of tile 2:
        MIN((CASE WHEN ntile4=2 THEN ping_count END)) AS min_ntile2,
        -- Highest value
        MAX((CASE WHEN ntile4=3 THEN ping_count END)) AS max_ntile3,
        -- Lowest value of tile 2:
        MIN((CASE WHEN ntile4=4 THEN ping_count END)) AS min_ntile4
    FROM pings2
    GROUP BY navstat)
SELECT navstat, (CASE count%2
    --- Odd number of rows
    WHEN 1 THEN max_ntile1
    --- Even number of rows
    ELSE (max_ntile1+min_ntile2)/2.0 END) AS median_ping_count,
    (max_ntile1 + min_ntile3)/2.0 AS q1,
    (max_ntile3 + min_ntile4)/2.0 AS q3,
    count
FROM pings3
ORDER BY navstat
);


