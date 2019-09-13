DROP TABLE if exists cleaned.ais;
CREATE TABLE cleaned.ais (
    mmsi            VARCHAR,
    time_stamp      TIMESTAMP WITH TIME ZONE,
    longitude       VARCHAR,
    latitude        VARCHAR,
    location        GEOMETRY(Point, 4326),  -- this sets the GEOM COLUMN SRID AS 4326
    cog             FLOAT,
    sog             FLOAT,
    heading         INT,
    rot             INT,
    navstat         VARCHAR,
    imo             VARCHAR,
    name            VARCHAR,
    callsign        VARCHAR,
    type            VARCHAR,
    length          INT,
    width           INT,
    draught         FLOAT,
    dest            VARCHAR,
    eta             TIMESTAMP
);

INSERT INTO cleaned.ais
    SELECT
        mmsi,
        time::timestamp with time zone at time zone 'GMT' AS time_stamp,
        longitude::float,
        latitude::float,
        ST_SetSRID(ST_MakePoint(longitude::float, latitude::float), 4326) AS location,  -- this sets the GEOM COLUMN SRID AS 4326
        cog::float,
        sog::float,
        heading::int,
        rot::int,
        navstat,
        imo,
        name,
        callsign,
        type,
        a::int + b::int AS length,
        c::int + d::int AS width,
        draught::float,
        dest,
        TO_TIMESTAMP(eta, 'MM-DD HH24:MI') AS eta
        FROM raw.ais
;

-- create temporary id to aid in deletion of duplicates
ALTER TABLE cleaned.ais
    ADD COLUMN temp_id SERIAL;

-- remove all rows where one boat pings at the same time
DELETE  FROM
    cleaned.ais a
        USING cleaned.ais b
WHERE
    a.temp_id > b.temp_id
    AND a.mmsi = b.mmsi
    AND a.time_stamp = b.time_stamp;

-- drop the temp id and make a real one
ALTER TABLE cleaned.ais
    DROP COLUMN temp_id;
    ADD COLUMN id SERIAL PRIMARY KEY;

-- create indices
CREATE INDEX ais_spatial_idx ON cleaned.ais USING gist(location);