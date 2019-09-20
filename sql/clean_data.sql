select current_timestamp;

DROP TABLE if exists cleaned.ais;
CREATE TABLE cleaned.ais (
    mmsi            VARCHAR,
    time_stamp      TIMESTAMP WITH TIME ZONE,
    longitude       VARCHAR,
    latitude        VARCHAR,
    geom            GEOMETRY(Point, 4326),  -- this sets the GEOM COLUMN SRID AS 4326
--  geog            GEOGRAPHY,
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
    --  geog,
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


-- create indices
CREATE INDEX ais_temp_id_idx ON cleaned.ais(temp_id);
CREATE INDEX ais_mmsi_idx on cleaned.ais(mmsi);
CREATE INDEX ais_time_stamp_idx on cleaned.ais(time_stamp);


select current_timestamp;


DELETE FROM
cleaned.ais a
         USING cleaned.ais b
WHERE
    a.temp_id > b.temp_id
     AND a.mmsi = b.mmsi
     AND a.time_stamp = b.time_stamp;


select current_timestamp;


-- drop the temp id and make a real one
ALTER TABLE cleaned.ais
    DROP COLUMN temp_id,
    ADD COLUMN id SERIAL PRIMARY KEY;


-- create spatial index
CREATE INDEX ais_spatial_idx ON cleaned.ais USING gist(location);


-- import shapefiles of marine sanctuaries from https://www.protectedplanet.net/
DROP TABLE if exists cleaned.sanctuaries;
CREATE TABLE cleaned.sanctuaries (
    geom            GEOMETRY(MULTIPOLYGON, 4326),  -- This departs from raw rules, but eases import
    geog            GEOGRAPHY,
    wdpaid          VARCHAR,
    wdpa_pid        VARCHAR,
    pa_def          VARCHAR,
    name            VARCHAR,
    orig_name       VARCHAR,
    desig           VARCHAR,
    desig_eng       VARCHAR,
    desig_type      VARCHAR,
    iucn_cat        VARCHAR,
    int_crit        VARCHAR,
    marine          VARCHAR,
    rep_m_area      VARCHAR,
    gis_m_area      VARCHAR,
    rep_area        VARCHAR,
    gis_area        VARCHAR,
    no_take         VARCHAR,
    no_tk_area      VARCHAR,
    status          VARCHAR,
    status_yr       VARCHAR,
    gov_type        VARCHAR,
    own_type        VARCHAR,
    mang_auth       VARCHAR,
    mang_plan       VARCHAR,
    verif           VARCHAR,
    metadataid      VARCHAR,
    sub_loc         VARCHAR,
    parent_iso      VARCHAR,
    iso3            VARCHAR
);

INSERT INTO cleaned.sanctuaries
    SELECT
        ST_SetSRID(wkb_geometry, 4326) AS geom,
        wkb_geometry::geography as geog,
        wdpaid,
        wdpa_pid,
        pa_def,
        name,
        orig_name,
        desig,
        desig_eng,
        desig_type,
        iucn_cat,
        int_crit,
        marine,
        rep_m_area,
        gis_m_area,
        rep_area,
        gis_area,
        no_take,
        no_tk_area,
        status,
        status_yr,
        gov_type,
        own_type,
        mang_auth,
        mang_plan,
        verif,
        metadataid,
        sub_loc,
        parent_iso,
        iso3
    FROM raw.sanctuaries
;
CREATE INDEX sanct_spatial_idx ON cleaned.sanctuaries USING gist(geom);