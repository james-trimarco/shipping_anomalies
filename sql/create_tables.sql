DROP TABLE if exists raw.ais;
CREATE TABLE raw.ais (
    MMSI        VARCHAR,
    TIME        VARCHAR,
    LONGITUDE   VARCHAR,
    LATITUDE    VARCHAR,
    COG         VARCHAR,
    SOG         VARCHAR,
    HEADING     VARCHAR,
    ROT         VARCHAR,
    NAVSTAT     VARCHAR,
    IMO         VARCHAR,
    NAME        VARCHAR,
    CALLSIGN    VARCHAR,
    TYPE        VARCHAR,
    A           VARCHAR,
    B           VARCHAR,
    C           VARCHAR,
    D           VARCHAR,
    DRAUGHT     VARCHAR,
    DEST        VARCHAR,
    ETA         VARCHAR
);

-- import shapefiles of marine sanctuaries from https://www.protectedplanet.net/
DROP TABLE if exists raw.sanctuaries;
CREATE TABLE raw.sanctuaries (
    wkb_geometry    GEOMETRY,  -- This departs from raw rules, but eases import
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

