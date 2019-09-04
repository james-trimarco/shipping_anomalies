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