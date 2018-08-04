-- -*- mode: sql; sql-product: ms; -*-

CREATE TABLE HEALTH_CENTER (
    ID                          NVARCHAR(4000) PRIMARY KEY,
    "NAME"                      NVARCHAR(4000),
    CONTACT                     NVARCHAR(4000) REFERENCES PERSON (ID),
    DISTRICT_HOSPITAL           NVARCHAR(4000) REFERENCES DISTRICT_HOSPITAL,
    REPORTED_DATE               datetime,
    IMPORTED_DATE               datetime,
    WARD                        NVARCHAR(4000),
    PARISH                      NVARCHAR(4000),
    VILLAGE                     NVARCHAR(4000),
    DISTRICT                    NVARCHAR(4000),
    HSD                         NVARCHAR(4000),
    SUB_COUNTY                  NVARCHAR(4000),
    SUB_DISTRICT                NVARCHAR(4000),
    COMMUNITY_UNIT              NVARCHAR(4000),
    LANDMARK                    NVARCHAR(4000),
    HEALTH_FACILITY             NVARCHAR(4000),
    LINK_FACILITY               NVARCHAR(4000),
    EXTERNAL_ID                 NVARCHAR(4000),
    FACILITY_ID                 integer
);

INSERT INTO COUCHDB_TABLES (TABLE_NAME) VALUES ('HEALTH_CENTER');
