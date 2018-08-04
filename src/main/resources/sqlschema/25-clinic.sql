-- -*- mode: sql; sql-product: ms; -*-

CREATE TABLE CLINIC (
    ID                      NVARCHAR(4000) NOT NULL PRIMARY KEY,
    "NAME"                  NVARCHAR(4000) NOT NULL,
    NOTES                   NVARCHAR(4000),
    HEALTH_CENTER           NVARCHAR(4000) REFERENCES HEALTH_CENTER,
    CONTACT                 NVARCHAR(4000) REFERENCES PERSON (ID),
    SOLAR_LIGHT             BIT,
    WATER_FILTER            BIT,
    CHILDREN_UNDER_5        TINYINT,
    IMPROVED_COOK_STOVE     BIT,
    LLIN                    TINYINT,
    LATRINE                 BIT,
    HOW_WATER_TREATED       NVARCHAR(4000),
    HAND_WASHING_FACILITIES BIT,
    LATITUDE                FLOAT,
    LONGITUDE               FLOAT,
    PHONE                   NVARCHAR(4000),
    IMPORTED_DATE           DATETIME,
    REPORTED_DATE           DATETIME,
);

INSERT INTO COUCHDB_TABLES (TABLE_NAME) VALUES ('CLINIC');
