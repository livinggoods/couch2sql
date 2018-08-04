-- -*- mode: sql; sql-product: ms; -*-

CREATE TABLE DISTRICT_HOSPITAL (
    ID                          NVARCHAR(4000) NOT NULL PRIMARY KEY,
    "NAME"                      NVARCHAR(4000) NOT NULL,
    CONTACT                     NVARCHAR(4000) REFERENCES PERSON (ID),
    NOTES                       NVARCHAR(4000),
    REPORTED_DATE               datetime,
    IS_MALARIA_ENDEMIC          BIT,
    IS_AMOXICILIN_APPROVED      BIT,
    IS_AMOXICILIN_DISPERSIBLE   BIT
);

INSERT INTO COUCHDB_TABLES (TABLE_NAME) VALUES ('DISTRICT_HOSPITAL');
