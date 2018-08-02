-- -*- mode: sql; sql-product: ms; -*-

CREATE TABLE COUCHDB_REPLICATION (
    LAST_SEQ    NVARCHAR(4000)
);
GO

DECLARE @CurrentUser sysname;
SELECT @CurrentUser = user_name();

EXECUTE sp_addextendedproperty 'MS_Description', 
   'Tracks status of CouchDB replication',
   'user', @CurrentUser, 'table', 'COUCHDB_REPLICATION'
    
EXECUTE sp_addextendedproperty 'MS_Description', 
   'This is the _changes sequence number of the most recently loaded change',
   'user', @CurrentUser, 'table', 'COUCHDB_REPLICATION', 'column', 'LAST_SEQ'
GO

CREATE TABLE COUCHDB_IGNORED_REVISIONS (
    ID          NVARCHAR(4000),
    REV         NVARCHAR(4000),
    PRIMARY KEY (ID, REV)
);
GO

DECLARE @CurrentUser sysname;
SELECT @CurrentUser = user_name();

EXECUTE sp_addextendedproperty 'MS_Description', 
   'Lists records to ignore from CouchDB (because they are malformed for example). Fix bad records over there, then come here to get replication to resume.',
   'user', @CurrentUser, 'table', 'COUCHDB_IGNORED_REVISIONS'
GO
    
CREATE TABLE COUCHDB_TABLES (
    ID          INT IDENTITY PRIMARY KEY,
    TABLE_NAME  NVARCHAR(4000) NOT NULL,
    ID_COLUMN   NVARCHAR(4000) NOT NULL
);
GO

DECLARE @CurrentUser sysname;
SELECT @CurrentUser = user_name();

EXECUTE sp_addextendedproperty 'MS_Description',
   'This is just an inventory of tables with CouchDB data. It is needed to support COUCHDB_IDS.',
   'user', @CurrentUser, 'table', 'COUCHDB_TABLES'
GO
    
CREATE TABLE COUCHDB_IDS (
    ID          NVARCHAR(4000) NOT NULL,
    "TABLE"     INT REFERENCES COUCHDB_TABLES (ID) NOT NULL,
    PRIMARY KEY (ID, "TABLE")
);
GO

DECLARE @CurrentUser sysname;
SELECT @CurrentUser = user_name();

EXECUTE sp_addextendedproperty 'MS_Description',
   'This table tells us where to delete from when a record is removed from CouchDB.',
   'user', @CurrentUser, 'table', 'COUCHDB_IDS'
GO
