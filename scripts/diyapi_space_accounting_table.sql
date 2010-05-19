/*****************************************************************************
* diyapi_space_accounting table
*****************************************************************************/
CREATE SEQUENCE diyapi_space_accounting_id;
CREATE TABLE diyapi_space_accounting(
   id INTEGER PRIMARY KEY DEFAULT nextval('diyapi_space_accounting_id'),
   avatar_id int4 NOT NULL,
   timestamp TIMESTAMP NOT NULL,
   bytes_added int8 NOT NULL DEFAULT 0,
   bytes_removed int8 NOT NULL DEFAULT 0,
   bytes_retrieved int8 NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX avatar_timestamp_idx 
ON diyapi_space_accounting (avatar_id, timestamp);

GRANT SELECT, INSERT, UPDATE, DELETE ON diyapi_space_accounting 
TO pandora_storage_server;

