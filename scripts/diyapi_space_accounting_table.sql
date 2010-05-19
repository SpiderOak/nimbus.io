/*****************************************************************************
* diyapi_space_accounting table
*****************************************************************************/
CREATE TABLE diyapi_space_accounting(
   avatar_id int4 NOT NULL,
   timestamp TIMESTAMP NOT NULL,
   bytes_added int8 NOT NULL DEFAULT 0,
   bytes_removed int8 NOT NULL DEFAULT 0,
   bytes_retrieved int8 NOT NULL DEFAULT 0
);

GRANT SELECT, INSERT, UPDATE, DELETE ON diyapi_space_accounting 
TO pandora_storage_server;

