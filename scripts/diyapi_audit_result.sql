/*****************************************************************************
* diyapi_audit_result table
*****************************************************************************/
CREATE SEQUENCE diyapi_audit_result_id_sequence;
CREATE TABLE diyapi_audit_result(
   diyapi_audit_result_id int4 NOT NULL 
   PRIMARY KEY DEFAULT NEXTVAL('diyapi_audit_result_id_sequence'),
   avatar_id int4 NOT NULL,
   state TEXT,
   audit_scheduled TIMESTAMP DEFAULT now(),
   audit_started TIMESTAMP,
   audit_finished TIMESTAMP,
   reconstruct_scheduled TIMESTAMP,
   reconstruct_started TIMESTAMP,
   reconstruct_finished TIMESTAMP
);

CREATE INDEX diyapi_audit_result_avatar_id ON diyapi_audit_result (avatar_id);

GRANT SELECT, UPDATE ON diyapi_audit_result_id_sequence TO diyapi_auditor;
GRANT SELECT, INSERT, UPDATE, DELETE ON diyapi_audit_result 
TO diyapi_auditor;
