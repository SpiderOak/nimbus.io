# -*- coding: utf-8 -*-
"""
audit_result_database.py

wrap access to the diyapi_audit_result_table 
"""
import logging
import os

from diyapi_tools.pandora_database_connection import get_database_connection

state_audit_started = "audit-started"
state_audit_waiting_retry = "audit-waiting-retry"
state_audit_too_many_retries = "audit-too-many-retries"
state_audit_successful = "audit-successful"
state_audit_error = "audit-error"

_database_user = "diyapi_auditor"
_database_password = os.environ['PANDORA_DB_PW_diyapi_auditor']

_start_audit_command = """
INSERT INTO diyapi_audit_result
(avatar_id, state, audit_started)
VALUES(%s, '%s', '%s'::timestamp)
RETURNING diyapi_audit_result_id;
""".strip()

_audit_retry_command = """
UPDATE diyapi_audit_result
SET state = '%s', audit_started = NULL
WHERE diyapi_audit_result_id = %s;
""".strip()

_restart_audit_command = """
UPDATE diyapi_audit_result
SET state = '%s', audit_started = '%s'::timestamp
WHERE diyapi_audit_result_id = %s;
""".strip()

_audit_result_command = """
UPDATE diyapi_audit_result
SET state = '%s', audit_finished = '%s'::timestamp
WHERE diyapi_audit_result_id = %s;
""".strip()

_ineligible_query = """
SELECT avatar_id FROM diyapi_audit_result
WHERE COALESCE(state, 'audit-successful') != 'audit-successful'
OR COALESCE(audit_finished, now()) > '%s'::timestamp;
""".strip()

_clear_command = """
DELETE FROM diyapi_audit_result
WHERE avatar_id = %s
""".strip()

class AuditResultDatabase(object):
    """wrap access to the diyapi_audit_result table"""
    def __init__(self):
        self._log = logging.getLogger("AuditResultDatabase")
        self._connection = get_database_connection(
            user=_database_user, password=_database_password
        )

    def close(self):
        """commit the updates and close the database connection"""
        self._connection.close()

    def start_audit(self, avatar_id, timestamp):
        """insert a row to mark the start of an audit"""
        command = _start_audit_command % (
            avatar_id, state_audit_started, timestamp, 
        )
        (row_id, ) = self._connection.fetch_one_row(command)
        self._connection.commit()
        return row_id

    def wait_for_retry(self, row_id):
        """update a row to mark waiting for retry"""
        command = _audit_retry_command % (
            state_audit_waiting_retry, row_id, 
        )
        self._connection.execute(command)
        self._connection.commit()

    def too_many_retries(self, row_id):
        """update a row to mark too many retries"""
        command = _audit_retry_command % (
            state_audit_too_many_retries, row_id, 
        )
        self._connection.execute(command)
        self._connection.commit()

    def restart_audit(self, row_id, timestamp):
        """update a row to mark waiting for retry"""
        command = _restart_audit_command % (
            state_audit_started, timestamp, row_id,  
        )
        self._connection.execute(command)
        self._connection.commit()

    def successful_audit(self, row_id, timestamp):
        """update a row to mark success of an audit"""
        command = _audit_result_command % (
            state_audit_successful, timestamp, row_id, 
        )
        self._connection.execute(command)
        self._connection.commit()

    def audit_error(self, row_id, timestamp):
        """update a row to mark failure of an audit"""
        command = _audit_result_command % (
            state_audit_error, timestamp, row_id, 
        )
        self._connection.execute(command)
        self._connection.commit()

    def ineligible_avatar_ids(self, cutoff_timestamp):
        """return a list of avatar_ids that are NOT eligible for audit"""
        command = _ineligible_query % (cutoff_timestamp, )
        result = self._connection.fetch_all_rows(command)
        return [avatar_id for (avatar_id, ) in result]

    def _clear_avatar(self, avatar_id):
        """for testing: remove all rows for an avatar"""
        command = _clear_command % (avatar_id, )
        self._connection.execute(command)
        self._connection.commit()

if __name__ == "__main__":
    import datetime
    avatar_id = 1001
    start_time = datetime.datetime.now()
    restart_time = start_time + datetime.timedelta(minutes=1)
    finished_time = start_time + datetime.timedelta(minutes=3)
    audit_time = start_time + datetime.timedelta(minutes=2)

    print
    print "testing"

    audit_result_database = AuditResultDatabase()
    audit_result_database._clear_avatar(avatar_id)

    ineligible_list = audit_result_database.ineligible_avatar_ids(audit_time)
    assert len(ineligible_list) == 0

    row_id = audit_result_database.start_audit(avatar_id, start_time)
    print "row_id =", row_id

    audit_result_database.wait_for_retry(row_id)

    audit_result_database.restart_audit(row_id, restart_time)
    
    audit_result_database.successful_audit(row_id, finished_time)

    ineligible_list = audit_result_database.ineligible_avatar_ids(audit_time)
    print "ineligible", ineligible_list

    audit_result_database._clear_avatar(avatar_id)
    audit_result_database.close()
    print "test complete"

