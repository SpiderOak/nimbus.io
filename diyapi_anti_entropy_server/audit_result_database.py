# -*- coding: utf-8 -*-
"""
audit_result_database.py

wrap access to the diy_central.audit_result_table 
"""
import logging

state_audit_started = "audit-started"
state_audit_waiting_retry = "audit-waiting-retry"
state_audit_too_many_retries = "audit-too-many-retries"
state_audit_successful = "audit-successful"
state_audit_error = "audit-error"

_start_audit_command = """
INSERT INTO diy_central.audit_result
(avatar_id, state, audit_started)
VALUES(%s, %s, %s::timestamp);
""".strip()

_audit_retry_command = """
UPDATE diy_central.audit_result
SET state = %s, audit_started = NULL
WHERE diy_central.audit_result_id = %s;
""".strip()

_restart_audit_command = """
UPDATE diy_central.audit_result
SET state = %s, audit_started = %s::timestamp
WHERE diy_central.audit_result_id = %s;
""".strip()

_audit_result_command = """
UPDATE diy_central.audit_result
SET state = %s, audit_finished = %s::timestamp
WHERE diy_central.audit_result_id = %s;
""".strip()

_ineligible_query = """
SELECT avatar_id FROM diy_central.audit_result
WHERE COALESCE(state, audit-successful) != audit-successful
OR COALESCE(audit_finished, now()) > %s::timestamp;
""".strip()

_clear_command = """
DELETE FROM diy_central.audit_result
WHERE avatar_id = %s
""".strip()

class AuditResultDatabase(object):
    """wrap access to the diy_central.audit_result table"""
    def __init__(self, central_connection):
        self._log = logging.getLogger("AuditResultDatabase")
        self._connection = central_connection
    def close(self):
        """commit the updates and close the database connection"""
        self._connection.close()

    def start_audit(self, avatar_id, timestamp):
        """insert a row to mark the start of an audit"""
        cursor = self._connection._connection.cursor()
        cursor.execute(
            _start_audit_command, 
            [avatar_id, state_audit_started, timestamp, ]
        )
        row_id = cursor.lastrowid
        cursor.close()

        self._connection.commit()
        return row_id

    def wait_for_retry(self, row_id):
        """update a row to mark waiting for retry"""
        self._connection.execute(
            _audit_retry_command,
            [state_audit_waiting_retry, row_id, ]
        )
        self._connection.commit()

    def too_many_retries(self, row_id):
        """update a row to mark too many retries"""
        self._connection.execute(
            _audit_retry_command,
            [state_audit_too_many_retries, row_id, ]
        )
        self._connection.commit()

    def restart_audit(self, row_id, timestamp):
        """update a row to mark waiting for retry"""
        self._connection.execute(
            _restart_audit_command,
            [state_audit_started, timestamp, row_id, ]
        )
        self._connection.commit()

    def successful_audit(self, row_id, timestamp):
        """update a row to mark success of an audit"""
        self._connection.execute(
            _audit_result_command,
            [state_audit_successful, timestamp, row_id, ]
        )
        self._connection.commit()

    def audit_error(self, row_id, timestamp):
        """update a row to mark failure of an audit"""
        self._connection.execute(
            _audit_result_command,
            [state_audit_error, timestamp, row_id, ]
        )
        self._connection.commit()

    def ineligible_avatar_ids(self, cutoff_timestamp):
        """return a list of avatar_ids that are NOT eligible for audit"""
        result = self._connection.fetch_all_rows(
            _ineligible_query, [cutoff_timestamp, ]
        )
        return [avatar_id for (avatar_id, ) in result]

    def _clear_avatar(self, avatar_id):
        """for testing: remove all rows for an avatar"""
        self._connection.execute(_clear_command, [avatar_id, ])
        self._connection.commit()

