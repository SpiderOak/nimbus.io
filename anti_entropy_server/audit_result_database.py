# -*- coding: utf-8 -*-
"""
audit_result_database.py

wrap access to the nimbusio_central.audit_result_table 
"""
import logging

state_audit_started = "audit-started"
state_audit_waiting_retry = "audit-waiting-retry"
state_audit_too_many_retries = "audit-too-many-retries"
state_audit_successful = "audit-successful"
state_audit_error = "audit-error"

_start_audit_command = """
INSERT INTO nimbusio_central.audit_result
(collection_id, state, audit_started)
VALUES(%s, %s, %s::timestamp);
""".strip()

_audit_retry_command = """
UPDATE nimbusio_central.audit_result
SET state = %s, audit_started = NULL
WHERE nimbusio_central.audit_result_id = %s;
""".strip()

_restart_audit_command = """
UPDATE nimbusio_central.audit_result
SET state = %s, audit_started = %s::timestamp
WHERE nimbusio_central.audit_result_id = %s;
""".strip()

_audit_result_command = """
UPDATE nimbusio_central.audit_result
SET state = %s, audit_finished = %s::timestamp
WHERE nimbusio_central.audit_result_id = %s;
""".strip()

_ineligible_query = """
SELECT collection_id FROM nimbusio_central.audit_result
WHERE COALESCE(state, 'audit-successful') != 'audit-successful'
OR COALESCE(audit_finished, now()) > %s::timestamp;
""".strip()

_clear_command = """
DELETE FROM nimbusio_central.audit_result
WHERE collection_id = %s
""".strip()

class AuditResultDatabase(object):
    """wrap access to the nimbusio_central.audit_result table"""
    def __init__(self, central_connection):
        self._log = logging.getLogger("AuditResultDatabase")
        self._connection = central_connection

    def close(self):
        """commit the updates and close the database connection"""
        pass

    def start_audit(self, collection_id, timestamp):
        """insert a row to mark the start of an audit"""
        row_id = self._connection.execute(
            _start_audit_command, 
            [collection_id, state_audit_started, timestamp, ]
        )

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

    def ineligible_collection_ids(self, cutoff_timestamp):
        """return a list of collection_ids that are NOT eligible for audit"""
        result = self._connection.fetch_all_rows(
            _ineligible_query, [cutoff_timestamp, ]
        )
        return [collection_id for (collection_id, ) in result]

    def _clear_collection(self, collection_id):
        """for testing: remove all rows for an collection"""
        self._connection.execute(_clear_command, [collection_id, ])
        self._connection.commit()

