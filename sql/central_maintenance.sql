BEGIN;

/* summerize everything older than the last 7 days by the resolution of 1
 * hour and across all nodes. move it to the collection_ops_accounting_old
 * table. */
INSERT INTO collection_ops_accounting_old 
SELECT collection_id,
       date_trunc('hour', timestamp) AS timestamp,
       '1 hour'::interval AS duration,
       sum(get_request),
       sum(get_success),
       sum(get_error),
       sum(put_request),
       sum(put_success),
       sum(put_error),
       sum(listmatch_request),
       sum(listmatch_success),
       sum(listmatch_error),
       sum(delete_request),
       sum(delete_success),
       sum(delete_error),
       sum(socket_bytes_in),
       sum(socket_bytes_out),
       sum(success_bytes_in),
       sum(success_bytes_out),
       sum(error_bytes_in),
       sum(error_bytes_out)
  FROM collection_ops_accounting
 WHERE timestamp < date_trunc('day', 
                              current_timestamp - '7 days'::interval)
 GROUP BY collection_id, timestamp, duration
 ORDER BY collection_id, timestamp, duration;

/* remove the rows we just summerized into the old table */
DELETE FROM collection_ops_accounting 
 WHERE timestamp < date_trunc('day', 
                              current_timestamp - '7 days'::interval);

ROLLBACK;

/* avoid this table getting too large. We shouldn't go more than 3 days
 * between a crash and the next run of the collection/flush program!  This
 * table is going to get 1 row per minute per storage node per key
 * (currently 17 keys)  */
BEGIN;
DELETE FROM collection_ops_accounting_flush_dedupe
 WHERE age(timestamp) > '3 days'::interval;
ROLLBACK;
