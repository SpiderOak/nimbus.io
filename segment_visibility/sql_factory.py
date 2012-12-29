"""
SQL generation library for segment/conjoined garbage collection and other
things that need to be aware of the garbage or versioned status of rows. 
Supports scenarios such as:

  Listing rows that are elegible to be garbage collected
  Getting the most recent and available version of a key
  Getting a particular available version of a key
  Listing the newest version for all keys (with limit and key marker)
  Listing all versions (with limit, key marker, version marker)

The public functions here return an SQL query.  It's expected that you'll then
run that query using keyword based paramater binding (in other words, the
values of the parameters are not directly added to the generated SQL.)  Callers
should bind with a parameter dictionary with the same names as the names of the
arguments used when calling the SQL generation function.

An important detail is the definable parameter _max_handoff_time.  This
basically defines an upper bound for how long we expect a handoff might take,
and therefore how much already-collected garbage we need to look through when
deteriming the garbage status of other rows.  For example, if we expect that a
handoff might take as much as 2 weeks to complete (which would certainly be a
long time for a node to be down and recover) then we need to look through 2
weeks of previous garbage.  Consider this scenario:
 - In an un-versioned collection:
 - An archive is created (ver 1)
 - A handoff archive w/ same key is created (but not yet received to the local
   node) (this is ver 2 of the same archive as above)
 - Another archive is created (ver 3)
 - A specific tombstone is added for ver 3.
 - Versions 1 and 3 are garbage collected. (3 is garbage b/c of the tombstone.
   1 is garbage because 3 exists.)
 - Version 2 arrives as a handoff.  It's actually garbage the minute it
   arrives. It's arrival also means that version 1 is garbage sooner than we
   previously thought (i.e. version 1's end_time, which was previously
   calculated and stored as collected_end_time at the time it was garbage
   collected, needs to be moved back.  It's collected_by_unified_id column
   should be updated too.)

The above lends us the following design:
 - Current, not-known-garbage segments are stored in segment and conjoined
   tables as we expect.
 - When we collect rows, we move them to garbage_segment_conjoined_recent.
   This is a single table that's a de-normalized join of segment and conjoined
   PLUS all the columns calculated by the garbage collection queries (see the
   list of_gc_columns below) AND a few columns record keeping the status of the
   row at collection time. (see _collected_columns.)
 - If we don't know that the garbage collector has kept up (see below about
   optimizations), when we query, we actually query across the union of
   segment+conjoined with garbage_segment_conjoined_recent.
   (garbage_segment_conjoined_recent has indexes to make this fast.)
 - When rows in garbage_segment_conjoined_recent have a collected_time that is
   older than _max_handoff_time, they are moved to garbage_segment_conjoined_old.
 - We can collect rows like tombstones immediately (moving them into the
   garbage_segment_conjoined_recent table) instead of having to leave them in
   the segment table for up to _max_handoff_time.

Possible optimizations:
 These queries look quite long but in terms of CPU and IO, they should be
 fairly cheap.  The cases where they will be most costly are where there are a
 very large number of uncollected versions for the same key.

 If database performance becomes concerning, we could keep an unlogged stats
 table on the max unified_id of each collection, and the max unified_id

 query the main tables directly, knowing that all rows would be non-garbage.

 To support the above, we could arrange some sort of event-driven auto garbage
 collector.  

I recommend working on it by generating the text of queries via the command
line interface and looking at them in the editor.  Then use paren matching in
Vi/Emacs to move around the nested parts of the query.

    python2.7 ./gc.py -c 1 -k key-10 -q collectable_archive > /tmp/test.sql
    vim /tmp/test.sql

More examples:

    python2.7 ./gc.py -c 1 -k key-10 -q collectable_archive > /tmp/test.sql ; vim /tmp/test.sql
    python2.7 ./gc.py -c 1 -v -q list_versions > /tmp/test.sql ; vim /tmp/test.sql
    python2.7 ./gc.py -c 1 -v -q list_versions -p key-10 -km key-100 -vm 140914007 > /tmp/test.sql ; vim /tmp/test.sql

"""

import os
import sys
import argparse
from psycopg2.extensions import adapt

# longest amount of time we anticipate a handoff could take
_max_handoff_time = 86400 * 14 # 2 weeks
# make it configurable
_max_handoff_time = int(os.environ.get("NIMBUSIO_MAX_HANDOFF_TIME", 
                                       _max_handoff_time))

_max_handoff_time_sql = "%d seconds" % ( _max_handoff_time, )

_segment_columns = u"""
segment_id
collection_id
key
status
unified_id
timestamp
segment_num
conjoined_part
file_size
file_adler32
file_hash
file_tombstone_unified_id
source_node_id
handoff_node_id
""".strip().split()

_conjoined_columns = u"""
conjoined_id
conjoined_create_timestamp
conjoined_abort_timestamp
conjoined_complete_timestamp
conjoined_delete_timestamp
combined_size
combined_hash
""".strip().split()

_gc_columns = u"""
gc_window_unified_id
gc_general_tombstone_unified_id
gc_general_tombstone_timestamp
gc_specific_tombstone_unified_id
gc_specific_tombstone_timestamp
gc_archive_unified_id
gc_archive_timestamp
key_row_num
key_row_count
gc_next_general_tombstone_unified_id
gc_next_general_tombstone_timestamp
gc_next_archive_unified_id
gc_next_archive_timestamp
gc_next_specific_tombstone_unified_id
gc_next_specific_tombstone_timestamp
unversioned_end_time
versioned_end_time
""".strip().split()

_collected_columns = u"""
collected_time
collected_end_time
collected_by_unified_id
""".strip().split()

_columns = ( _segment_columns + _conjoined_columns + _gc_columns
             + _collected_columns )

def _from(base_where=None, include_recent_garbage=True, 
          include_all_garbage=False, exclude_handoffs=False):
    """
    This generates the FROM clause of the query, making the table defined by a
    nested subselect that includes the calculations necessary for deterimining
    row availability/garbage status.  
    
    In general this implements the inside-out, nested-subselect form of the
    views from sql/gc.sql through gc_archive_batches_with_end_time.  This form
    is more compatible with the query planner.

    In most cases, include_recent_garbage should be True, and this means the
    query will look at rows in the garbage_segment_conjoined_recent table
    (which has garbage rows collected up until _max_handoff_time ago) as well
    as the segment and conjoined tables.  This allows the "eventually
    consistent" late arriving (through handoffs) rows to still be calculated as
    garbage if appropriate.

    If include_all_garbage is True, then we also include
    garbage_segment_conjoined_old, which includes all garbage rows through
    whatever our retention policy is.  This might be useful for historical
    reporting queries.
    """

    if base_where is not None:
        base_where = u"%s\n" % (base_where, )
    else:
        base_where = u""

    template = u"""
(
SELECT 
       gc_archive_batches.*,
       /* several cases, for each of the possibilities */
       least( 
           conjoined_abort_timestamp, conjoined_delete_timestamp,
           /* how soon does a later archive replace this one? */
           CASE WHEN status='F' AND gc_next_archive_unified_id<>unified_id 
           THEN gc_next_archive_timestamp
           ELSE NULL END,
           /* how soon does a general tombstone apply? */
           CASE WHEN status='F' AND gc_next_general_tombstone_unified_id<>unified_id
           THEN gc_next_general_tombstone_timestamp
           ELSE NULL END,
           /* how soon does a specific tombstone apply */
           CASE WHEN status='F' AND gc_next_specific_tombstone_unified_id<>unified_id
           THEN gc_next_specific_tombstone_timestamp
           ELSE NULL END
       ) AS unversioned_end_time,
       least(
           /* same as above, except next archive doesn't apply */
           conjoined_abort_timestamp, conjoined_delete_timestamp,
   
           /* how soon does a general tombstone apply? */
           CASE WHEN status='F' AND gc_next_general_tombstone_unified_id<>unified_id
           THEN gc_next_general_tombstone_timestamp
           else null end,
           /* how soon does a specific tombstone apply */
           CASE WHEN status='F' AND gc_next_specific_tombstone_unified_id<>unified_id
           THEN gc_next_specific_tombstone_timestamp
           else null end
   
       ) AS versioned_end_time
  FROM
(
SELECT 
       /* from the real segment table */
       gc_archive.*,
       /* window over all the like keys for this collection_id  */
       row_number() over key_rows AS key_row_num,
       count(*) over key_rows AS key_row_count,
       /* window over all the later segments for this key */
       min(gc_general_tombstone_unified_id) over key_later_rows 
           AS gc_next_general_tombstone_unified_id,
       min(gc_general_tombstone_timestamp) over key_later_rows 
           AS gc_next_general_tombstone_timestamp,
       min(gc_archive_unified_id) over key_later_rows 
           AS gc_next_archive_unified_id,
       min(gc_archive_timestamp) over key_later_rows 
           AS gc_next_archive_timestamp,
       /* window over this key and later specific tombstones for this key */
       min(gc_specific_tombstone_unified_id) over key_unified_id_rows
           AS gc_next_specific_tombstone_unified_id,
       min(gc_specific_tombstone_timestamp) over key_unified_id_rows
           AS gc_next_specific_tombstone_timestamp
FROM
(
SELECT
    archive.*,

    /* add another column with the unified_id a version-specific tombstone
     * points to so that we can create a window with an archive and its
     * tombstone(s) */
    CASE status = 'T' and file_tombstone_unified_id is not null
        when true then file_tombstone_unified_id
        else unified_id
    END as gc_window_unified_id,

    /* add some additional columns that only have null values
       unless they meet specific conditions.  this lets us use a window
       function across these columns to find, for example, the minimum
       timestamp of a tombstone that applies to a row. */

    CASE status = 'T' and file_tombstone_unified_id is null
        when true then unified_id
        else null 
    END as gc_general_tombstone_unified_id,

    CASE status = 'T' and file_tombstone_unified_id is null
        when true then timestamp
        else null 
    END as gc_general_tombstone_timestamp,

    CASE status = 'T' and file_tombstone_unified_id is not null
        when true then unified_id
        else null 
    END as gc_specific_tombstone_unified_id,

    CASE status = 'T' and file_tombstone_unified_id is not null
        when true then timestamp
        else null 
    END as gc_specific_tombstone_timestamp,

    /* a single (not conjoined) archive can replace a previous version when it
     * becomes final
     * a conjoined archive can only replace a previosu version when the whole
       conjoined archive becomes complete */
    CASE 
        when status = 'F' and conjoined_part=0 then unified_id
        when status = 'F' 
             and conjoined_part=1 
             and conjoined_complete_timestamp is not null 
        then unified_id
        else null
    END as gc_archive_unified_id,

    CASE 
        when status = 'F' and conjoined_part=0 then timestamp
        when status = 'F' 
             and conjoined_part=1 
             and conjoined_complete_timestamp is not null 
        then conjoined_complete_timestamp
        else null
    END as gc_archive_timestamp
FROM
(
SELECT segment.id as segment_id,
       segment.collection_id,
       segment.key,
       segment.status,
       segment.unified_id,
       segment.timestamp,
       segment.segment_num,
       segment.conjoined_part,
       segment.file_size,
       segment.file_adler32,
       segment.file_hash,
       segment.file_tombstone_unified_id,
       segment.source_node_id,
       segment.handoff_node_id,
       conjoined.id AS conjoined_id,
       conjoined.create_timestamp AS conjoined_create_timestamp,
       conjoined.abort_timestamp AS conjoined_abort_timestamp,
       conjoined.complete_timestamp AS conjoined_complete_timestamp,
       conjoined.delete_timestamp AS conjoined_delete_timestamp,
       conjoined.combined_size,
       conjoined.combined_hash,
       null::timestamp as collected_time,
       null::timestamp as collected_end_time,
       null::int8 as collected_by_unified_id
       /*  these are redundant 
       conjoined.collection_id,
       conjoined.key,
       conjoined.unified_id,
       conjoined.handoff_node_id */
  FROM nimbusio_node.segment
  LEFT OUTER JOIN nimbusio_node.conjoined 
  USING (collection_id, key, unified_id)
"""

    if exclude_handoffs:
        template += """
  WHERE ((segment.handoff_node_id IS NULL AND 
          conjoined.handoff_node_id IS NULL))
""".lstrip("\n")

        if base_where:
            template += """
    AND %(base_where_unjoined)s
""".lstrip("\n")

    else:
        template += """
  WHERE ((segment.handoff_node_id IS NULL AND 
          conjoined.handoff_node_id IS NULL)
          OR
          segment.handoff_node_id = conjoined.handoff_node_id)
""".lstrip("\n")

        if base_where:
            template += """
    AND %(base_where_unjoined)s
""".lstrip("\n")
    
    if include_recent_garbage or include_all_garbage:
        template += u"""
UNION ALL
SELECT *
  FROM nimbusio_node.garbage_segment_conjoined_recent
""".lstrip()
    
        if base_where:
            template += """
 WHERE %(base_where)s
""".lstrip("\n")

    if include_all_garbage:
        template += u"""
UNION ALL
SELECT *
  FROM nimbusio_node.garbage_segment_conjoined_old
""".lstrip()

        if base_where:
            template += """
 WHERE %(base_where)s
"""

    template += u"""
) archive
) gc_archive
    WINDOW key_rows AS (
        PARTITION BY collection_id, key 
        ORDER BY unified_id ASC, conjoined_part ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    key_later_rows AS (
        PARTITION BY collection_id, key 
        ORDER BY unified_id ASC, conjoined_part ASC
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
    key_unified_id_rows AS (
        PARTITION BY collection_id, key, gc_window_unified_id
        ORDER BY unified_id ASC, conjoined_part ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ORDER BY collection_id, key, unified_id
) gc_archive_batches
) gc_archive_batches_with_end_time
""" 
    # 2012-12-18 dougfort -- this incorrectly changes conjoined_part to 
    # conjoined.part. So we change it back
    args = dict(
        base_where = base_where, 
        base_where_unjoined = base_where.replace("conjoined_", "conjoined.")) 
    args["base_where_unjoined"] = \
        args["base_where_unjoined"].replace("conjoined.part", "conjoined_part") 

    return template % args

class _NamedParam(object):
    """
    helper class for _base_where so callers can pass comparisons for column
    names that are different that the query parameters.  See for example how
    list_versions uses it.
    """
    # this thing's existence is kind of ugly. wish I had thought of a simpler
    # way.

    def __init__(self, **kwargs):
        assert len(kwargs) == 1
        self.param_name = kwargs.keys()[0]
        self.value = kwargs.values()[0]
    def __str__(self):
        return str(self.param_name)
    def __unicode__(self):
        return unicode(self.param_name)

def _base_where(pre_formed_clauses=[], **kwargs):
    """
    Generate the where clause to use at the deepest level of the query.

    This should be used to eliminate as many rows as possible, being mindful of
    which rows cannot be eliminated in order to satisfiy the needs (such as
    deteriming a row's garbage/recency/availability) in the higher layers of
    the query.

    Note that for eliminating handoff rows, use _from's exclude_handoffs
    argument instead, because that's actually part of the join conditions.

    keyword forms that look like __gt or __gteq and __prefix will make the
    comparsion using the appropriate operators, on the column name before the
    __.

    Pass a _NamedParam instance if you need to compare a column that has a
    different name than the parameter you want it bound to.  Examples are
    key_marker, version_marker, and prefix, which compare to the key,
    unified_id, and key columns respectively but are bound to different
    parameter names.

    A few boolean only arguments are special cases:

    'exclude_later_parts' eliminates rows with conjoined_part >= 2.
    'exclude_active' exclude archives that are actively being uploaded (by the
        segment status of A or a missing conjoined_complete_timestamp)
    'exclude_canceled' exclude archives that have been canceled (by the segment
        being canceled or the conjoined archive being aborted.)

    """

    # I feel like I've started writing an ORM here...and I hate most ORMs.
    # If you're maintaining this code, please don't let this bit grow out of
    # control.  Refactor as needed.  Just be sure to find a way that keeps the
    # implementation of the public functions equally consise and clear.

    if not kwargs:
        return None

    clauses = []

    if kwargs.pop("exclude_later_parts", None):
        clauses.append("conjoined_part < 2")

    if kwargs.pop("exclude_active", None):
        clauses.append("status <> 'A'")
        clauses.append("(conjoined_create_timestamp IS NULL \n"
                       "         OR conjoined_complete_timestamp IS NOT NULL)")

    if kwargs.pop("exclude_canceled", None):
        clauses.append("status <> 'C'")
        clauses.append("conjoined_abort_timestamp IS NULL")

    names = ( 'collection_id key '
              'key__prefix key__gt key__lt key__gteq '
              'unified_id unified_id__gt unified_id__gteq' ).split()
    for name in kwargs:
        if not name in names:
            #print names
            raise ValueError("unknown _base_where argument: %r" % (name, ))

    for name in names:
        value = kwargs.get(name, None)
        if value is None:
            continue
        if value.__class__ is _NamedParam and value.value is None:
            continue

        param_name = None
        if value.__class__ is _NamedParam:
            param_name = value.param_name

        if name.endswith("__prefix"):
            col_name = name[:-8]
            param_name = param_name if param_name else col_name
            clauses.append("%s LIKE ( %%(%s)s || '%%%%' )" %
                           (col_name, param_name, ))
        elif name.endswith("__lt"):
            col_name = name[:-4]
            param_name = param_name if param_name else col_name
            clauses.append(u"%s < %%(%s)s" % (col_name, param_name, ))
        elif name.endswith("__gt"):
            col_name = name[:-4]
            clauses.append(u"%s > %%(%s)s" % (col_name, param_name, ))
        elif name.endswith("__gteq"):
            col_name = name[:-6]
            param_name = param_name if param_name else col_name
            clauses.append(u"%s >= %%(%s)s" % (col_name, param_name, ))
        else:
            param_name = param_name if param_name else name
            clauses.append(u"%s = %%(%s)s" % (name, param_name, ))

    clauses.extend(pre_formed_clauses)

    base_where = "\n   AND ".join(clauses)
    return base_where

def _where(final=True, garbage=False, versioned=False, collectable=False,
           unified_id=None):
    """
    generate a where clause for the outer level, when columns calculated
    by the gc querys are known.

     final=True gives your only archives that are completely uploaded.
     garbage=False excludes rows that are garbage
     collectable=True gives rows that are garbage but not previously collected
     unified_id=filter by unified_id. (In many cases you can't do it earlier
         w/o breaking gc.)
    """

    clauses = []

    if unified_id:
        clauses.append(u"unified_id = %(unified_id)s")

    if final:
        clauses.append(u"""
   status = 'F' 
   AND (conjoined_part=0 
        OR (conjoined_complete_timestamp IS NOT NULL
            AND conjoined_abort_timestamp IS NULL))
""".strip())

    if garbage is False:
        clauses.append(u"collected_time IS NULL")
        if versioned:
            clauses.append(u"versioned_end_time IS NULL")
        else:
            clauses.append(u"unversioned_end_time IS NULL")

    if collectable:
        clauses.append('collected_time IS NULL')
        if versioned:
            end_time_column = u"versioned_end_time"
        else:
            end_time_column = u"unversioned_end_time"
        clauses.append(u"""
      ((status = 'C' 
         OR status = 'T'
         OR (conjoined_part > 0 AND conjoined_abort_timestamp IS NOT NULL))
        OR 
         %s IS NOT NULL)
""".strip() % (end_time_column, ))
        
    sql = u"\n   AND ".join(clauses)
    return sql

def _add_num_newer_versions(base_sql, allow=0, limit=None, sort=True):
    """
    wrap a query in a couple more window selects to give us a new column
    "num_newer_versions", which tells us, for every row, how many unified_ids
    are more recent.  Implemented as dense_rank - 1.
     
    The allow paramater limits the included num_newer_versions.  For example,
    setting it to 0 means that every row will be the newest row for that key.
    Setting it to 5 would give you the newest row, and the 4 most recent rows
    behind it.

    The limit paramater is applied after allow, such that you can say "I want
    1000 rows, newest versions of each archive only".
    
    Set sort to False earlier in the query stack (and leave it True here) to
    reduce the number of sorts, or leave it False in both cases if you don't
    care.
    """

    sql = u"""
SELECT survivor_key_window_base2.* 
  FROM 
(
SELECT survivor_key_window_base.*,
       (dense_rank() over surviving_key_rows) - 1 as num_newer_versions
  FROM (
"""

    sql += base_sql

    sql += u"""
) survivor_key_window_base
WINDOW surviving_key_rows AS (
    PARTITION BY collection_id, key 
    ORDER BY unified_id DESC
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
) survivor_key_window_base2
 WHERE num_newer_versions <= %d
""" % (allow, )

# 2012-12-19 dougfort -- ORDER BY must precede LIMIT

    if sort:
        sql +=  u""" 
 ORDER BY collection_id, key, unified_id DESC
"""

    if limit:
        sql += u"""
 LIMIT %d""" % ( limit, )

    return sql

def _compose(columns, from_, where='', sort="unified-id-desc", limit=None):
    "return a full SQL query based on the outputs of the helper functions"
    columns = u",\n       ".join(columns)
    sql =       u"SELECT " + columns + u"\n"
    sql +=      u"  FROM " + from_ 
    if where:
        sql +=  u" WHERE " + where + u"\n"
    assert sort in [None, "unified-id-desc", "unified-id-asc", ]
    if sort == "unified-id-desc":
        sql +=  u" ORDER BY collection_id, key, unified_id DESC" + u"\n"
    if sort == "unified-id-asc":
        sql +=  u" ORDER BY collection_id, key, unified_id ASC" + u"\n"
    if limit:
        sql +=  u" LIMIT %d" % (limit, )
    return sql

def mogrify(sql, params):
    "safely quote/escape an SQL statement w/ named parameters"
    safe_params = []
    for k, v in params.iteritems():
        if type(v) is unicode:
            safe_params.append((k, adapt(v), ))
        elif type(v) is str:
            safe_params.append((k, adapt(v.decode('utf_8')), ))
        elif type(v) in [int, long]:
            safe_params.append((k, v, ))
        else:
            raise ValueError("cannot mogrify key %r type %r" % (k, type(v), ))
    safe_params = dict(safe_params)
    #print repr(params)
    #print sql
    safe_sql = unicode(sql) % safe_params
    return safe_sql

def full_gc_pass(versioned_collection_ids):
    """
    """
    pass

def gc_pass(collection_id, key = None, versioned = None):
    """
    """

    pass

def collectable_archive(collection_id, versioned = False, key = None, 
                                                  unified_id = None):
    """
    return sql query for a list of uncollected garbage archives meeting the
    selection criteria
    """

    base_where = _base_where(
        collection_id = collection_id,
        key = key,
        unified_id__gteq = unified_id,
        exclude_active = True
    )

    sql = _compose(_columns,
                   _from(base_where = base_where),
                   where = _where(final = None,
                                  garbage = None,
                                  collectable = True,
                                  versioned = versioned,
                                  unified_id = unified_id))
    return sql

def list_keys(collection_id, 
              versioned=False, 
              prefix=None, 
              key_marker=None, 
              limit=10001):
    """
    return sql to select the newest version row for final, not-garbage keys.
    Does not include handoff rows or later conjoined parts.
    """

    if key_marker is not None:
        if key_marker < prefix:
            raise ValueError("key_marker should be >= prefix")
        if not key_marker.startswith(prefix):
            raise ValueError("key_marker should start with prefix")

    base_where = _base_where(
        collection_id = collection_id,
        key__prefix = _NamedParam(prefix = prefix),
        key__gt = _NamedParam(key_marker = key_marker),
        exclude_later_parts = True,
        exclude_active = True,
        exclude_canceled = True
    )

    sql = _compose(_columns,
                   _from(exclude_handoffs = True,
                         base_where = base_where),
                   where = _where(final = True, 
                                  garbage = False, 
                                  versioned = versioned),
                    sort = None,
                   limit = None)

    # Reduce down to just the newest version of each key.  This is earliest
    # point that we can apply the LIMIT for a versioned collection.
    # Another way: we could filter out the rows with a unversioned_end_time and
    # then apply the LIMIT (since we're already using exclude_later_parts and
    # exclude_handoffs) and that would probably be faster.  For now, I'd rather
    # have fewer code paths.

    sql = _add_num_newer_versions(sql, allow = 0, limit = limit)

    return sql

def list_versions(collection_id, versioned=False, prefix=None, key_marker=None, 
                                                    version_marker=None,
                                                    limit=10001):
    """
    return sql to select rows for every version of for final, not-garbage keys.
    Does not include handoff rows or later conjoined parts.
    """

    # this is actually much easier than list_keys, because we don't need to go
    # to the effort of eliminating every row that isn't the newest version of a
    # key.

    if key_marker is not None:
        if key_marker < prefix:
            raise ValueError("key_marker should be >= prefix")
        if not key_marker.startswith(prefix):
            raise ValueError("key_marker should start with prefix")

    # require key_marker and version_marker or neither
    if ((key_marker or version_marker) and not 
        (key_marker and version_marker)
    ):
        raise ValueError("version_marker should be used with key_marker")

    # version_marker can only be applied to rows that have the same key as key
    # marker. Rows from later keys might have earlier unified_ids that we
    # should not exclude.
    pre_formed_clauses = []
    if version_marker is not None:
        pre_formed_clauses.append(
            "( key <> %(key_marker)s OR unified_id > %(version_marker)s )")

    base_where = _base_where(
        collection_id = collection_id,
        exclude_later_parts = True,
        exclude_active = True,
        exclude_canceled = True,
        key__prefix = _NamedParam(prefix = prefix),
        key__gteq = _NamedParam(key_marker = key_marker),
        pre_formed_clauses = pre_formed_clauses,
    )

    sql = _compose(_columns,
                   _from(exclude_handoffs = True, 
                         base_where = base_where),
                   where = _where(final=True,
                                  garbage=False,
                                  versioned=versioned),
                    # have to sort such that the versions are returned in order
                    # of increasing unified_id, because the version_marker is
                    # compared using greater than.
                    sort = "unified-id-asc",
                   limit = limit)
    return sql

def version_for_key(collection_id, versioned=False, key=None, unified_id=None):
    """
    Select all the final, not-garbage rows (including handoffs and conjoined
    parts beyond the first one) for a version of a key.

    If unified_id is specified, select rows for that version.  Otherwise,
    select rows for the newest avaliable version.
    """
    # this really needs to solve the problem of finding the newest
    # available described archive
    
    # for _base_where, we cannot filter all records by unified_id even if we
    # know the specific unified_id we are looking for. this is because the
    # presence of a later tombstone or later archive may make the record we
    # want, with the unified_id, garbage.  We won't know that it's garbage if
    # we filter by unified_id at the base.  However, we do know that only later
    # unified_ids may make a specific unified_id garbage. So we can at least
    # use >=.

    # we then filter on the specific unified_id in the outer level _where,
    # where the garbage status is known.
    base_where = _base_where(
        collection_id = collection_id,
        key = key,
        unified_id__gteq = unified_id,
        exclude_active = True,
        exclude_canceled = True,
    )

    sql = _compose(_columns,
                   _from(base_where = base_where),
                   where = _where(final = True, 
                                  garbage = False, 
                                  versioned = versioned,
                                  unified_id = unified_id),
                    sort = None,
                   limit = None)

    # I'm not sure if it would be any faster, but another way of reducing down
    # to only the most recent version's rows would be as Common Table
    # Expressions.  I.e.: 
    #   WITH all_verion_rows AS ( 
    #       _sql 
    #   ), first_row_unified_id AS (
    #       SELECT unified_id 
    #         FROM all_version_rows 
    #        LIMIT 1
    #   )
    #   SELECT * 
    #     FROM all_version_rows 
    #    WHERE unified_id=(SELECT unified_id 
    #                        FROM first_row_unified_id 
    #                       LIMIT 1)

    # Sticking w/ this approach for now since it employs more code re-use
     
    sql = _add_num_newer_versions(sql, allow = 0)

    return sql

def _parse_command_line():
    parser = argparse.ArgumentParser(description="command line gc sql printer")
    parser.add_argument("-q", "--query", dest="query", 
        help=u"query to provide")
    parser.add_argument("-c", "--collection", dest="collection_id", 
        help=u"collection_id to operate on")
    parser.add_argument("-v", "--versioned", dest="versioned", 
        action='store_true', help=u"collection is versioned", default=False)
    parser.add_argument("-k", "--key", dest="key", help="key to operate on", 
        default=None)
    parser.add_argument("-u", "--unified_id", dest="unified_id", 
        help=u"unified_id to operate on", default=None)
    parser.add_argument("-p", "--prefix", dest="prefix", 
        help=u"prefix when listing keys", default='')
    parser.add_argument("-km", "--key_marker", dest="key_marker", 
        help=u"key_marker when listing keys/versions", default=None)
    parser.add_argument("-vm", "--version_marker", dest="version_marker", 
        help=u"version_marker when listing versions (a unified_id)", 
        default=None)
    parser.add_argument("-l", "--limit", dest="limit", 
        help=u"limit query to N results", default=None)
    args = parser.parse_args()
    query_params = dict([(k, v) for k, v in args._get_kwargs() 
                                if v is not None])
    query_params.pop("versioned", None)
    query_params.pop("query", None)
    return args, query_params

def run_as_utility():
    "process command line args and print mogrifed SQL to stdout"
    args, query_params = _parse_command_line()
    if not args.query in globals():
        print >> sys.stderr, "Unknown query %s" % (args.query ,) 
        return 1

    func = globals().get(args.query, None)
    if args.query == 'collectable_archive':
        sql = func(args.collection_id, args.versioned, args.key)
    elif args.query == 'list_versions':
        sql = func(args.collection_id, args.versioned, 
                   args.prefix, args.key_marker, args.version_marker, 
                   args.limit)
    elif args.query == 'list_keys':
        sql = func(args.collection_id, args.versioned, 
                   args.prefix, args.key_marker, args.limit)
    elif args.query == 'version_for_key':
        sql = func(args.collection_id, args.versioned,
                   args.key, args.unified_id)
    else:
        raise RuntimeError("Unknown query")

    safe_sql = mogrify(sql, query_params)
    print safe_sql.encode("utf_8")

    return 0

if __name__ == "__main__":
    sys.exit(run_as_utility())
