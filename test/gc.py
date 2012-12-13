"""
Testing for sql/gc.py.  

This is intended to be ran connecting to a node local database that has been
populated by sql/test_gc.sql.  Turn up the generate_series() statement in that
file to high values to check performance.
"""

# TODO write me

test_collection_id = 1
test_key = 'key-10'
test_prefix = 'key-1'
test_unified_id = None    # define later
test_no_such_unified_id = 1

# determine which rows are collectable garbage. 
# check that none of these rows appear in any other result.
# check that the rows from other results are not included here.
collectable_archive(test_collection_id, False, test_key, unified_id = None)

collectable_archive(test_collection_id, True, test_key, 
                    unified_id = test_no_such_unified_id)

# check that there's no more than one row per key for a versioned collection
# check that every row begins with prefix
list_versions(test_collection_id, False, test_prefix, key_marker=None,
                                    version_marker=None,
                                    limit=None)
# check that there's >= as many rows now as above.
list_versions(test_collection_id, True, test_prefix, key_marker=None,
                                    version_marker=None,
                                    limit=None)

# check that the list keys result is the same as list_versions in the
# unversioned case above (although there could be extra columns.)
list_keys(test_collection_id, False, test_prefix, key_marker=None,
                                    limit=None)
list_keys(test_collection_id, True, test_prefix, key_marker=None,
                                    limit=None)

# check that the limits and markers work correctly. perhaps take the result
# with limit=None, and run a series of queries with limit=1 for each of those
# rows, checking results.

# check that this should newer return any rows
version_for_key(test_collection_id, True, test_key, 
                unified_id = test_no_such_unified_id)

# check that for every row in list_keys, calling version_for_key with
# unified_id=None should return the same row, regardless of it being versioned
# or not.
version_for_key(test_collection_id, True, test_key, unified_id=None)


# check that this returns empty
version_for_key(test_collection_id, False, test_key, 
                unified_id = test_no_such_unified_id)
# check that this returns empty
version_for_key(test_collection_id, True, test_key, 
                unified_id = test_no_such_unified_id)

# check that this can find all the same rows list_keys returns
version_for_key(test_collection_id, False, test_key, unified_id=None)

# check that this can find all the same rows list_versions returns in the
# versioned case above
version_for_key(test_collection_id, True, test_key, 
                unified_id = loop_over_unified_id_from_above)

# check that this can ONLY find the same rows list_versions returns 
# above IF they are also in the result that list_keys returns
# (i.e. some of them should be findable, some not.)
version_for_key(test_collection_id, True, test_key, 
                unified_id = loop_over_unified_id_from_above)
