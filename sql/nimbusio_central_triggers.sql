begin;

set search_path to nimbusio_central, public;

CREATE OR REPLACE FUNCTION notify_cache_update() RETURNS TRIGGER AS $$
    import cPickle
    import zlib
    import base64
    import uuid

    payload = dict(
        event = TD['event'],
        table_schema = TD['table_schema'], 
        table_name = TD['table_name'], 
        old = TD['old'], 
        new = TD['new'], )

    if TD['old'] is not None and 'id' in TD['old']:
        payload['row_id'] = TD['old']['id']
    elif TD['new'] is not None and id in TD['new']:
        payload['row_id'] = TD['new']['id']
    else:
        payload['row_id'] = 0

    binary_payload = base64.b64encode(zlib.compress(cPickle.dumps(payload)))

    # if our message is too large, strip out the row dumps
    if len(binary_payload) > 7800:
        del payload['old']
        del payload['new']
        binary_payload = base64.b64encode(
            zlib.compress(cPickle.dumps(payload)))

    header = "%s.%s\n%s\n" % (
        TD['table_schema'], TD['table_name'], str(uuid.uuid4()), )

    # get query plan
    if "notify_cache_update_plan" in SD:
      notify_cache_update_plan = SD["notify_cache_update_plan"]
    else:
        notify_cache_update_plan = \
            plpy.prepare("select pg_notify($1, $2 || $3)", 
                [ "text", "text", "text"])
        SD["notify_cache_update_plan"] = notify_cache_update_plan

    res = plpy.execute(notify_cache_update_plan,
        [ "nimbusio_central_cache_update", header, binary_payload, ])

$$ LANGUAGE plpythonu;
         
DROP TRIGGER IF EXISTS z00000_notify_invalid_node_cache ON node;
CREATE TRIGGER z00000_notify_invalid_node_cache
AFTER UPDATE ON node
FOR EACH ROW
EXECUTE PROCEDURE notify_cache_update();

DROP TRIGGER IF EXISTS z00000_notify_invalid_cluster_cache ON "cluster";
CREATE TRIGGER z00000_notify_invalid_cluster_cache
AFTER UPDATE ON "cluster"
FOR EACH ROW
EXECUTE PROCEDURE notify_cache_update();

DROP TRIGGER IF EXISTS z00000_notify_invalid_customer_cache ON customer;
CREATE TRIGGER z00000_notify_invalid_customer_cache
AFTER UPDATE ON customer
FOR EACH ROW
EXECUTE PROCEDURE notify_cache_update();

DROP TRIGGER IF EXISTS z00000_notify_invalid_customer_key_cache ON customer_key;
CREATE TRIGGER z00000_notify_invalid_customer_key_cache
AFTER UPDATE ON customer_key
FOR EACH ROW
EXECUTE PROCEDURE notify_cache_update();

DROP TRIGGER IF EXISTS z00000_notify_invalid_collection_cache ON collection;
CREATE TRIGGER z00000_notify_invalid_collection_cache
AFTER UPDATE ON collection
FOR EACH ROW
EXECUTE PROCEDURE notify_cache_update();

commit;
