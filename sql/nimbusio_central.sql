/****
 * schema for centralized nimbus.io database
 ****/

begin;

drop schema if exists nimbusio_central cascade;
create schema nimbusio_central;
set search_path to nimbusio_central, public;

/* just a mostly informational table to describe the details of the whole cluster */
create table "cluster" (
    id serial primary key,
    name varchar(255) unique not null,
    node_count int4 not null default 10,
    replication_level int4 not null default 3,
    creation_time timestamp default 'now'
);

create table node (
    id serial primary key,
    cluster_id int4 not null,
    node_number_in_cluster int4 not null,
    name varchar(255) unique not null,
    hostname varchar(255) not null,
    offline bool not null default false,
    creation_time timestamp default 'now'
);

create sequence customer_id_seq;
create table customer (
    id int4 primary key default nextval('nimbusio_central.customer_id_seq'),
    username varchar(60) unique not null,
    creation_time timestamp not null default now(),
    deletion_time timestamp
);

create sequence customer_key_id_seq;
create table customer_key(
   id int4 unique not null default nextval('nimbusio_central.customer_key_id_seq'),  
   customer_id int4 not null references nimbusio_central.customer(id),
   key text unique not null,
   description text,
   creation_time timestamp not null default now(),
   deletion_time timestamp
);

create index customer_key_customer_idx on nimbusio_central.customer_key("customer_id");

create sequence collection_id_seq;
create table collection (
    id int4 primary key default nextval('nimbusio_central.collection_id_seq'),
    name text unique not null,
    customer_id int4 not null references nimbusio_central.customer(id),
    cluster_id int4 not null references nimbusio_central.cluster(id),
    versioning bool not null default false,
    access_control text,
    creation_time timestamp not null default 'now',
    deletion_time timestamp,
    CONSTRAINT collection_name_length_check 
        check (deletion_time is not null or length(name) <= 63)
);


/* get all collection names for customer_id */
create index collection_customer_id_name_idx on nimbusio_central.collection("customer_id", "name");

create table space_accounting(
   collection_id int4 not null references nimbusio_central.collection(id),
   timestamp timestamp not null,
   bytes_added int8 not null default 0,
   bytes_removed int8 not null default 0,
   bytes_retrieved int8 not null default 0
);

create table collection_ops_accounting (
   collection_id int4 not null references nimbusio_central.collection(id),
   node_id int4 not null references nimbusio_central.node(id),
   timestamp timestamp not null,
   duration interval not null,
   get_request int4 not null default 0,
   get_success int4 not null default 0,
   get_error int4 not null default 0,
   put_request int4 not null default 0,
   put_success int4 not null default 0,
   put_error int4 not null default 0,
   listmatch_request int4 not null default 0,
   listmatch_success int4 not null default 0,
   listmatch_error int4 not null default 0,
   delete_request int4 not null default 0,
   delete_success int4 not null default 0,
   delete_error int4 not null default 0,
   socket_bytes_in int8 not null default 0,
   socket_bytes_out int8 not null default 0,
   success_bytes_in int8 not null default 0,
   success_bytes_out int8 not null default 0,
   error_bytes_in int8 not null default 0,
   error_bytes_out int8 not null default 0
);
create unique index collection_ops_accounting_idx on collection_ops_accounting
    ("collection_id", "node_id", "timestamp");

COMMENT ON TABLE collection_ops_accounting IS 
'Historical summary usage data for potentially billable operations such as API
requests and bandwidth. Ops accounting is distinct from space usage accounting.

Rows describe a count or sum of events handled by a particular storage node
within a specific time period.';
COMMENT ON COLUMN collection_ops_accounting.node_id IS
'The storage node reporting for this period';
COMMENT ON COLUMN collection_ops_accounting.timestamp IS 
'A truncated timestamp for the period (i.e. the minute, hour, day, month)';
COMMENT ON COLUMN collection_ops_accounting.timestamp IS 
'The interval that timestamp was truncated to (ex 1 min, 5 min, 1 hour, etc)';
COMMENT ON COLUMN collection_ops_accounting.get_request IS 
'Number of GET requests initiated during the period';
COMMENT ON COLUMN collection_ops_accounting.get_success IS 
'Number of GET requests completed successfully. 

Note: A request is completed successfully even if the result is an error.  For
example, a GET for an object that does not exist is still a successfully
completed request, even though the result is a 404.  A request in which the
client socket disconnects before we can finish transferring all the data is
also a success.

Note: For all _success and _error columns in this table, the timestamp refers
to the time period in which the request completed -- not the time that it
began, which could have been much earlier.
';
COMMENT ON COLUMN collection_ops_accounting.get_error IS 
'Number of GET requests that ended in a server side processing error.';

COMMENT ON COLUMN collection_ops_accounting.socket_bytes_in IS
'This is the raw number of bytes read from a socket during the time period.

In other words, at the time a socket .read call completes indicating some
number of bytes received, the counter for the current period is incremented by
that number of bytes.

Note: This will not be exactly the same as bandwidth used, but probably close
enough for most billing needs.  Things like SSL and packet retries mean that
the socket doesn''t see exactly the same number of bytes as the network.';

COMMENT ON COLUMN collection_ops_accounting.socket_bytes_out IS 
'Like socket_bytes_in, but for outbound bytes.

Note: Most requests will accumulate both inbound and outbound bytes.  For
example, a GET request results in the server reading a request header, then
writing a reply.  There will be some bytes_out (for the server''s respone
headers) even if the request results in a 404.';

COMMENT ON COLUMN collection_ops_accounting.success_bytes_in IS 
'The sum of socket_bytes_in across the lifetime of the request, at the time
that a request has successfully completed (same definition of success as
discussed above.)

In other words, unlike socket_bytes, even if a request takes place across many
hours (a slow upload, for example), success and error bytes accumulate entirely
in the period that the request concluded.';
COMMENT ON COLUMN collection_ops_accounting.error_bytes_in IS 
'The sum of socket_bytes_in across the lifetime of the request, at the time
that a request has failed due to a server side processing error';

/* rollback; */
commit;
