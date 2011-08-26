/****
 * schema for centralized diy database
 ****/

begin;

drop schema if exists diy_central cascade;
create schema diy_central;
set search_path to diy_central, public;

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

create sequence collection_id_seq;
create table collection (
    id int4 unique not null default nextval('diy_central.collection_id_seq'),
    cluster_id int4 not null references diy_central.cluster(id),
    name varchar(1024) not null default '(default)',
    avatar_id int4 not null,
    creation_time timestamp default 'now',
    unique (avatar_id, name)
);

/* get avatar_id for collection name */
create index collection_name_idx on collection("name");

/* get all collection names for avatar_id */
create index collection_avatar_id_name_idx on collection("avatar_id", "name");

create sequence conjoined_id_seq;
create table conjoined (
    conjoined_id int4 not null default nextval('diy_central.conjoined_id_seq'),
    collection_id int4 not null,
    key varchar(1024) not null,
    identifier bytea not null, 
    create_timestamp timestamp not null default current_timestamp,
    abort_timestamp timestamp,
    complete_timestamp timestamp,
    combined_size int8,
    combined_hash bytea
);
create unique index conjoined_identifier_idx on diy_central.conjoined ("identifier");

create table space_accounting(
   avatar_id int4 not null,
   timestamp timestamp not null,
   bytes_added int8 not null default 0,
   bytes_removed int8 not null default 0,
   bytes_retrieved int8 not null default 0
);

create sequence audit_result_id_sequence;
create table audit_result(
   diyapi_audit_result_id int4 not null primary key default nextval('diy_central.audit_result_id_sequence'),
   avatar_id int4 not null,
   state text,
   audit_scheduled timestamp default now(),
   audit_started timestamp,
   audit_finished timestamp,
   reconstruct_scheduled timestamp,
   reconstruct_started timestamp,
   reconstruct_finished timestamp
);

create index audit_result_avatar_id on diy_central.audit_result (avatar_id);

grant all privileges on schema diy_central to pandora;
grant all privileges on all tables in schema diy_central to pandora;
grant all privileges on all sequences in schema diy_central to pandora;

/*rollback;*/
commit;

