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
    creation_time timestamp
);

create table node (
    id serial primary key,
    cluster_id int4 not null,
    node_number_in_cluster int4 not null,
    name varchar(255) unique not null,
    hostname varchar(255) not null,
    offline bool not null default false,
    creation_time timestamp
);

create sequence collection_id_seq;
create table collection (
    id int4 unique not null default nextval('diy_central.collection_id_seq'),
    cluster_id int4 not null references diy_central.cluster(id),
    name varchar(1024) unique not null,
    avatar_id int4 not null,
    timestamp timestamp not null
);

/* get avatar_id for collection name */
create index collection_name_idx on collection("name");

/* get all collection names for avatar_id */
create index collection_avatar_id_name_idx on collection("avatar_id", "name");

create table space_accounting(
   collection_id int4 not null references diy_central.collection(id),
   timestamp timestamp not null,
   bytes_added int8 not null default 0,
   bytes_removed int8 not null default 0,
   bytes_retrieved int8 not null default 0
);

grant all privileges on schema diy_central to pandora;
grant all privileges on all tables in schema diy_central to pandora;
grant all privileges on all sequences in schema diy_central to pandora;

rollback;
/*commit;*/

