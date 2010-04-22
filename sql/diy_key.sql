begin;

create sequence diy_key_key_id;
create table diy_key (
    key_id int4 primary key not null default nextval('diy_key_key_id'),
    key text not null
);

grant select on diy_key to diyapi;

-- key for unit tests
insert into diy_key (key_id, key) values (9999, 'deadbeef');

commit;
