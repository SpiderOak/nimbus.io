begin;

create sequence diy_user_user_id_seq;
create table diy_user (
    user_id int4 primary key not null default nextval('diy_user_user_id_seq'),
    username varchar(50) not null unique check (
        username ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$'
        and username !~ '--'),
    avatar_id int4 not null
);


create table diy_user_key (
    user_id int4 not null references diy_user (user_id),
    key_id int4 not null references diy_key (key_id),
    unique (user_id, key_id)
);


grant select on diy_user to diyapi;
grant select on diy_user_key to diyapi;


-- for unit tests
insert into diy_user (user_id, username, avatar_id) values (0, 'test', 77970);  -- dhtest35
insert into diy_user_key (user_id, key_id) values (0, 0);

commit;


/* -- add avatar_id column
begin;
alter table diy_user add column avatar_id int4 not null default 77970;
alter table diy_user alter avatar_id drop default;
commit;
*/
