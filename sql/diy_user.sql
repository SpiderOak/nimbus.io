begin;

create sequence diy_user_user_id;
create table diy_user (
    user_id int4 primary key not null default nextval('diy_user_user_id'),
    username varchar(50) not null unique
        check (
            username ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$'
            and username !~ '--'
        )
);


create table diy_user_key (
    user_id int4 not null references diy_user (user_id),
    key_id int4 not null references diy_key (key_id),
    unique (user_id, key_id)
);


grant select on diy_user to diyapi;
grant select on diy_user_key to diyapi;


-- for unit tests
insert into diy_user (user_id, username) values (0, 'test');
insert into diy_user_key (user_id, key_id) values (0, 0);

commit;
