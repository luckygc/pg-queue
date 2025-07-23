create sequence if not exists pgq_message_seq start
with
    1 increment by 1 minvalue 1;

drop table if exists pgq_queue;
-- status : PENDING,PROCESSING,COMPLETED,DEAD
create table
    if not exists pgq_queue (
        id bigint default nextval ('pgq_message_seq') primary key,
        create_time timestamp not null default now(),
        update_time timestamp not null default now(),
        visible_time timestamp not null default now(),
        status varchar(20) collate "C" not null,
        topic varchar(100) collate "C" not null,
        priority int not null default 0,
        payload varchar collate "C" not null,
        attempt int default 0
    );

create index if not exists idx_pgq_queue_visible_time_status_topic_priority_id on pgq_queue (
    visible_time,
    status,
    topic,
    priority desc,
    id asc
);
