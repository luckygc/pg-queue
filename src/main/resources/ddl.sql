create sequence if not exists pgq_message_seq start
    with
    1 increment by 1 minvalue 1;

drop table if exists pgq_invisible_queue;
create table pgq_invisible_queue
(
    id           bigint                            default nextval('pgq_message_seq') primary key,
    create_time  timestamp                not null default now(),
    topic        varchar(100) collate "C" not null,
    priority     int                      not null default 0,
    payload      varchar collate "C"      not null,
    attempt      int                      not null default 0,
    visible_time timestamp                not null
);

create index pgq_invisible_queue_visible_time on pgq_invisible_queue using btree (visible_time);

drop table if exists pgq_pending_queue;
create table pgq_pending_queue
(
    id          bigint                            default nextval('pgq_message_seq') primary key,
    create_time timestamp                not null default now(),
    topic       varchar(100) collate "C" not null,
    priority    int                      not null default 0,
    payload     varchar collate "C"      not null,
    attempt     int                      not null default 0
);

create index idx_pgq_pending_queue_topic_priority_id on pgq_pending_queue (
                                                                           topic,
                                                                           priority desc,
                                                                           id asc
    );

drop table if exists pgq_processing_queue;
create table pgq_processing_queue
(
    id           bigint primary key,
    create_time  timestamp                not null,
    topic        varchar(100) collate "C" not null,
    priority     int                      not null default 0,
    payload      varchar collate "C"      not null,
    attempt      int                      not null,
    process_time timestamp                not null default now()
);

create index idx_pgq_processing_queue_process_time on pgq_processing_queue using btree (process_time);

drop table if exists pgq_dead_queue;
create table pgq_dead_queue
(
    id          bigint primary key,
    create_time timestamp                not null,
    topic       varchar(100) collate "C" not null,
    priority    int                      not null,
    payload     varchar collate "C"      not null,
    attempt     int                      not null,
    dead_time   timestamp                not null
);
