create sequence if not exists pgmq_message_seq start
    with
    1 increment by 1 minvalue 1;

drop table if exists pgmq_invisible_queue;
create table pgmq_invisible_queue
(
    id           bigint                            default nextval('pgmq_message_seq') primary key,
    create_time  timestamp                not null default now(),
    topic        varchar(100) collate "C" not null,
    priority     int                      not null default 0,
    payload      varchar collate "C"      not null,
    attempt      int                      not null default 0,
    visible_time timestamp                not null
);

create index pgmq_invisible_queue_visible_time on pgmq_invisible_queue using btree (visible_time);

drop table if exists pgmq_pending_queue;
create table pgmq_pending_queue
(
    id          bigint                            default nextval('pgmq_message_seq') primary key,
    create_time timestamp                not null default now(),
    topic       varchar(100) collate "C" not null,
    priority    int                      not null default 0,
    payload     varchar collate "C"      not null,
    attempt     int                      not null default 0
);

create index idx_pgmq_pending_queue_topic_priority_id on pgmq_pending_queue (
                                                                           topic,
                                                                           priority desc,
                                                                           id asc
    );

drop table if exists pgmq_processing_queue;
create table pgmq_processing_queue
(
    id           bigint primary key,
    create_time  timestamp                not null,
    topic        varchar(100) collate "C" not null,
    priority     int                      not null default 0,
    payload      varchar collate "C"      not null,
    attempt      int                      not null,
    timeout_time timestamp                not null
);

create index idx_pgmq_processing_queue_timeout_time on pgmq_processing_queue using btree (timeout_time);

drop table if exists pgmq_dead_queue;
create table pgmq_dead_queue
(
    id          bigint primary key,
    create_time timestamp                not null,
    topic       varchar(100) collate "C" not null,
    priority    int                      not null,
    payload     varchar collate "C"      not null,
    attempt     int                      not null,
    dead_time   timestamp                not null
);

create index idx_pgmq_dead_queue_topic on pgmq_dead_queue (topic);
