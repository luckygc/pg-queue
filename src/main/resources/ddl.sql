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


drop function if exists pgmq_move_timeout_and_visible_msg_to_pending_then_notify;
CREATE FUNCTION pgmq_move_timeout_and_visible_msg_to_pending_then_notify()
    RETURNS TABLE
            (
                _topic varchar(100)
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF NOT pg_try_advisory_xact_lock(1997, 38) THEN
        RETURN;
    END IF;

    -- 如果拿到锁，则执行你的 delete/insert/notify 逻辑，并返回所有 topic
    RETURN QUERY
        WITH timeout_processing_messages AS (
            DELETE FROM pgmq_processing_queue
                WHERE timeout_time <= now()
                RETURNING id, create_time, topic, priority, payload, attempt),
             visible_messages AS (
                 DELETE FROM pgmq_invisible_queue
                     WHERE visible_time <= now()
                     RETURNING id, create_time, topic, priority, payload, attempt),
             message_to_pending AS (SELECT id, create_time, topic, priority, payload, attempt
                                    FROM timeout_processing_messages
                                    UNION ALL
                                    SELECT id, create_time, topic, priority, payload, attempt
                                    FROM visible_messages),
             insert_op AS (
                 INSERT INTO pgmq_pending_queue (id, create_time, topic, priority, payload, attempt)
                     SELECT id, create_time, topic, priority, payload, attempt
                     FROM message_to_pending),
             message_available_topics AS (SELECT DISTINCT topic
                                          FROM pgmq_pending_queue),
             _notify AS (SELECT pg_notify('pgmq_topic_channel', topic), topic
                         FROM message_available_topics)
        SELECT topic
        FROM _notify;
END;
$$;

select pgmq_move_timeout_and_visible_msg_to_pending_then_notify()
