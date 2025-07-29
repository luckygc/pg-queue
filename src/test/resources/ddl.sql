-- 消息队列全局序列，用于生成消息的唯一ID
create sequence if not exists pgmq_message_seq start
    with
    1 increment by 1 minvalue 1;

-- 不可见消息队列表：存储延迟消息和重试消息
drop table if exists pgmq_invisible_queue;
create table pgmq_invisible_queue
(
    id           bigint                            default nextval('pgmq_message_seq') primary key, -- 消息唯一标识
    create_time  timestamp                not null default now(),                                   -- 消息创建时间
    topic        varchar(100) collate "C" not null,                                                 -- 消息主题/队列名称
    priority     int                      not null default 0,                                       -- 消息优先级，数值越大优先级越高
    payload      varchar collate "C"      not null,                                                 -- 消息内容/负载数据
    attempt      int                      not null default 0,                                       -- 重试次数
    visible_time timestamp                not null                                                  -- 消息可见时间，到达此时间后消息才能被消费
);

-- 为不可见队列的可见时间字段创建索引，用于快速查找到期的消息
create index pgmq_invisible_queue_visible_time on pgmq_invisible_queue using btree (visible_time);

-- 为不可见消息队列表添加注释
COMMENT ON TABLE pgmq_invisible_queue IS '不可见消息队列表：存储延迟消息和重试消息';
COMMENT ON COLUMN pgmq_invisible_queue.id IS '消息唯一标识';
COMMENT ON COLUMN pgmq_invisible_queue.create_time IS '消息创建时间';
COMMENT ON COLUMN pgmq_invisible_queue.topic IS '消息主题/队列名称';
COMMENT ON COLUMN pgmq_invisible_queue.priority IS '消息优先级，数值越大优先级越高';
COMMENT ON COLUMN pgmq_invisible_queue.payload IS '消息内容/负载数据';
COMMENT ON COLUMN pgmq_invisible_queue.attempt IS '重试次数';
COMMENT ON COLUMN pgmq_invisible_queue.visible_time IS '消息可见时间，到达此时间后消息才能被消费';

-- 待处理消息队列表：存储等待被消费的消息
drop table if exists pgmq_pending_queue;
create table pgmq_pending_queue
(
    id          bigint                            default nextval('pgmq_message_seq') primary key, -- 消息唯一标识
    create_time timestamp                not null default now(),                                   -- 消息创建时间
    topic       varchar(100) collate "C" not null,                                                 -- 消息主题/队列名称
    priority    int                      not null default 0,                                       -- 消息优先级，数值越大优先级越高
    payload     varchar collate "C"      not null,                                                 -- 消息内容/负载数据
    attempt     int                      not null default 0                                        -- 重试次数
);

-- 为待处理队列创建复合索引，按主题、优先级降序、ID升序排序，用于高效的消息获取
create index idx_pgmq_pending_queue_topic_priority_id on pgmq_pending_queue (
                                                                             topic,
                                                                             priority desc,
                                                                             id asc
    );

-- 为待处理消息队列表添加注释
COMMENT ON TABLE pgmq_pending_queue IS '待处理消息队列表：存储等待被消费的消息';
COMMENT ON COLUMN pgmq_pending_queue.id IS '消息唯一标识';
COMMENT ON COLUMN pgmq_pending_queue.create_time IS '消息创建时间';
COMMENT ON COLUMN pgmq_pending_queue.topic IS '消息主题/队列名称';
COMMENT ON COLUMN pgmq_pending_queue.priority IS '消息优先级，数值越大优先级越高';
COMMENT ON COLUMN pgmq_pending_queue.payload IS '消息内容/负载数据';
COMMENT ON COLUMN pgmq_pending_queue.attempt IS '重试次数';

-- 处理中消息队列表：存储正在被消费者处理的消息
drop table if exists pgmq_processing_queue;
create table pgmq_processing_queue
(
    id           bigint primary key,                          -- 消息唯一标识
    create_time  timestamp                not null,           -- 消息创建时间
    topic        varchar(100) collate "C" not null,           -- 消息主题/队列名称
    priority     int                      not null default 0, -- 消息优先级，数值越大优先级越高
    payload      varchar collate "C"      not null,           -- 消息内容/负载数据
    attempt      int                      not null,           -- 重试次数
    timeout_time timestamp                not null            -- 消息处理超时时间
);

-- 为处理中队列的超时时间字段创建索引，用于快速查找超时的消息
create index idx_pgmq_processing_queue_timeout_time on pgmq_processing_queue using btree (timeout_time);

-- 为处理中消息队列表添加注释
COMMENT ON TABLE pgmq_processing_queue IS '处理中消息队列表：存储正在被消费者处理的消息';
COMMENT ON COLUMN pgmq_processing_queue.id IS '消息唯一标识';
COMMENT ON COLUMN pgmq_processing_queue.create_time IS '消息创建时间';
COMMENT ON COLUMN pgmq_processing_queue.topic IS '消息主题/队列名称';
COMMENT ON COLUMN pgmq_processing_queue.priority IS '消息优先级，数值越大优先级越高';
COMMENT ON COLUMN pgmq_processing_queue.payload IS '消息内容/负载数据';
COMMENT ON COLUMN pgmq_processing_queue.attempt IS '重试次数';
COMMENT ON COLUMN pgmq_processing_queue.timeout_time IS '消息处理超时时间';

-- 死信队列表：存储处理失败且超过最大重试次数的消息
drop table if exists pgmq_dead_queue;
create table pgmq_dead_queue
(
    id          bigint primary key,                -- 消息唯一标识
    create_time timestamp                not null, -- 消息创建时间
    topic       varchar(100) collate "C" not null, -- 消息主题/队列名称
    priority    int                      not null, -- 消息优先级，数值越大优先级越高
    payload     varchar collate "C"      not null, -- 消息内容/负载数据
    attempt     int                      not null, -- 重试次数
    dead_time   timestamp                not null  -- 消息进入死信队列的时间
);

-- 为死信队列的主题字段创建索引，用于按主题查询死信消息
create index idx_pgmq_dead_queue_topic on pgmq_dead_queue (topic);

-- 为死信队列表添加注释
COMMENT ON TABLE pgmq_dead_queue IS '死信队列表：存储处理失败且超过最大重试次数的消息';
COMMENT ON COLUMN pgmq_dead_queue.id IS '消息唯一标识';
COMMENT ON COLUMN pgmq_dead_queue.create_time IS '消息创建时间';
COMMENT ON COLUMN pgmq_dead_queue.topic IS '消息主题/队列名称';
COMMENT ON COLUMN pgmq_dead_queue.priority IS '消息优先级，数值越大优先级越高';
COMMENT ON COLUMN pgmq_dead_queue.payload IS '消息内容/负载数据';
COMMENT ON COLUMN pgmq_dead_queue.attempt IS '重试次数';
COMMENT ON COLUMN pgmq_dead_queue.dead_time IS '消息进入死信队列的时间';


-- 删除已存在的函数
drop function if exists pgmq_move_timeout_and_visible_msg_to_pending_then_notify;

-- 消息队列核心处理函数：将超时和可见的消息移动到待处理队列并发送通知
-- 返回值：包含有消息可用的主题列表
CREATE OR REPLACE FUNCTION pgmq_move_timeout_and_visible_msg_to_pending_then_notify()
    RETURNS TABLE(_topic varchar(100))
    LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
BEGIN
    -- 1. 事务级咨询锁，防止并发执行
    IF NOT pg_try_advisory_xact_lock(1997, 38) THEN
        RETURN;  -- 拿不到锁就空返回
    END IF;

    -- 2. 原子性删除 processing_queue/invisible_queue 并插入 pending_queue，
    --    同时通过 RETURNING 收集本次实际插入的 topic
    FOR rec IN
        WITH
            moved_processing AS (
                DELETE FROM pgmq_processing_queue
                    WHERE timeout_time <= now()
                    RETURNING id, create_time, topic, priority, payload, attempt
            ),
            moved_visible AS (
                DELETE FROM pgmq_invisible_queue
                    WHERE visible_time <= now()
                    RETURNING id, create_time, topic, priority, payload, attempt
            ),
            moved AS (
                SELECT * FROM moved_processing
                UNION ALL
                SELECT * FROM moved_visible
            ),
            insert_op AS (
                INSERT INTO pgmq_pending_queue (id, create_time, topic, priority, payload, attempt)
                    SELECT id, create_time, topic, priority, payload, attempt
                    FROM moved
                    RETURNING topic
            )
        SELECT DISTINCT topic
        FROM insert_op
        LOOP
            -- 3. 对每个本次搬运的主题发送通知
            PERFORM pg_notify('pgmq_topic_channel', rec.topic);
            -- 4. 将主题作为返回值返回
            _topic := rec.topic;
            RETURN NEXT;
        END LOOP;

    -- 5. 如果没有任何搬运，则函数直接结束，返回空结果集
END;
$$;

COMMENT ON FUNCTION pgmq_move_timeout_and_visible_msg_to_pending_then_notify()
    IS '将超时和可见的消息原子地移入 pending_queue，仅对本次搬运的主题发送 pg_notify，并返回这些主题列表';
