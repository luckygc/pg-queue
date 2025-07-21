insert into pgq_message (create_time,
                         payload,
                         topic,
                         status,
                         next_process_time,
                         priority,
                         attempt,
                         max_attempt)
values (:createTime,
        :payload,
        :topic,
        :status,
        :nextProcessTime,
        :priority,
        :attempt,
        :maxAttempt);


update pgq_message
set status            = :status,
    next_process_time = :nextProcessTime,
    attempt           = :attempt
where id = :id;

with messages (id) as (select id
                       from pgq_message
                       where topic = :topic
                         and status = 'PENDING'
                         and next_process_time <= :nextProcessTime
                       order by id, priority desc
                       limit :limit for update skip locked),
     op as (
         update pgq_message set status = 'PROCESSING' where id in (select id from messages))
select *
from pgq_message
where id in (select id from messages)
