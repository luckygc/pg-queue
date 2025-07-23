insert into pgq_queue (create_time,
                         update_time,
                         topic,
                         status,
                         priority,
                         attempt,
                         max_attempt)
values ($1, $2, $3, $4, $5, $6, $7);


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
where id in (select id from messages);

