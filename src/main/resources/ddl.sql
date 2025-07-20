-- statsu : PENDING,PROCESSING,COMPLETED,DEAD
create table
    pgq_simple_queue (
        id bigint generated always as identity primary key,
        create_time timestamp not null,
        payload text not null,
        topic varchar(100) not null,
        status varchar(20) not null,
        next_process_time timestamp not null ,
        attempt int,
        max_attempt int
    )
with
    (fillfactor = 70);

create index idx_pgq_simple_queue_status_next_process_time on pgq_simple_queue (status, next_process_time);
