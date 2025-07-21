-- status : PENDING,PROCESSING,COMPLETED,DEAD
create table
    pgq_message (
        id bigint generated always as identity primary key,
        create_time timestamp not null,
        payload text not null,
        topic varchar(100) not null,
        status varchar(20) not null,
        next_process_time timestamp not null,
        priority int not null,
        attempt int,
        max_attempt int
    );

create index idx_pgq_message_topic_status_next_process_time_priority_id on pgq_message (
    topic,
    status,
    next_process_time,
    priority desc,
    id asc
);


