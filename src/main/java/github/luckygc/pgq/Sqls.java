package github.luckygc.pgq;

public final class Sqls {

    private Sqls() {
    }

    public static final String INSERT = """
            insert into pgq_message (
                    create_time,
                    payload,
                    topic,
                    status,
                    next_process_time,
                    priority,
                    attempt,
                    max_attempt
                )
                values (
                    :createTime,
                    :payload,
                    :topic,
                    :status,
                    :nextProcessTime,
                    :priority,
                    :attempt,
                    :maxAttempt
                )
            """;

    public static final String UPDATE = """
            update pgq_message
                 set status            = :status,
                     next_process_time = :nextProcessTime,
                     attempt           = :attempt
                 where id = :id;
            """;


    public static String PULL = """
            with messages (id) as (select id
                                   from pgq_message
                                   where topic = :topic
                                     and status = 'PENDING'
                                     and next_process_time <= now()
                                   order by priority desc, id
                                   limit :limit for update skip locked),
                 op as (
                     update pgq_message set status = 'PROCESSING' where id in (select id from messages))
            select *
            from pgq_message
            where id in (select id from messages)
            """;
}
