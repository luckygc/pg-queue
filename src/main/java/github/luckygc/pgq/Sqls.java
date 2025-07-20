package github.luckygc.pgq;

public final class Sqls {

    private Sqls() {
    }

    public static final String INSERT_INTO_SIMPLE_QUEUE = """
            insert into pgq_simple_queue (
                    create_time,
                    topic,
                    payload,
                    status,
                    next_process_time,
                    attempt,
                    max_attempt
                )
                values (
                    :createTime
                    :topic,
                    :payload,
                    :status,
                    :nextProcessTime,
                    :attempt,
                    :maxAttempt
                )
            """;

    public static final String PULL_WAIT_HANDLE_FROM_SIMPLE_QUEUE = """
            select * from pgq_simple_queue
                where topic = :topic
                and status = 'PENDING'
                and next_process_time <= :nextProcessTime
                order by id
                limit :limit
                for update skip locked
            """;

    public static final String UPDATE_SIMPLE_QUEUE_STATUS = """
            update pgq_simple_queue set status = :status where id = any(:ids)
            """;
}
