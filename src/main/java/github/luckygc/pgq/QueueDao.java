package github.luckygc.pgq;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;

public class QueueDao {

    private static final Function<ResultSet, Message> MESSAGE_ENTITY_ROW_MAPPER = rs -> {
        try {
            Message message = new Message();
            message.setId(rs.getLong("id"));
            message.setCreateTime(rs.getTimestamp("create_time").toLocalDateTime());
            message.setUpdateTime(rs.getTimestamp("update_time").toLocalDateTime());
            message.setVisibleTime(rs.getTimestamp("visible_time").toLocalDateTime());
            message.setTopic(rs.getString("topic"));
            message.setPriority(rs.getInt("priority"));
            message.setPayload(rs.getString("payload"));
            message.setAttempt(rs.getInt("attempt"));
            return message;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public QueueDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertMessageEntity(@Nullable Message message) {
        if (message == null) {
            return;
        }

        jdbcTemplate.execute(insertPsCreator(message), PreparedStatement::executeUpdate);
    }

    private PreparedStatementCreator insertPsCreator(Message message) {
        return con -> {
            String sql = """
                    insert into pgq_queue
                        (create_time, update_time, visible_time, status, topic, priority, payload, attempt)
                        values($1, $2, $3, $4, $5, $6, $7, $8)
                    """;
            PreparedStatement ps = con.prepareStatement(sql);

            ps.setTimestamp(1, Timestamp.valueOf(message.getCreateTime()));
            ps.setTimestamp(2, Timestamp.valueOf(message.getUpdateTime()));
            ps.setTimestamp(3, Timestamp.valueOf(message.getVisibleTime()));
            ps.setString(4, MessageStatus.PENDING.name());
            ps.setString(5, message.getTopic());
            ps.setInt(6, message.getPriority());
            ps.setString(7, message.getPayload());
            ps.setInt(8, message.getAttempt());

            return ps;
        };
    }

    public void insertMessageEntities(@Nullable List<Message> messageEntities) {
        if (messageEntities == null || messageEntities.isEmpty()) {
            return;
        }

        if (messageEntities.size() == 1) {
            insertMessageEntity(messageEntities.get(0));
            return;
        }

        jdbcTemplate.execute(batchInsertPsCreator(messageEntities), PreparedStatement::executeUpdate);
    }

    private PreparedStatementCreator batchInsertPsCreator(List<Message> messageEntities) {
        return con -> {
            String sql = """
                    insert into pgq_queue
                    (create_time, update_time, visible_time, status, topic, priority, payload, attempt)
                    select * from unnest($1, $2, $3, $4, $5, $6, $7, $8)
                    as t
                    (create_time, update_time, visible_time, status, topic, priority, payload, attempt)
                    """;
            PreparedStatement ps = con.prepareStatement(sql);

            // 准备数组数据
            int size = messageEntities.size();
            Timestamp[] createTimes = new Timestamp[size];
            Timestamp[] updateTimes = new Timestamp[size];
            String[] payloads = new String[size];
            String[] topics = new String[size];
            String[] statuses = new String[size];
            Timestamp[] visibleTimes = new Timestamp[size];
            Integer[] priorities = new Integer[size];
            Integer[] attempts = new Integer[size];
            int i = 0;
            // 填充数组数据
            for (Message entity : messageEntities) {
                createTimes[i] = Timestamp.valueOf(entity.getCreateTime());
                updateTimes[i] = Timestamp.valueOf(entity.getUpdateTime());
                visibleTimes[i] = Timestamp.valueOf(entity.getVisibleTime());
                statuses[i] = MessageStatus.PENDING.name();
                topics[i] = entity.getTopic();
                priorities[i] = entity.getPriority();
                payloads[i] = entity.getPayload();
                attempts[i] = entity.getAttempt();
                i++;
            }

            // 绑定参数
            ps.setArray(1, con.createArrayOf("timestamp", createTimes));
            ps.setArray(2, con.createArrayOf("timestamp", updateTimes));
            ps.setArray(3, con.createArrayOf("timestamp", visibleTimes));
            ps.setArray(4, con.createArrayOf("varchar", statuses));
            ps.setArray(5, con.createArrayOf("varchar", topics));
            ps.setArray(6, con.createArrayOf("integer", priorities));
            ps.setArray(7, con.createArrayOf("varchar", payloads));
            ps.setArray(8, con.createArrayOf("integer", attempts));

            return ps;
        };
    }

    public void updateMessageEntity(Message message) {

    }

    public List<Message> findWaitHandleMessageEntities(String topic, long limit) {
        return null;
    }

    public long deleteByStatus(String topic, MessageStatus status) {
        return 0;
    }
}
