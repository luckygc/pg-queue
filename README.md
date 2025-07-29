# pg-queue

基于PostgreSQL的轻量级消息队列库，提供延时消息、优先级队列和重试机制。支持集群

## 特性

- **持久化存储**: 使用PostgreSQL数据库存储消息，保证消息不丢失
- **延时消息**: 支持指定延时时间的消息处理，通过定时任务实现（1分钟间隔）
- **优先级队列**: 支持消息优先级，高优先级消息优先处理
- **重试机制**: 支持消息处理失败后的延时重试
- **死信队列**: 支持将处理失败的消息移动到死信队列
- **集群支持**: 可选的PostgreSQL NOTIFY机制，支持集群环境下的消息通知
- **批量操作**: 支持批量发送和拉取消息

## 环境要求

- Java 17+
- PostgreSQL 9.5+
- Spring jdbc

## 快速开始

### 1. 添加依赖

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.luckygc</groupId>
    <artifactId>pg-queue</artifactId>
    <version>1.1.0</version>
</dependency>
```

### 2. 创建数据库表

执行 `src/main/resources/ddl.sql` 中的SQL语句创建必要的数据库表和函数：

### 3. 基础使用

```java
// 创建队列管理器
QueueManager queueManager = new QueueManagerImpl(
    new JdbcTemplate(dataSource),
    new TransactionTemplate(transactionManager)
);

// 使用PgmqManager（推荐方式）
PgmqManager pgmqManager = new PgmqManagerImpl(jdbcTemplate);

// 发送普通消息
pgmqManager.queue().send("order", "{\"orderId\": 123}");

// 发送延时消息（5分钟后处理）
pgmqManager.delayQueue().send("order", "{\"orderId\": 124}", Duration.ofMinutes(5));

// 发送优先级消息
pgmqManager.priorityQueue().send("order", "{\"orderId\": 125}", 10);
```

### 4. 消息处理

```java
// 实现消息处理器
MessageHandler handler = new MessageHandler() {
    @Override
    public String topic() {
        return "order";
    }

    @Override
    public int threadCount() {
        return 8; // 处理线程数，默认为1
    }

    @Override
    public void handle(Message message) {
        try {
            // 处理消息逻辑
            processOrder(message.getPayload());
            message.delete(); // 处理成功，删除消息
        } catch (RetryableException e) {
            // 可重试异常
            if (message.getAttempt() >= 3) {
                message.dead(); // 超过重试次数，进入死信队列
            } else {
                message.retry(Duration.ofMinutes(10)); // 10分钟后重试
            }
        } catch (Exception e) {
            // 不可重试异常，直接进入死信队列
            message.dead();
        }
    }
};

// 注册处理器并启动
PgmqManager pgmqManager = new PgmqManagerImpl(jdbcTemplate);
pgmqManager.registerHandler(handler);
pgmqManager.start();
```

### 5. 手动拉取消息

```java
// 手动拉取单条消息
Message message = pgmqManager.queue().poll("order");
if (message != null) {
    // 处理消息
    processOrder(message.getPayload());
    message.delete();
}
```

## Spring Boot集成

### 1. 启用自动配置

```java
@Configuration
public class PgmqConfig {

    @Bean
    public void pgmqManager(JdbcTemplate jdbcTemplate){
       return new PgmqManagerImpl(jdbcTemplate);
    }
}
```

### 2. 注入使用

简单使用

```java
@Service
public class OrderService {

    @Autowired
    private QueueManager queueManager;

    public void createOrder(Order order) {
        // 业务逻辑
        saveOrder(order);

        // 发送消息（自动参与事务）
        queueManager.send("order", order.toJsonString());
    }
}
```

其他用法

```java
public class Demo {

    private PgmqManager pgmqManager;

    void demo() {
        pgmqManager.registerHandler(new TestMessageHandler());
        pgmqManager.registerHandler(new Test2MessageHandler());

        try {
            pgmqManager.start();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        pgmqManager.queue().send("test", "hello");
        pgmqManager.queue().send("test2", "hello");
        pgmqManager.delayQueue().send("test", "hello2", Duration.ofMinutes(15));
        pgmqManager.priorityQueue().send("test2", "hello3", 2);

        pgmqManager.queue().send("test3", "xxx");
        Message message = pgmqManager.queue().poll("test3");
        if (message != null) {
            message.delete();
        }
    }

    static class TestMessageHandler implements MessageHandler {

        @Override
        public String topic() {
            return "test";
        }

        @Override
        public void handle(Message message) {
            try {
                String payload = message.getPayload();
                // handle
                message.delete();
            } catch (IllegalStateException e) {
                if (message.getAttempt() >= 3) {
                    message.dead();
                } else {
                    message.retry(Duration.ofMinutes(10));
                }
            } catch (Exception e) {
                message.dead();
            }
        }
    }

    static class Test2MessageHandler implements MessageHandler {

        @Override
        public String topic() {
            return "test2";
        }

        @Override
        public int threadCount() {
            int cpuCores = Runtime.getRuntime().availableProcessors();
            return cpuCores * 2 + 1;
        }

        @Override
        public void handle(Message message) {
            try {
                String payload = message.getPayload();
                // handle
                message.delete();
            } catch (IllegalStateException e) {
                if (message.getAttempt() >= 3) {
                    message.dead();
                } else {
                    message.retry(Duration.ofDays(1));
                }
            } catch (Exception e) {
                message.dead();
            }
        }
    }
}
```

## 高级配置

### 启用PostgreSQL NOTIFY

启用NOTIFY机制可以在集群环境下实现实时消息通知：

```java
// 创建支持NOTIFY的管理器
PgmqManager pgmqManager = new PgmqManagerImpl(
    jdbcTemplate,
    "jdbc:postgresql://localhost:5432/db",
    "username",
    "password"
);
```

启用NOTIFY后，当有新消息时，集群中的所有节点都能实时收到通知。

## 工作原理

1. **消息发送**: 普通消息直接进入`pending_queue`，延时消息进入`invisible_queue`
2. **定时调度**: 每分钟执行一次定时任务，将到期的延时消息和超时的处理中消息移回`pending_queue`
3. **消息消费**: 消费者从`pending_queue`拉取消息，消息移入`processing_queue`
4. **消息处理**: 处理成功删除消息，处理失败可重试或移入死信队列

## 许可证

Apache License 2.0
