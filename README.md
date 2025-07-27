# pg-queue

基于PostgreSQL的消息队列库，支持延时消息、优先级队列、重试机制。

## 特性

- **可靠性**: 基于PostgreSQL ACID特性，保证消息不丢失
- **延时消息**: 支持指定延时时间的消息处理,误差一分钟
- **优先级队列**: 支持消息优先级，高优先级消息优先处理
- **重试机制**: 支持消息处理失败延时重试
- **死信队列**: 支持手动发送消息到死信队列
- **集群**: 可选的PostgreSQL NOTIFY机制，实现集群消息可用推送
- **批处理**: 支持批量操作

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

执行 `src/main/resources/ddl.sql` 中的SQL语句创建必要的数据库表。

### 3. 基础使用

```java
// 创建队列管理器
QueueManager queueManager = new QueueManagerImpl(
    new JdbcTemplate(dataSource),
    new TransactionTemplate(transactionManager)
);

// 发送消息
queueManager.queue("order").push("{\"orderId\": 123}");

// 发送延时消息
queueManager.queue("order").push("{\"orderId\": 124}", Duration.ofMinutes(5));

// 发送优先级消息
queueManager.queue("order").push("{\"orderId\": 125}", 10);
```

### 4. 消息处理

```java
// 单消息处理器
SingleMessageHandler handler = new SingleMessageHandler() {
    @Override
    public String topic() {
        return "order";
    }
    
    @Override
    public int threadCount() {
        return 8; // 处理线程数
    }
    
    @Override
    public void handle(ProcessingMessageManager manager, Message message) {
        try {
            // 处理消息逻辑
            processOrder(message.getPayload());
            manager.delete(message); // 处理成功，删除消息
        } catch (RetryableException e) {
            // 可重试异常，重试处理
            if (message.getAttempt() >= 3) {
                manager.dead(message); // 超过重试次数，进入死信队列
            } else {
                manager.retry(message, Duration.ofMinutes(10)); // 10分钟后重试
            }
        } catch (Exception e) {
            // 不可重试异常，直接进入死信队列
            manager.dead(message);
        }
    }
};

// 注册处理器
queueManager.registerMessageHandler(handler);

// 启动队列管理器
queueManager.start(10); // 10秒轮询间隔
```
也可以手动pull消息然后处理

## API参考

### QueueManager

队列管理器，负责队列创建、消息处理器注册和生命周期管理。

```java
// 获取指定主题的队列
DatabaseQueue queue(String topic);

// 注册单消息处理器
void registerMessageHandler(SingleMessageHandler handler);

// 注册批量消息处理器  
void registerMessageHandler(BatchMessageHandler handler);

// 启动队列管理器
void start(long loopIntervalSeconds);

// 停止队列管理器
void stop();

// 获取线程池状态
Map<String, String> getThreadPoolStatus();
```

### DatabaseQueue

数据库队列，提供消息的发送和拉取功能。

```java
// 发送消息
void push(String message);
void push(String message, Duration processDelay);
void push(String message, int priority);
void push(List<String> messages);

// 拉取消息
Message pull();
Message pull(Duration processTimeout);
List<Message> pull(int pullCount);
```

### ProcessingMessageManager

消息处理管理器，用于处理消息的状态变更。

```java
// 删除消息（处理成功）
void delete(Message message);

// 完成消息（保留记录）
void complete(Message message);

// 重试消息
void retry(Message message, Duration retryDelay);

// 进入死信队列
void dead(Message message);
```

## 高级配置

### 启用PostgreSQL NOTIFY

```java
QueueManager queueManager = new QueueManagerImpl(
    jdbcTemplate,
    transactionTemplate,
    "jdbc:postgresql://localhost:5432/mydb", // 数据库连接URL
    "username",
    "password"
);
```

启用NOTIFY后，集群其他机器也可以实时收到消息可用通知。

### 批量消息处理

```java
BatchMessageHandler batchHandler = new BatchMessageHandler() {
    @Override
    public String topic() {
        return "batch-order";
    }
    
    @Override
    public int pullCount() {
        return 100; // 每次拉取100条消息
    }
    
    @Override
    public void handle(ProcessingMessageManager manager, List<Message> messages) {
        // 批量处理逻辑
        processBatchOrders(messages);
        
        // 批量删除
        manager.delete(messages);
    }
};
```

## 数据库表结构

- `pgq_pending_queue`: 待处理消息队列
- `pgq_invisible_queue`: 延时消息队列  
- `pgq_processing_queue`: 处理中消息队列
- `pgq_complete_queue`: 已完成消息队列
- `pgq_dead_queue`: 死信消息队列

## 许可证

Apache License 2.0
