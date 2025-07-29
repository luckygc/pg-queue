package github.luckygc.pgq.tool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PgmqHandlerThreadFactoryTest {

    private PgmqHandlerThreadFactory threadFactory;
    private final String topic = "test-topic";

    @BeforeEach
    void setUp() {
        threadFactory = new PgmqHandlerThreadFactory(topic);
    }

    @Test
    void shouldCreateThreadWithCorrectName() {
        Runnable task = () -> {};
        
        Thread thread = threadFactory.newThread(task);
        
        assertThat(thread.getName()).startsWith("pgmq-handler-test-topic-");
        assertThat(thread.getName()).matches("pgmq-handler-test-topic-\\d+");
    }

    @Test
    void shouldCreateDaemonThread() {
        Runnable task = () -> {};
        
        Thread thread = threadFactory.newThread(task);
        
        assertThat(thread.isDaemon()).isTrue();
    }

    @Test
    void shouldSetUncaughtExceptionHandler() {
        Runnable task = () -> {};
        
        Thread thread = threadFactory.newThread(task);
        
        assertThat(thread.getUncaughtExceptionHandler()).isNotNull();
    }

    @Test
    void shouldIncrementThreadCount() {
        Runnable task = () -> {};
        
        Thread thread1 = threadFactory.newThread(task);
        Thread thread2 = threadFactory.newThread(task);
        Thread thread3 = threadFactory.newThread(task);
        
        assertThat(thread1.getName()).endsWith("-1");
        assertThat(thread2.getName()).endsWith("-2");
        assertThat(thread3.getName()).endsWith("-3");
    }

    @Test
    void shouldThrowExceptionWhenTopicIsNull() {
        assertThatThrownBy(() -> new PgmqHandlerThreadFactory(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldHandleEmptyTopic() {
        PgmqHandlerThreadFactory factory = new PgmqHandlerThreadFactory("");
        Runnable task = () -> {};
        
        Thread thread = factory.newThread(task);
        
        assertThat(thread.getName()).startsWith("pgmq-handler--");
    }

    @Test
    void shouldHandleSpecialCharactersInTopic() {
        PgmqHandlerThreadFactory factory = new PgmqHandlerThreadFactory("topic-with-special_chars.123");
        Runnable task = () -> {};
        
        Thread thread = factory.newThread(task);
        
        assertThat(thread.getName()).startsWith("pgmq-handler-topic-with-special_chars.123-");
    }

    @Test
    void shouldCreateThreadWithGivenRunnable() {
        boolean[] executed = {false};
        Runnable task = () -> executed[0] = true;
        
        Thread thread = threadFactory.newThread(task);
        thread.run(); // 直接调用run方法而不是start，避免并发问题
        
        assertThat(executed[0]).isTrue();
    }

    @Test
    void shouldCreateMultipleThreadsWithDifferentNames() {
        Runnable task = () -> {};
        
        Thread thread1 = threadFactory.newThread(task);
        Thread thread2 = threadFactory.newThread(task);
        Thread thread3 = threadFactory.newThread(task);
        
        assertThat(thread1.getName()).isNotEqualTo(thread2.getName());
        assertThat(thread2.getName()).isNotEqualTo(thread3.getName());
        assertThat(thread1.getName()).isNotEqualTo(thread3.getName());
    }

    @Test
    void shouldHandleNullRunnable() {
        // Thread构造函数可以接受null的Runnable
        Thread thread = threadFactory.newThread(null);
        
        assertThat(thread).isNotNull();
        assertThat(thread.getName()).startsWith("pgmq-handler-test-topic-");
    }

    @Test
    void shouldCreateThreadsWithConsistentProperties() {
        Runnable task = () -> {};
        
        for (int i = 0; i < 10; i++) {
            Thread thread = threadFactory.newThread(task);
            
            assertThat(thread.isDaemon()).isTrue();
            assertThat(thread.getUncaughtExceptionHandler()).isNotNull();
            assertThat(thread.getName()).startsWith("pgmq-handler-test-topic-");
        }
    }
}
