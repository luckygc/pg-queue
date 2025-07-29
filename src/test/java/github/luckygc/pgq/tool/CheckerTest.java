package github.luckygc.pgq.tool;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CheckerTest {

    @Test
    void shouldNotThrowWhenMessagesIsNotEmpty() {
        List<String> messages = Arrays.asList("message1", "message2");

        assertThatCode(() -> Checker.checkMessagesNotEmpty(messages))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowWhenMessagesIsNull() {
        assertThatThrownBy(() -> Checker.checkMessagesNotEmpty(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    void shouldThrowWhenMessagesIsEmpty() {
        List<String> emptyList = Collections.emptyList();

        assertThatThrownBy(() -> Checker.checkMessagesNotEmpty(emptyList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    void shouldThrowWhenMessagesIsEmptyArrayList() {
        List<String> emptyList = new ArrayList<>();

        assertThatThrownBy(() -> Checker.checkMessagesNotEmpty(emptyList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    void shouldNotThrowWhenDurationIsPositive() {
        Duration positiveDuration = Duration.ofSeconds(1);

        assertThatCode(() -> Checker.checkDurationIsPositive(positiveDuration))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldNotThrowWhenDurationIsLarge() {
        Duration largeDuration = Duration.ofDays(365);

        assertThatCode(() -> Checker.checkDurationIsPositive(largeDuration))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowWhenDurationIsZero() {
        Duration zeroDuration = Duration.ZERO;

        assertThatThrownBy(() -> Checker.checkDurationIsPositive(zeroDuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");
    }

    @Test
    void shouldThrowWhenDurationIsNegative() {
        Duration negativeDuration = Duration.ofSeconds(-1);

        assertThatThrownBy(() -> Checker.checkDurationIsPositive(negativeDuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");
    }

    @Test
    void shouldNotThrowWhenMaxPollIsInValidRange() {
        assertThatCode(() -> Checker.checkMaxPollRange(1))
                .doesNotThrowAnyException();

        assertThatCode(() -> Checker.checkMaxPollRange(50))
                .doesNotThrowAnyException();

        assertThatCode(() -> Checker.checkMaxPollRange(5000))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowWhenMaxPollIsZero() {
        assertThatThrownBy(() -> Checker.checkMaxPollRange(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowWhenMaxPollIsNegative() {
        assertThatThrownBy(() -> Checker.checkMaxPollRange(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowWhenMaxPollIsTooLarge() {
        assertThatThrownBy(() -> Checker.checkMaxPollRange(5001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowWhenMaxPollIsVeryLarge() {
        assertThatThrownBy(() -> Checker.checkMaxPollRange(Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldHandleBoundaryValues() {
        // 测试边界值
        assertThatCode(() -> Checker.checkMaxPollRange(1))
                .doesNotThrowAnyException();

        assertThatCode(() -> Checker.checkMaxPollRange(5000))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> Checker.checkMaxPollRange(0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Checker.checkMaxPollRange(5001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldHandleMillisecondDuration() {
        Duration millisDuration = Duration.ofMillis(1);

        assertThatCode(() -> Checker.checkDurationIsPositive(millisDuration))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldHandleNanosecondDuration() {
        Duration nanosDuration = Duration.ofNanos(1);

        assertThatCode(() -> Checker.checkDurationIsPositive(nanosDuration))
                .doesNotThrowAnyException();
    }
}
