package ru.yandex.money.common.dbqueue.internal.runner;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.ReenqueueRetrySettings;
import ru.yandex.money.common.dbqueue.settings.ReenqueueRetryType;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReenqueueRetryStrategyTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void should_throw_exception_when_calculate_delay_with_manual_strategy() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.MANUAL)
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.create(settings);

        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("re-enqueue delay must be set explicitly via 'reenqueue(Duration)' method call");

        strategy.calculateDelay(createTaskRecord(0));
    }

    @Test
    public void should_calculate_delay_when_using_fixed_delay_strategy() {
        Duration fixedDelay = Duration.ofSeconds(10L);
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.FIXED)
                .withFixedDelay(fixedDelay)
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(fixedDelay, fixedDelay, fixedDelay, fixedDelay, fixedDelay)));
    }

    @Test
    public void should_calculate_delay_when_using_sequential_strategy() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.SEQUENTIAL)
                .withSequentialPlan(Arrays.asList(Duration.ofSeconds(1L), Duration.ofSeconds(2L), Duration.ofSeconds(3L)))
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(1L),
                Duration.ofSeconds(2L),
                Duration.ofSeconds(3L),
                Duration.ofSeconds(3L),
                Duration.ofSeconds(3L)
        )));
    }

    @Test
    public void should_calculate_delay_when_using_arithmetic_strategy() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.ARITHMETIC)
                .withInitialDelay(Duration.ofSeconds(10L))
                .withArithmeticStep(Duration.ofSeconds(1L))
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(10L),
                Duration.ofSeconds(11L),
                Duration.ofSeconds(12L),
                Duration.ofSeconds(13L),
                Duration.ofSeconds(14L)
        )));
    }

    @Test
    public void should_calculate_delay_when_using_geometric_strategy() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.GEOMETRIC)
                .withInitialDelay(Duration.ofSeconds(10L))
                .withGeometricRatio(3L)
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(10L),
                Duration.ofSeconds(30L),
                Duration.ofSeconds(90L),
                Duration.ofSeconds(270L),
                Duration.ofSeconds(810L)
        )));
    }

    @Nonnull
    private static TaskRecord createTaskRecord(long reenqueueAttemptsCount) {
        return new TaskRecord(
                1L,
                "",
                0,
                reenqueueAttemptsCount,
                reenqueueAttemptsCount,
                ZonedDateTime.now(),
                ZonedDateTime.now(),
                null,
                null
        );
    }
}
