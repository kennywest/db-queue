package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.ReenqueueRetrySettings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Стратегия по вычислению задержки перед следующим выполнением задачи в случае, если задачу требуется вернуть в очередь.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
interface ReenqueueRetryStrategy {

    /**
     * Вычисляет задержку перед следующим выполнением задачи.
     *
     * @param taskRecord информация о задаче
     * @return задержка
     */
    @Nonnull
    Duration calculateDelay(@Nonnull TaskRecord taskRecord);

    /**
     * Создает стратегию на основе переданных настроек переоткладывания задач для очереди.
     *
     * @param reenqueueRetrySettings настройки переоткладывания задач
     * @return стратегия
     */
    @Nonnull
    static ReenqueueRetryStrategy create(@Nonnull ReenqueueRetrySettings reenqueueRetrySettings) {
        Objects.requireNonNull(reenqueueRetrySettings, "reenqueueRetrySettings");

        switch (reenqueueRetrySettings.getType()) {
            case MANUAL:
                return new ManualReenqueueRetryStrategy();
            case FIXED:
                return new FixedDelayReenqueueRetryStrategy(reenqueueRetrySettings.getFixedDelayOrThrow());
            case SEQUENTIAL:
                return new SequentialReenqueueRetryStrategy(reenqueueRetrySettings.getSequentialPlanOrThrow());
            case ARITHMETIC:
                return new ArithmeticReenqueueRetryStrategy(
                        reenqueueRetrySettings.getInitialDelay(),
                        reenqueueRetrySettings.getArithmeticStep()
                );
            case GEOMETRIC:
                return new GeometricReenqueueRetryStrategy(
                        reenqueueRetrySettings.getInitialDelay(),
                        reenqueueRetrySettings.getGeometricRatio()
                );
            default:
                throw new IllegalArgumentException("unknown re-enqueue retry type: type=" + reenqueueRetrySettings.getType());
        }
    }

    /**
     * Стратегия, которая не вычисляет задержку. Используется в случае, если продолжительность задержки выбирается
     * пользователем для каждого выполнения задачи отдельно.
     */
    class ManualReenqueueRetryStrategy implements ReenqueueRetryStrategy {

        @Nonnull
        @Override
        public Duration calculateDelay(@Nonnull TaskRecord taskRecord) {
            throw new UnsupportedOperationException(
                    "re-enqueue delay must be set explicitly via 'reenqueue(Duration)' method call"
            );
        }
    }

    /**
     * Стратегия, которая возвращает фиксированную задержку для любого выполнения задачи.
     */
    class FixedDelayReenqueueRetryStrategy implements ReenqueueRetryStrategy {

        @Nonnull
        private final Duration delay;

        FixedDelayReenqueueRetryStrategy(@Nonnull Duration delay) {
            this.delay = Objects.requireNonNull(delay, "delay");
        }

        @Nonnull
        @Override
        public Duration calculateDelay(@Nonnull TaskRecord taskRecord) {
            return delay;
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании некоторой конечной последовательности.
     * Если количество попыток выполнения задачи превышает размер последовательности, будет возвращен ее последний
     * элемент.
     */
    class SequentialReenqueueRetryStrategy implements ReenqueueRetryStrategy {

        @Nonnull
        private final List<Duration> retryPlan;

        SequentialReenqueueRetryStrategy(@Nonnull List<Duration> retryPlan) {
            this.retryPlan = Collections.unmodifiableList(retryPlan);
        }

        @Nonnull
        @Override
        public Duration calculateDelay(@Nonnull TaskRecord taskRecord) {
            if (taskRecord.getReenqueueAttemptsCount() >= retryPlan.size()) {
                return retryPlan.get(retryPlan.size() - 1);
            }
            return retryPlan.get((int) taskRecord.getReenqueueAttemptsCount());
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании арифметической прогрессии, заданной с помощью ее
     * первого члена и разности.
     */
    class ArithmeticReenqueueRetryStrategy implements ReenqueueRetryStrategy {

        @Nonnull
        private final Duration initialDelay;
        @Nonnull
        private final Duration step;

        ArithmeticReenqueueRetryStrategy(@Nonnull Duration initialDelay, @Nonnull Duration step) {
            this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
            this.step = Objects.requireNonNull(step, "step");
        }

        @Nonnull
        @Override
        public Duration calculateDelay(@Nonnull TaskRecord taskRecord) {
            return initialDelay.plus(step.multipliedBy(taskRecord.getReenqueueAttemptsCount()));
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании геометрической прогрессии, заданной с помощью ее первого
     * члена и целочисленного знаменателя.
     */
    class GeometricReenqueueRetryStrategy implements ReenqueueRetryStrategy {

        @Nonnull
        private final Duration initialDelay;
        private final long ratio;

        GeometricReenqueueRetryStrategy(@Nonnull Duration initialDelay, long ratio) {
            this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
            this.ratio = ratio;
        }

        @Nonnull
        @Override
        public Duration calculateDelay(@Nonnull TaskRecord taskRecord) {
            return initialDelay.multipliedBy((long) Math.pow(ratio, taskRecord.getReenqueueAttemptsCount()));
        }
    }
}
