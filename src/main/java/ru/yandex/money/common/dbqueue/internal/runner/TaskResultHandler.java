package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Обработчик результат выполенения задачи
 *
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskResultHandler {

    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueShard queueShard;
    @Nonnull
    private final ReenqueueRetryStrategy reenqueueRetryStrategy;

    /**
     * Конструктор
     *
     * @param location местоположение очереди
     * @param queueShard шард на котором происходит обработка задачи
     * @param reenqueueRetryStrategy стратегия для переоткладывания задач
     */
    TaskResultHandler(@Nonnull QueueLocation location,
                      @Nonnull QueueShard queueShard,
                      @Nonnull ReenqueueRetryStrategy reenqueueRetryStrategy) {
        this.location = Objects.requireNonNull(location);
        this.queueShard = Objects.requireNonNull(queueShard);
        this.reenqueueRetryStrategy = Objects.requireNonNull(reenqueueRetryStrategy);
    }

    /**
     * Обработать результат выполнения задачи
     *
     * @param taskRecord      обработанная задача
     * @param executionResult результат обработки
     */
    void handleResult(@Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult) {
        Objects.requireNonNull(taskRecord);
        Objects.requireNonNull(executionResult);
        switch (executionResult.getActionType()) {
            case FINISH:
                queueShard.getTransactionTemplate().execute(status ->
                        queueShard.getQueueDao().deleteTask(location, taskRecord.getId()));
                return;

            case REENQUEUE:
                queueShard.getTransactionTemplate().execute(status ->
                        queueShard.getQueueDao().reenqueue(
                                location,
                                taskRecord.getId(),
                                executionResult.getExecutionDelay()
                                        .orElseGet(() -> reenqueueRetryStrategy.calculateDelay(taskRecord))
                        )
                );
                return;
            case FAIL:
                return;

            default:
                throw new IllegalStateException("unknown action type: " + executionResult.getActionType());
        }
    }
}
