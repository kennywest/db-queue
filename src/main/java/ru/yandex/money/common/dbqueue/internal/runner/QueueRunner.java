package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.internal.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Интерфейс обработчика пула задач очереди
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings("rawtypes")
@FunctionalInterface
public interface QueueRunner {

    /**
     * Единократно обработать заданную очередь
     *
     * @param queueConsumer очередь для обработки
     * @return тип результата выполнения задачи
     */
    @Nonnull
    QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer);

    /**
     * Фабрика исполнителей задач в очереди
     */
    final class Factory {

        private Factory() {
        }

        /**
         * Создать исполнителя задач очереди
         *
         * @param queueConsumer         очередь обработки задач
         * @param queueShard              dao взаимодействия с очередью
         * @param taskLifecycleListener слушатель исполнения задач в очереди
         * @param externalExecutor      пул через который выполняются задачи в режиме
         *                              {@link ProcessingMode
         *                              #USE_EXTERNAL_EXECUTOR}
         * @return инстанс исполнителя задач
         */
        @SuppressWarnings("rawtypes")
        public static QueueRunner createQueueRunner(@Nonnull QueueConsumer queueConsumer, @Nonnull QueueShard queueShard,
                                                    @Nonnull TaskLifecycleListener taskLifecycleListener,
                                                    @Nullable Executor externalExecutor) {
            requireNonNull(queueConsumer);
            requireNonNull(queueShard);
            requireNonNull(taskLifecycleListener);

            RetryTaskStrategy retryTaskStrategy = RetryTaskStrategy.Factory.create(queueConsumer.getQueueConfig().getSettings());
            ReenqueueRetryStrategy reenqueueRetryStrategy = ReenqueueRetryStrategy
                    .create(queueConsumer.getQueueConfig().getSettings().getReenqueueRetrySettings());

            PickTaskDao pickTaskDao = new PickTaskDao(queueShard.getShardId(),
                    queueShard.getJdbcTemplate(), queueShard.getTransactionTemplate());
            TaskPicker taskPicker = new TaskPicker(pickTaskDao, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), retryTaskStrategy);
            TaskResultHandler taskResultHandler = new TaskResultHandler(queueConsumer.getQueueConfig().getLocation(),
                    queueShard, reenqueueRetryStrategy);
            TaskProcessor taskProcessor = new TaskProcessor(queueShard, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler);
            QueueSettings settings = queueConsumer.getQueueConfig().getSettings();

            switch (settings.getProcessingMode()) {
                case SEPARATE_TRANSACTIONS:
                    return new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor);
                case WRAP_IN_TRANSACTION:
                    return new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard);
                case USE_EXTERNAL_EXECUTOR:
                    return new QueueRunnerInExternalExecutor(taskPicker, taskProcessor,
                            requireNonNull(externalExecutor));
                default:
                    throw new IllegalStateException("unknown processing mode: " + settings.getProcessingMode());
            }
        }
    }
}
