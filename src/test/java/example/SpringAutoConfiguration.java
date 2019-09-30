package example;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.SpringQueueCollector;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConsumer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueShardRouter;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringAutoConfiguration.Base.class, SpringAutoConfiguration.Client.class})
public class SpringAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SpringAutoConfiguration.class);

    private static CountDownLatch latch = new CountDownLatch(9);

    private static final QueueId EXAMPLE_QUEUE = new QueueId("example_queue");

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void spring_auto_config() throws Exception {
        Assert.assertTrue(true);

        QueueProducer<String> producer = (QueueProducer<String>) applicationContext.getBean("exampleProducer");

        producer.enqueue(EnqueueParams.create("dit is een test1").withPriority(2));
        producer.enqueue(EnqueueParams.create("dit is een test2").withPriority(7));
        producer.enqueue(EnqueueParams.create("dit is een test3").withPriority(6));
        producer.enqueue(EnqueueParams.create("dit is een test4").withPriority(5));
        producer.enqueue(EnqueueParams.create("dit is een test5").withPriority(4));
        producer.enqueue(EnqueueParams.create("dit is een test6").withPriority(3));
        producer.enqueue(EnqueueParams.create("dit is een test7").withPriority(2));
        producer.enqueue(EnqueueParams.create("dit is een test8").withPriority(1));
        producer.enqueue(EnqueueParams.create("dit is een test9").withPriority(0));

        latch.await();
    }

    @ContextConfiguration
    public static class Client {
        @Bean
        QueueConsumer<String> exampleQueue() {
            return new SpringQueueConsumer<String>(EXAMPLE_QUEUE, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    logger.info("EXECUTING: " + task.getPayload());
                    latch.countDown();
                    return TaskExecutionResult.finish();
                }
            };
        }

        @Bean
        QueueProducer<String> exampleProducer() {
            return new SpringTransactionalProducer<>(EXAMPLE_QUEUE, String.class);
        }

        @Bean
        QueueShardRouter<String> exampleShardRouter() {
            return new SpringQueueShardRouter<String>(EXAMPLE_QUEUE, String.class) {

                private final QueueShard singleShard = new QueueShard(new QueueShardId("master"),
                        QueueDatabaseInitializer.getJdbcTemplate(),
                        QueueDatabaseInitializer.getTransactionTemplate());

                @Nonnull
                @Override
                public Collection<QueueShard> getProcessingShards() {
                    return Collections.singletonList(singleShard);
                }

                @Nonnull
                @Override
                public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<String> enqueueParams) {
                    return singleShard;
                }
            };
        }

        @Bean
        TaskPayloadTransformer<String> examplePayloadTransformer() {
            return new SpringNoopPayloadTransformer(EXAMPLE_QUEUE);
        }
    }

    @ContextConfiguration
    public static class Base {

        @Bean
        SpringQueueConfigContainer springQueueConfigContainer() {
            QueueDatabaseInitializer.createTable("example_spring_table");
            return new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(
                    QueueLocation.builder().withTableName("example_spring_table")
                            .withQueueId(EXAMPLE_QUEUE).build(),
                    QueueSettings.builder()
                            .withBetweenTaskTimeout(Duration.ofMillis(1000L))
                            .withNoTaskTimeout(Duration.ofSeconds(10L))
                            .build())));
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }

        @Bean
        SpringQueueInitializer springQueueInitializer(SpringQueueConfigContainer springQueueConfigContainer,
                                                      SpringQueueCollector springQueueCollector) {
            return new SpringQueueInitializer(springQueueConfigContainer, springQueueCollector,
                    new QueueExecutionPool(new QueueRegistry(), new EmptyTaskListener(), new EmptyListener()));
        }
    }

}
