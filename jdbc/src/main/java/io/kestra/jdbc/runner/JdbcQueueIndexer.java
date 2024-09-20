package io.kestra.jdbc.runner;

import io.kestra.core.metrics.MetricRegistry;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible to index the queue synchronously at message production time.<p>
 * Some queue messages are batch-indexed asynchronously via the {@link JdbcIndexer}
 * which listen to (receive) those queue messages.
 */
@Slf4j
@Singleton
public class JdbcQueueIndexer {
    private final Map<Class<?>, JdbcQueueIndexerInterface<?>> repositories = new HashMap<>();

    private final MetricRegistry metricRegistry;

    @Inject
    public JdbcQueueIndexer(ApplicationContext applicationContext) {
        applicationContext.getBeansOfType(JdbcQueueIndexerInterface.class)
            .forEach(saveRepositoryInterface -> {
                String typeName = ((ParameterizedType) ((Class<?>) saveRepositoryInterface.getClass()
                    .getGenericSuperclass()).getGenericInterfaces()[1]).getActualTypeArguments()[0].getTypeName();

                try {
                    repositories.put(Class.forName(typeName), saveRepositoryInterface);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });

        this.metricRegistry = applicationContext.getBean(MetricRegistry.class);
    }

    public void accept(DSLContext context, Object item) {
        if (repositories.containsKey(item.getClass())) {
            this.metricRegistry.counter(MetricRegistry.METRIC_INDEXER_REQUEST_COUNT, "type", item.getClass().getName()).increment();
            this.metricRegistry.counter(MetricRegistry.METRIC_INDEXER_MESSAGE_IN_COUNT, "type", item.getClass().getName()).increment();

            this.metricRegistry.timer(MetricRegistry.METRIC_INDEXER_REQUEST_DURATION, "type", item.getClass().getName()).record(() -> {
                repositories.get(item.getClass()).save(context, cast(item));

                this.metricRegistry.counter(MetricRegistry.METRIC_INDEXER_MESSAGE_OUT_COUNT, "type", item.getClass().getName()).increment();
            });

        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> T cast(Object message) {
        return (T) message;
    }
}
