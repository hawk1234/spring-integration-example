package com.mz.example.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.PollerSpec;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.splitter.FileSplitter.FileMarker;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.SimpleMessageGroupFactory;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.validation.annotation.Validated;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
@EnableIntegration
public class ProcessingConfiguration {

    @Bean
    @Validated
    @ConfigurationProperties("com.mz.example.file-processing")
    public ProcessingProperties processingProperties() {
        return new ProcessingProperties();
    }

    @Bean
    public IntegrationFlow processingFlow(ProcessingProperties processingProperties,
                                          KafkaTemplate<?, ?> kafkaTemplate) {
        return IntegrationFlows.from(ProcessingGateway.class)
                .split(Files.splitter()
                        .markers()
                        .charset(Charset.forName(processingProperties.getFileEncoding())))
                .channel(channels -> channels.queue("rowProcessingQueue", processingProperties.getMaxChunkSize()))
                .channel(channels -> channels.executor("rowProcessingExecutor",
                        Executors.newFixedThreadPool(processingProperties.getProcessingThreadsCount())))
                .enrichHeaders(enricher -> enricher.header(ProcessingConstants.PROCESSING_RESULT_HEADER, ProcessingResult.PROCESSED))
                .<Object, Class<?>>route(Object::getClass, mapping -> mapping
                        .subFlowMapping(String.class, subflow -> subflow
                                .enrichHeaders(enricher -> enricher.header(MessageHeaders.ERROR_CHANNEL, ProcessingConstants.PROCESSING_ERROR_CHANNEL, true))
                                .filter(String.class, s -> true, e -> e.id("rowFilter"))//TODO: implement some filtering. Filtered rows should go to AGGREGATION_CHANNEL
                                .transform(String.class, s -> s, e -> e.id("rowTransformer"))//TODO: implement some errors when transforming
                                .enrichHeaders(enricher -> enricher.header(MessageHeaders.ERROR_CHANNEL, ProcessingConstants.KAFKA_ERROR_CHANNEL, true))
                                .enrichHeaders(enricher -> enricher.headerFunction(KafkaHeaders.MESSAGE_KEY, message -> "EXAMPLE_KAFKA_KEY"))
                                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)//TODO: in tests implement mock handler that produces to Success or Failure Channel
                                        .sendFailureChannel(ProcessingConstants.KAFKA_ERROR_CHANNEL)
                                        .sendSuccessChannel(ProcessingConstants.AGGREGATION_CHANNEL),
                                        e -> e.id(ProcessingConstants.KAFKA_ADAPTER))
                                .nullChannel())
                        .subFlowMapping(FileMarker.class, subflow -> subflow
                                .log(Level.INFO, msg -> "======================== MARKER MSG: " + msg)
                                .channel(ProcessingConstants.AGGREGATION_CHANNEL)
                                .nullChannel()))
                .get();
    }

    @Bean
    public MessageGroupStore messageGroupStore() {
        SimpleMessageStore ret = new SimpleMessageStore();
        ret.setMessageGroupFactory(new SimpleMessageGroupFactory() {
            @Override
            public MessageGroup create(Collection<? extends Message<?>> messages, Object groupId, long timestamp, boolean complete) {
                RowCountingMessageGroup group = new RowCountingMessageGroup((String) groupId);
                group.setTimestamp(timestamp);
                if (complete) {
                    group.complete();
                }
                for (Message<?> message : messages) {
                    if(!group.canAdd(message)) {
                        throw new IllegalStateException("Initial message can't be added to the group");
                    }
                    group.add(message);
                }
                return group;
            }
        });
        return ret;
    }

    @Bean
    public IntegrationFlow aggregationFlow(MessageGroupStore messageGroupStore) {
        return IntegrationFlows.from(ProcessingConstants.AGGREGATION_CHANNEL)
                .log(Level.INFO, msg -> "================================== AGG CHANNEL: " + msg)
                .aggregate(aggregatorSpec -> aggregatorSpec
                        .releaseStrategy(MessageGroup::isComplete)
                        .correlationStrategy(message -> message.getHeaders().get(FileHeaders.FILENAME))
                        .messageStore(messageGroupStore)
                        .outputProcessor(mg -> ((RowCountingMessageGroup) mg).getProcessingResult())
                        .sendPartialResultOnExpiry(true)
                        .groupTimeout(5*1000))
                .logAndReply(Level.INFO, msg -> "Produced aggregated message. End of flow for file: "
                        + ((FileProcessingResultDescriptor) msg.getPayload()).getFileName());
    }

    @Bean
    public IntegrationFlow processingErrorFlow() {
        return createErrorFlow(ProcessingConstants.PROCESSING_ERROR_CHANNEL);
    }

    @Bean
    public IntegrationFlow kafkaErrorFlow() {
        return createErrorFlow(ProcessingConstants.KAFKA_ERROR_CHANNEL);
    }

    private IntegrationFlow createErrorFlow(String sourceErrorChannel) {
        return IntegrationFlows.from(sourceErrorChannel)//TODO: after handling is done messages should go to AGGREGATION_CHANNEL
                .log(Level.INFO, msg -> "================================= ERR CHANNEL: " + sourceErrorChannel + "; msg: " + msg)
                .nullChannel();
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerSpec poller() {
        return Pollers.fixedDelay(10, TimeUnit.MILLISECONDS);
    }
}
