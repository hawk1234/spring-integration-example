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
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
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

    //<editor-fold desc="Integration flows" defaultstate="collapsed">
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
                                .route(String.class, this::filteringRouter, filterMapping -> filterMapping
                                        .subFlowMapping(ProcessingResult.FILTERED, sf -> sf
                                                .enrichHeaders(enricher -> enricher.header(ProcessingConstants.PROCESSING_RESULT_HEADER, ProcessingResult.FILTERED, true))
                                                .channel(ProcessingConstants.AGGREGATION_CHANNEL))
                                        .subFlowMapping(ProcessingResult.PROCESSED, sf -> sf
                                                .transform(String.class, s -> {
                                                    if (s.startsWith("ERROR")) {
                                                        throw new IllegalArgumentException("Throw this to simulate error while transforming row.");
                                                    }
                                                    return s;
                                                }, e -> e.id("rowTransformer"))
                                                .channel(ProcessingConstants.KAFKA_SEND_CHANNEL))))
                        .subFlowMapping(FileMarker.class, subflow -> subflow
                                .channel(ProcessingConstants.AGGREGATION_CHANNEL)))
                .get();
    }

    @Bean
    public IntegrationFlow kafkaSendFlow(KafkaTemplate<?, ?> kafkaTemplate) {
        return IntegrationFlows.from(ProcessingConstants.KAFKA_SEND_CHANNEL)
                .enrichHeaders(enricher -> enricher.header(MessageHeaders.ERROR_CHANNEL, ProcessingConstants.KAFKA_ERROR_CHANNEL, true))
                .enrichHeaders(enricher -> enricher.headerFunction(KafkaHeaders.MESSAGE_KEY, message -> "EXAMPLE_KAFKA_KEY"))
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)
                                .sendFailureChannel(ProcessingConstants.KAFKA_ERROR_CHANNEL)
                                .sendSuccessChannel(ProcessingConstants.AGGREGATION_CHANNEL),
                        e -> e.id(ProcessingConstants.KAFKA_ADAPTER))
                .nullChannel();
    }

    @Bean
    public IntegrationFlow aggregationFlow(ProcessingProperties processingProperties,
                                           MessageGroupStore messageGroupStore) {
        return IntegrationFlows.from(ProcessingConstants.AGGREGATION_CHANNEL)
                .aggregate(aggregatorSpec -> aggregatorSpec
                        .releaseStrategy(MessageGroup::isComplete)
                        .correlationStrategy(message -> message.getHeaders().get(FileHeaders.FILENAME))
                        .messageStore(messageGroupStore)
                        .outputProcessor(mg -> ((RowCountingMessageGroup) mg).getProcessingResult())
                        .sendPartialResultOnExpiry(true)
                        .groupTimeout(processingProperties.getFileProcessingResultTimeout().toMillis()))
                .logAndReply(Level.INFO, msg -> "Produced aggregated message. End of flow for file: "
                        + ((FileProcessingResultDescriptor) msg.getPayload()).getFileName());
    }

    @Bean
    public IntegrationFlow processingErrorFlow() {
        return createErrorFlow(ProcessingConstants.PROCESSING_ERROR_CHANNEL, ProcessingResult.PROCESSING_ERROR);
    }

    @Bean
    public IntegrationFlow kafkaErrorFlow() {
        return createErrorFlow(ProcessingConstants.KAFKA_ERROR_CHANNEL, ProcessingResult.KAFKA_ERROR);
    }
    //</editor-fold>

    //<editor-fold desc="Spring integration beans" defaultstate="collapsed">
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

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerSpec poller() {
        return Pollers.fixedDelay(10, TimeUnit.MILLISECONDS);
    }
    //</editor-fold>

    //<editor-fold desc="Private helpers" defaultstate="collapsed">
    private IntegrationFlow createErrorFlow(String sourceErrorChannel, ProcessingResult channelCorrelatedResult) {
        return IntegrationFlows.from(sourceErrorChannel)
                .transform(Message.class, msg -> {
                    if(msg instanceof ErrorMessage) {
                        return ((MessagingException) msg.getPayload()).getFailedMessage();
                    } else {
                        return msg;
                    }
                }, e -> e.id(sourceErrorChannel + "OriginalMessageTransformer"))
                .enrichHeaders(enricher -> enricher.header(ProcessingConstants.PROCESSING_RESULT_HEADER, channelCorrelatedResult, true))
                .channel(ProcessingConstants.AGGREGATION_CHANNEL)
                .get();
    }

    private ProcessingResult filteringRouter(String messagePayload) {
        if(messagePayload.startsWith("FILTER")){
            return ProcessingResult.FILTERED;
        } else {
            return ProcessingResult.PROCESSED;
        }
    }
    //</editor-fold>
}
