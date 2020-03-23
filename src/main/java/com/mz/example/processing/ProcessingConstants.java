package com.mz.example.processing;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ProcessingConstants {

    public final String KAFKA_ADAPTER = "kafkaAdapter";

    public final String KAFKA_ERROR_CHANNEL = "kafkaErrorChannel";
    public final String PROCESSING_ERROR_CHANNEL = "processingErrorChannel";
    public final String KAFKA_SEND_CHANNEL = "kafkaSendChannel";
    public final String AGGREGATION_CHANNEL = "aggregationChannel";

    public final String PROCESSING_RESULT_HEADER = "processingResultHeaderKey";
}
