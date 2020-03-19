package com.mz.example.processing;

import lombok.Value;

@Value
public class FileProcessingResultDescriptor {

    private String fileName;
    private long expectedNumberOfRows;
    private long numberOfRows;
    private long numberOfProcessed;
    private long numberOfFiltered;
    private long numberOfProcessingErrors;
    private long numberOfKafkaErrors;

    public boolean isValid() {
        return expectedNumberOfRows == numberOfRows;
    }
}
