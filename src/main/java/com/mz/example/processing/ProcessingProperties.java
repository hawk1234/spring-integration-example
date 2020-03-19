package com.mz.example.processing;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;

@Getter
@Setter
public class ProcessingProperties {

    @Min(1)
    private int maxChunkSize = 1000;
    @Min(1)
    private int processingThreadsCount = 10;
    @NotEmpty
    private String fileEncoding = "UTF-8";
}
