package com.mz.example.processing;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;

@Getter
@Setter
public class ProcessingProperties {

    @Min(1)
    private int maxChunkSize = 1000;
    @Min(1)
    private int processingThreadsCount = 10;
    @NotEmpty
    private String fileEncoding = "UTF-8";
    @Getter(AccessLevel.NONE)
    private Integer fileProcessingResultTimeoutSeconds = 5;
    @Getter(AccessLevel.NONE)
    private Integer fileProcessingResultTimeoutMillis;

    /**
     * @return Max duration to wait between subsequent rows, before unlocking file processing flow.
     */
    public Duration getFileProcessingResultTimeout() {
        if(fileProcessingResultTimeoutMillis != null) {
            return Duration.ofMillis(fileProcessingResultTimeoutMillis);
        } else {
            return Duration.ofSeconds(fileProcessingResultTimeoutSeconds);
        }
    }
}
