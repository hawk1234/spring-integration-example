package com.mz.example.processing;

import java.io.File;
import java.util.concurrent.CompletableFuture;

public interface ProcessingGateway {

    CompletableFuture<FileProcessingResultDescriptor> processFile(File file);
}
