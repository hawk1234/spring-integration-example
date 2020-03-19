package com.mz.example.processing;

import lombok.RequiredArgsConstructor;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.splitter.FileSplitter.FileMarker;
import org.springframework.integration.store.MessageGroup;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class RowCountingMessageGroup implements MessageGroup {

    private final String groupId;
    private final AtomicLong expectedRowCount = new AtomicLong(-1);
    private final AtomicLong rowCount = new AtomicLong();
    private final AtomicLong processedCount = new AtomicLong();
    private final AtomicLong filteredCount = new AtomicLong();
    private final AtomicLong kafkaErrorCount = new AtomicLong();
    private final AtomicLong processingErrorCount = new AtomicLong();
    private boolean complete = false;
    private long lastModified;
    private long created = System.currentTimeMillis();
    private Object replyChannel;

    Message<FileProcessingResultDescriptor> getProcessingResult() {
        FileProcessingResultDescriptor payload = new FileProcessingResultDescriptor(
                groupId,
                expectedRowCount.get(),
                rowCount.get(),
                processedCount.get(),
                filteredCount.get(),
                processingErrorCount.get(),
                kafkaErrorCount.get()
        );
        HashMap<String, Object> headers = new HashMap<>();
        headers.put(MessageHeaders.REPLY_CHANNEL, replyChannel);
        return new GenericMessage<>(payload, headers);
    }

    @Override
    public boolean canAdd(Message<?> message) {
        return !isComplete() &&
                message.getHeaders().containsKey(FileHeaders.FILENAME) &&
                message.getHeaders().containsKey(ProcessingConstants.PROCESSING_RESULT_HEADER) &&
                groupId.equals(message.getHeaders().get(FileHeaders.FILENAME, String.class));
    }

    @Override
    public void add(Message<?> messageToAdd) {
        if(messageToAdd.getPayload() instanceof FileMarker) {
            FileMarker marker = (FileMarker) messageToAdd.getPayload();
            switch (marker.getMark()) {
                case START:
                    replyChannel = messageToAdd.getHeaders().get(MessageHeaders.REPLY_CHANNEL);
                    break;
                case END:
                    expectedRowCount.set(marker.getLineCount());
                    break;
                default:
                    throw new IllegalStateException("Unexpected file mark");
            }
        } else {
            ProcessingResult processingResult = messageToAdd.getHeaders()
                    .get(ProcessingConstants.PROCESSING_RESULT_HEADER, ProcessingResult.class);
            assert processingResult != null;
            rowCount.incrementAndGet();
            switch (processingResult) {
                case FILTERED:
                    filteredCount.incrementAndGet();
                    break;
                case PROCESSED:
                    processedCount.incrementAndGet();
                    break;
                case PROCESSING_ERROR:
                    processingErrorCount.incrementAndGet();
                    break;
                case KAFKA_ERROR:
                    kafkaErrorCount.incrementAndGet();
                    break;
                default:
                    throw new IllegalStateException("Unrecognized processing result: " + processingResult);
            }
        }
    }

    @Override
    public boolean remove(Message<?> messageToRemove) {
        return false;
    }

    @Override
    public Collection<Message<?>> getMessages() {
        return Collections.emptyList();
    }

    @Override
    public Object getGroupId() {
        return groupId;
    }

    @Override
    public int getLastReleasedMessageSequenceNumber() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void setLastReleasedMessageSequenceNumber(int sequenceNumber) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean isComplete() {
        return complete || expectedRowCount.get() == rowCount.get();
    }

    @Override
    public void complete() {
        complete = true;
    }

    @Override
    public int getSequenceSize() {
        return 0;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Message<?> getOne() {
        return getProcessingResult();
    }

    @Override
    public long getTimestamp() {
        return created;
    }

    void setTimestamp(long created) {
        this.created = created;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    @Override
    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public void clear() {
        expectedRowCount.set(0);
        rowCount.set(0);
        processedCount.set(0);
        filteredCount.set(0);
        processingErrorCount.set(0);
        kafkaErrorCount.set(0);
        complete = false;
    }
}
