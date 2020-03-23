package com.mz.example;

import com.mz.example.processing.FileProcessingResultDescriptor;
import com.mz.example.processing.ProcessingConstants;
import com.mz.example.processing.ProcessingGateway;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.test.context.MockIntegrationContext;
import org.springframework.integration.test.context.SpringIntegrationTest;
import org.springframework.integration.test.mock.MockIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
@SpringIntegrationTest(noAutoStartup = ProcessingConstants.KAFKA_ADAPTER)
@TestPropertySource(locations="classpath:test.properties")
public class ProcessingFlowTest {

    @Autowired
    private ProcessingGateway processingGateway;
    @Autowired
    private MockIntegrationContext mockIntegrationContext;
    @Autowired
    @Qualifier(ProcessingConstants.AGGREGATION_CHANNEL)
    private MessageChannel aggregationChannel;
    @Autowired
    @Qualifier(ProcessingConstants.KAFKA_ERROR_CHANNEL)
    private MessageChannel kafkaErrorChannel;

    private List<String> messagesSentToKafka = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setup() {
        messagesSentToKafka.clear();
        mockIntegrationContext.substituteMessageHandlerFor(ProcessingConstants.KAFKA_ADAPTER,
                MockIntegration.mockMessageHandler().handleNext(message -> {
                    log.info("Mock kafka adapter has received message: " + message.toString());
                    messagesSentToKafka.add((String) message.getPayload());
                    aggregationChannel.send(message);
                }));
    }

    private void simulateKafkaUnavailable() {
        mockIntegrationContext.substituteMessageHandlerFor(ProcessingConstants.KAFKA_ADAPTER,
                MockIntegration.mockMessageHandler().handleNext(message -> {
                    kafkaErrorChannel.send(message);
                }));
    }

    private File template(String name) throws URISyntaxException {
        final String TEMPLATE_FILE_PATH = "/template/" + name;
        return new File(getClass().getResource(TEMPLATE_FILE_PATH).toURI());
    }

    @Test
    public void testBaseFlow() throws Exception {
        String fileName = "baseFlow.txt";
        File file = template(fileName);

        FileProcessingResultDescriptor descriptor = processingGateway.processFile(file).get();
        checkFileProcessingResultDescriptor(descriptor, fileName, true,
                7, 7, 7, 0, 0, 0);
        messagesSentToKafka.sort(Comparator.comparing(String::toString));
        Assert.assertEquals("I want to have more rows then processing threads", messagesSentToKafka.get(0));
        Assert.assertEquals("row 1", messagesSentToKafka.get(1));
        Assert.assertEquals("row 2", messagesSentToKafka.get(2));
        Assert.assertEquals("row 3", messagesSentToKafka.get(3));
        Assert.assertEquals("row 4", messagesSentToKafka.get(4));
        Assert.assertEquals("row 5", messagesSentToKafka.get(5));
        Assert.assertEquals("row 6", messagesSentToKafka.get(6));
    }

    @Test
    public void testKafkaUnavailable() throws Exception {
        simulateKafkaUnavailable();

        String fileName = "kafkaUnavailable.txt";
        File file = template(fileName);

        FileProcessingResultDescriptor descriptor = processingGateway.processFile(file).get();
        checkFileProcessingResultDescriptor(descriptor, fileName, true,
                2, 2, 0, 0, 0, 2);
        Assert.assertTrue(messagesSentToKafka.isEmpty());
    }

    @Test
    public void testProcessingErrors() throws Exception {
        String fileName = "processingErrors.txt";
        File file = template(fileName);

        FileProcessingResultDescriptor descriptor = processingGateway.processFile(file).get();
        checkFileProcessingResultDescriptor(descriptor, fileName, true,
                3, 3, 2, 0, 1, 0);
        messagesSentToKafka.sort(Comparator.comparing(String::toString));
        Assert.assertEquals(2, messagesSentToKafka.size());
        Assert.assertEquals("row 1", messagesSentToKafka.get(0));
        Assert.assertEquals("row 3", messagesSentToKafka.get(1));
    }

    @Test
    public void testFiltering() throws Exception {
        String fileName = "filtering.txt";
        File file = template(fileName);

        FileProcessingResultDescriptor descriptor = processingGateway.processFile(file).get();
        checkFileProcessingResultDescriptor(descriptor, fileName, true,
                3, 3, 2, 1, 0, 0);
        messagesSentToKafka.sort(Comparator.comparing(String::toString));
        Assert.assertEquals(2, messagesSentToKafka.size());
        Assert.assertEquals("row 1", messagesSentToKafka.get(0));
        Assert.assertEquals("row 3", messagesSentToKafka.get(1));
    }

    @Test(expected = ExecutionException.class)
    public void testProcessingNonExistingFile() throws Exception {
        String filePath = "NON_EXISTING_FILE";
        File file = new File(filePath);
        Assert.assertFalse(file.exists());

        processingGateway.processFile(file).get();
    }

    private void checkFileProcessingResultDescriptor(FileProcessingResultDescriptor descriptor,
                                                     String fileName, boolean isValid, long expectedRows, long rows,
                                                     long processed, long filtered, long processingErr, long kafkaErr) {
        Assert.assertEquals(isValid, descriptor.isValid());
        Assert.assertEquals(fileName, descriptor.getFileName());
        Assert.assertEquals(expectedRows, descriptor.getExpectedNumberOfRows());
        Assert.assertEquals(rows, descriptor.getNumberOfRows());
        Assert.assertEquals(processed, descriptor.getNumberOfProcessed());
        Assert.assertEquals(filtered, descriptor.getNumberOfFiltered());
        Assert.assertEquals(processingErr, descriptor.getNumberOfProcessingErrors());
        Assert.assertEquals(kafkaErr, descriptor.getNumberOfKafkaErrors());
    }
}
