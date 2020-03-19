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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.test.context.MockIntegrationContext;
import org.springframework.integration.test.context.SpringIntegrationTest;
import org.springframework.integration.test.mock.MockIntegration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    private List<String> messagesSentToKafka = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setup() {
        messagesSentToKafka.clear();
        mockIntegrationContext.substituteMessageHandlerFor(ProcessingConstants.KAFKA_ADAPTER,
                MockIntegration.mockMessageHandler().handleNext(message -> {
                    log.info("Mock kafka adapter has received message: " + message.toString());
                    messagesSentToKafka.add((String) message.getPayload());
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
                2, 2, 2, 0, 0, 0);
        Assert.assertEquals("row 1", messagesSentToKafka.get(0));
        Assert.assertEquals("row 2", messagesSentToKafka.get(1));
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
