package intg.com.libraryeventsproducer.controller;

import com.libraryeventsproducer.domain.Book;
import com.libraryeventsproducer.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
//Override bootstrap server properties with embedded kafka
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers"})
public class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

    }


    @AfterEach
    void tearDown() {
        consumer.close();
    }


        @Test
        @Timeout(5)
        void postLibraryEvent() throws InterruptedException {

            Book book = Book.builder()
                    .bookId(456)
                    .bookAuthor("Thompson")
                    .bookName("Kafka using springboot").build();

            LibraryEvent libraryEvent = LibraryEvent.builder()
                            .libraryEventId(null)
                            .book(book)
                            .build();

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
            HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, httpHeaders);

            ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST,request,LibraryEvent.class);
            Assertions.assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

            ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
            //Thread.sleep(3000);
            String expectedRecord = "{\n" +
                    "    \"libraryEventId\": null,\n" +
                    "    \"libraryEventType\":\"NEW\",\n" +
                    "    \"book\": {\n" +
                    "        \"bookId\": 456,\n" +
                    "        \"bookName\": \"Kafka using springboot\",\n" +
                    "        \"bookAuthor\": \"Thompson\"\n" +
                    "    }\n" +
                    "}";
            String value = consumerRecord.value();
            Assertions.assertEquals(expectedRecord, value);
        }

}
