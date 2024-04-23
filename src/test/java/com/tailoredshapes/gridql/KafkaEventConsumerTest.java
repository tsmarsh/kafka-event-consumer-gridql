package com.tailoredshapes.gridql;

import com.tailoredshapes.stash.Stash;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

import static com.tailoredshapes.stash.Stash.stash;
import static org.mockito.Mockito.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KafkaEventConsumerTest {
    private KafkaContainer kafka;
    private KafkaConsumer<String, String> kafkaConsumer;

    private ApiService apiService = mock(ApiService.class);;
    private KafkaProducer<String, String> kafkaProducer;

    @BeforeEach
    void setUp() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Test_Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Test_Group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, String.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        kafkaConsumer = new KafkaConsumer<>(props);

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProducer = new KafkaProducer<>(prodProps);
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
        kafkaConsumer.close();
        kafkaProducer.close();
    }

    @Test
    void shouldCallCreateAPIOnCreateMessage(){
        String topic = "create";
        Stash payload = stash("name", "brian", "eggs", 3);
        Stash message = stash("payload", payload, "operation", "CREATE");

        KafkaEventConsumer kafkaEventConsumer = new KafkaEventConsumer(kafkaConsumer, apiService, topic);
        kafkaProducer.beginTransaction();
        ProducerRecord<String, String> pr = new ProducerRecord<>(topic, "141211", message.toJSONString());
        kafkaProducer.send(pr);
        kafkaProducer.commitTransaction();

        verify(apiService).create(eq(payload));

    }
}