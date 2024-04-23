package com.tailoredshapes.gridql;

import com.tailoredshapes.stash.Stash;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

import static com.tailoredshapes.underbar.ocho.Die.die;

public class KafkaEventConsumer implements Runnable{
    private static final Logger logger = Logger.getLogger("KafkaEventConsumer");
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ApiService apiService;
    private final String topic;

    public KafkaEventConsumer(KafkaConsumer kafkaConsumer, ApiService apiService, String topic) {
        this.kafkaConsumer = kafkaConsumer;
        this.apiService = apiService;
        this.topic = topic;
    }

    public void run() {
        kafkaConsumer.subscribe(Collections.singleton(topic));
        kafkaConsumer.poll(Duration.ofMillis(100)).forEach(record -> {
            Stash message = Stash.parseJSON(record.value());
            String operation = message.asString("operation");

            switch (operation) {
                case "CREATE":
                    apiService.create(message.asStash("payload"));
                    break;
                case "UPDATE":
                    apiService.update(message.asString("id"), message.asStash("payload"));
                    break;
                case "DELETE":
                    apiService.delete(message.asString("id"));
                    break;
                default:
                    die("Unexpected operatin");

            }
        });
    }
}