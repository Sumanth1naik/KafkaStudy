package com.learning.order.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducerService {

    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducerService(
            KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrder(String message) {

        kafkaTemplate.send(TOPIC, message)
                .whenComplete((result, exception) -> {

                    if (exception == null) {
                        System.out.println(
                            "Message published successfully: "
                            + result.getRecordMetadata().offset()
                        );
                    } else {
                        System.err.println(
                            "Failed to publish message: "
                            + exception.getMessage()
                        );
                    }
                });
    }
}
