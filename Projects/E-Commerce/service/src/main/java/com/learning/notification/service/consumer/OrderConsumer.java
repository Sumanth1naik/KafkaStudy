package com.learning.notification.service.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(
        topics = "order-created",
        groupId = "notification-group"
    )
    public void consumeOrder(String message) {

        System.out.println(
            "Received Order Event: " + message
        );

        // Business logic goes here
        System.out.println(
            "Processing notification..."
        );
    }
}
