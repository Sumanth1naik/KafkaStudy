package com.learning.order.controller;

import com.learning.order.model.OrderEvent;
import com.learning.order.service.OrderProducerService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(
            @RequestBody OrderEvent order) {

        producerService.publishOrder(
            order.getOrderId() + " - " + order.getProductName()
        );

        return ResponseEntity.accepted()
                .body("Order event submitted to Kafka");
    }
    @GetMapping("/")
    public String healthCheck() {
        return "Order Service is up and running!";
    }
}
