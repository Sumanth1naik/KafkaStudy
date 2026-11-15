### learning roadmap
1. Kafka + Spring Boot setup
   * Dependencies, Kafka broker and application configuration.
2. Producer implementation
   * Publish messages using KafkaTemplate.
3. Consumer implementation
   * Consume messages using @KafkaListener.
4. JSON serialization
   * Send Java objects instead of plain strings.
5. Error handling and retries
   * Understand failures, retries and dead-letter topics.
6. Interview knowledge check
   * Debug real-world producer and consumer scenarios.


## 1. Understand what we're building
Let's consider an e-commerce application.  
* When a customer places an order:  
    1. Order Service creates an order.  
    2. Order Service publishes an OrderCreated event to Kafka.  
    3. Kafka stores the event in an order topic.  
    4. Notification Service consumes the event.  
    5. Notification Service sends an order confirmation.  