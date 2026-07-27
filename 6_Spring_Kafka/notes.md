# Spring Boot Kafka ⭐⭐⭐⭐⭐

> A complete guide from Beginner → Advanced → Interview

---

## 1. Spring Kafka

### What is Spring Kafka?

Spring Kafka is a Spring Boot project that simplifies working with Apache Kafka.

Instead of using the low-level Kafka Java APIs, Spring Kafka provides annotations and helper classes.

**Without Spring Kafka:**

```java
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(record);
```

**With Spring Kafka:**

```java
kafkaTemplate.send("orders", order);
```

Much simpler.

### Why use Spring Kafka?

- Less boilerplate code
- Easy producer configuration
- Easy consumer configuration
- Annotation-based listeners
- Retry support
- Error handling
- Batch processing
- Transactions
- Integration with Spring Boot

### Real-life Example

**E-commerce**

```
Customer places order
         ↓
   Order Service
         ↓
   Kafka Topic
         ↓
   Inventory Service
         ↓
 Notification Service
         ↓
  Payment Service
```

Every service receives the event independently.

---

### Interview Notes

**Advantages over Java Kafka API**

- Auto Configuration
- Dependency Injection
- Annotation support
- Better Error Handling
- Retry Support
- JSON serialization
- Easy testing

### Interview Questions

**Q1.** Why use Spring Kafka instead of Kafka Java API?

> **Answer:** Because Spring Kafka reduces boilerplate code and integrates seamlessly with Spring Boot.

**Q2.** Can we use Kafka without Spring Kafka?

> **Answer:** Yes. Kafka itself is independent. Spring Kafka is only a wrapper library.

---

## 2. Spring Kafka Architecture

### Architecture

```
Application
      │
 KafkaTemplate
      │
 ProducerFactory
      │
  Kafka Broker
      │
 ConsumerFactory
      │
 Listener Container
      │
 @KafkaListener
```

### Flow

**Producer**

```
Controller
     ↓
  Service
     ↓
 KafkaTemplate
     ↓
 Kafka Broker
```

**Consumer**

```
 Kafka Broker
     ↓
Listener Container
     ↓
@KafkaListener Method
     ↓
 Business Logic
```

### Components

- KafkaTemplate
- ProducerFactory
- ConsumerFactory
- ListenerContainer
- KafkaListener
- KafkaAdmin

### Interview Questions

**Q1.** What creates `KafkaProducer` in Spring Boot?

> **Answer:** `ProducerFactory`.

**Q2.** Who continuously polls Kafka?

> **Answer:** Listener Container.

---

## 3. KafkaTemplate

Most important producer class. Used to publish messages.

```
    Service
      ↓
 KafkaTemplate
      ↓
 Kafka Broker
```

### Example

```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void send() {
    kafkaTemplate.send("orders", "Order Created");
}
```

### Send with Key

```java
kafkaTemplate.send("orders", "101", order);
```

Key determines partition selection (unless a custom partitioner is used).

### Send to Specific Partition

```java
kafkaTemplate.send("orders", 2, "101", order);
```

### Async Send

```java
kafkaTemplate.send("orders", order)
             .whenComplete((result, ex) -> {
                 if (ex == null)
                     System.out.println("Success");
             });
```

### Useful Methods

- `send()`
- `sendDefault()`
- `execute()`
- `flush()`

### Interview Questions

**Q1.** Is `KafkaTemplate` synchronous?

> **Answer:** No. It sends messages asynchronously and returns a future-like result.

**Q2.** How do you know whether a message was successfully published?

> **Answer:** Use the callback (`whenComplete`) on the returned future.

---

## 4. KafkaAdmin

Used to manage Kafka resources automatically. Creates topics during application startup.

### Example

```java
@Bean
public NewTopic orderTopic() {
    return TopicBuilder.name("orders")
            .partitions(3)
            .replicas(2)
            .build();
}
```

Spring creates the topic if it doesn't already exist and the broker allows it.

### Benefits

- Automatic topic creation
- Topic configuration
- Infrastructure as code

### Interview Questions

**Q1.** Does `KafkaAdmin` create topics every startup?

> **Answer:** It attempts to create them; existing topics are left unchanged.

**Q2.** Can `KafkaAdmin` delete topics?

> **Answer:** Topic deletion is typically handled through Kafka admin operations and broker settings, not automatically during Spring Boot startup.

---

## 5. Producer Configuration

### Example

```properties
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=StringSerializer
spring.kafka.producer.value-serializer=JsonSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=5
spring.kafka.producer.batch-size=16384
spring.kafka.producer.linger-ms=10
```

### Important Properties

| Property | Description |
|---|---|
| `bootstrap.servers` | Kafka server address |
| `key.serializer` | Converts key into bytes |
| `value.serializer` | Converts value into bytes |
| `acks` | Reliability (`0`, `1`, `all`) |
| `retries` | Retry failed sends |
| `batch.size` | Batch size before sending |
| `linger.ms` | Wait time to build a larger batch |

### Interview Questions

**Q1.** Which producer property provides the highest durability?

> **Answer:** `acks=all`.

**Q2.** Why increase `linger.ms`?

> **Answer:** To improve throughput by allowing more records to be batched together (with slightly higher latency).

---

## 6. Consumer Configuration

### Example

```properties
spring.kafka.consumer.group-id=order-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.key-deserializer=StringDeserializer
spring.kafka.consumer.value-deserializer=JsonDeserializer
```

### Important Properties

| Property | Description |
|---|---|
| `group-id` | Consumer group name |
| `auto-offset-reset` | `earliest` \| `latest` \| `none` |
| `enable-auto-commit` | Automatic offset commit |
| `max-poll-records` | Maximum records per poll |
| `fetch.min.bytes` | Minimum bytes before broker responds |

### Interview Questions

**Q1.** What happens if `group.id` changes?

> **Answer:** Kafka treats it as a new consumer group with separate offsets.

**Q2.** What happens if `auto-offset-reset` is `latest`?

> **Answer:** A new consumer group starts reading only newly arriving records.

---

## 7. Kafka Listener

A listener continuously consumes Kafka messages. Spring creates a listener container that polls Kafka and invokes your listener method.

### Example

```java
@Component
public class OrderConsumer {

    @KafkaListener(topics = "orders")
    public void consume(String message) {
        System.out.println(message);
    }
}
```

### Flow

```
  Kafka
    ↓
Listener Container
    ↓
 Listener Method
```

### Interview Questions

**Q1.** Does `@KafkaListener` poll Kafka?

> **Answer:** No. The listener container performs polling.

**Q2.** Can one application have multiple listeners?

> **Answer:** Yes.

---

## 8. @KafkaListener

Most commonly used annotation.

### Example

```java
@KafkaListener(topics = "orders")
public void receive(Order order) {
}
```

### Multiple Topics

```java
@KafkaListener(
    topics = {"orders", "payments"}
)
```

### Group

```java
@KafkaListener(
    topics = "orders",
    groupId = "group1"
)
```

### Specific Partition

```java
@KafkaListener(
    topics = "orders",
    topicPartitions = @TopicPartition(
        topic = "orders",
        partitions = {"0"}
    )
)
```

### Interview Questions

**Q1.** Can multiple `@KafkaListener` methods consume the same topic?

> **Answer:** Yes. Whether they all receive every message depends on their consumer groups.

**Q2.** Can `@KafkaListener` listen to multiple topics?

> **Answer:** Yes.

---

## 9. Listener Container

This is the actual Kafka consumer running behind the scenes.

### Responsibilities

- Poll Kafka
- Manage offsets
- Invoke listeners
- Handle retries
- Handle acknowledgements

### Architecture

```
     Kafka
       ↓
Listener Container
       ↓
 @KafkaListener
       ↓
 Business Logic
```

### Interview Questions

**Q1.** Who manages polling?

> **Answer:** Listener Container.

**Q2.** Can Listener Container manage multiple consumers?

> **Answer:** Yes, especially when concurrency is configured.

---

## 10. Concurrent Listeners

Allows multiple consumer threads.

### Example

```java
@KafkaListener(
    topics = "orders",
    concurrency = "3"
)
```

Spring creates 3 consumer threads (up to the number of partitions).

**Suppose:** 6 partitions, Concurrency = 3

**Distribution:**

| Consumer | Partitions |
|---|---|
| Consumer 1 | P0, P1 |
| Consumer 2 | P2, P3 |
| Consumer 3 | P4, P5 |

### Benefits

- Higher throughput
- Parallel processing

### Interview Questions

**Q1.** If concurrency is 10 but the topic has 3 partitions?

> **Answer:** Only 3 consumers can actively consume; the others remain idle.

**Q2.** Does concurrency preserve ordering across partitions?

> **Answer:** No. Ordering is guaranteed only within an individual partition.

---

## 11. Batch Listener

Consumes multiple records together.

### Enable

```properties
spring.kafka.listener.type=batch
```

### Example

```java
@KafkaListener(topics = "orders")
public void consume(List<Order> orders) {
}
```

### Benefits

- Better throughput
- Fewer poll cycles
- Efficient database writes

### Trade-off

Higher throughput, but processing an individual failed record may require additional handling.

### Interview Questions

**Q1.** Why use Batch Listener?

> **Answer:** To improve throughput by processing multiple records together.

**Q2.** Is ordering preserved?

> **Answer:** Within each partition, the order of records in the batch is preserved.

---

## 12. Manual Acknowledgement

Instead of automatic commits, you control when offsets are committed.

### Example

```java
@KafkaListener(topics = "orders")
public void consume(Order order, Acknowledgment ack) {

    process(order);

    ack.acknowledge();
}
```

### Benefits

- Prevent committing offsets before processing completes.
- Better control over retries and failures.

### Flow

```
 Receive
    ↓
 Process
    ↓
 Success
    ↓
Commit Offset
```

### Interview Questions

**Q1.** Why use manual acknowledgement?

> **Answer:** To commit offsets only after successful processing.

**Q2.** What happens if `acknowledge()` isn't called?

> **Answer:** The offset is not committed (when using manual ack mode), so the record may be reprocessed depending on the consumer configuration.

---

## 13. Message Headers

Kafka messages contain:

- **Key**
- **Value**
- **Headers**
- **Partition**
- **Offset**
- **Timestamp**

Headers carry metadata.

**Example:** `user-id`, `trace-id`, `correlation-id`, `tenant-id`

### Reading Header

```java
@KafkaListener(topics = "orders")
public void consume(
        String msg,
        @Header(KafkaHeaders.RECEIVED_KEY) String key) {
}
```

### Use Cases

- Distributed tracing
- Correlation IDs
- Multi-tenancy
- Versioning

### Interview Questions

**Q1.** Do headers affect partitioning?

> **Answer:** No. Partitioning is based on the key (unless a custom partitioner is used).

**Q2.** Why use `correlation-id`?

> **Answer:** To trace a request across multiple microservices.

---

## 14. Message Conversion

Converts Kafka bytes into Java objects.

**Without conversion:**

```
byte[] → String
```

**With conversion:**

```
JSON → Order Object
```

### Example

**Producer:**

```java
kafkaTemplate.send("orders", order);
```

**Consumer:**

```java
@KafkaListener(topics = "orders")
public void consume(Order order) {
}
```

Using `JsonSerializer` and `JsonDeserializer`, Spring automatically converts between JSON and the `Order` object.

### Benefits

- No manual JSON parsing
- Cleaner code
- Automatic object mapping

### Interview Questions

**Q1.** Which classes are commonly used for JSON conversion?

> **Answer:** `JsonSerializer` (producer) and `JsonDeserializer` (consumer).

**Q2.** What happens if the consumer cannot deserialize a message?

> **Answer:** A deserialization exception occurs. Without proper error handling, the consumer may repeatedly fail on that record. Spring Kafka provides error handlers and dead-letter topic support to manage such failures.

---

## ⭐ Quick Revision (Interview Cheat Sheet)

| Topic | One-Line Summary |
|---|---|
| Spring Kafka | Spring framework for Kafka integration |
| Architecture | `KafkaTemplate` → Broker → Listener Container → `@KafkaListener` |
| `KafkaTemplate` | Main producer API |
| `KafkaAdmin` | Creates/manages Kafka topics |
| Producer Config | Serializer, acks, retries, batch, linger |
| Consumer Config | Group ID, deserializer, offset handling |
| Kafka Listener | Consumes messages from Kafka |
| `@KafkaListener` | Annotation-based consumer |
| Listener Container | Polls Kafka and invokes listener methods |
| Concurrent Listeners | Parallel consumers for higher throughput |
| Batch Listener | Processes multiple records in one poll |
| Manual Acknowledgement | Commit offsets after successful processing |
| Message Headers | Metadata such as trace IDs and correlation IDs |
| Message Conversion | Automatic JSON ↔ Java object conversion |

---

## KafkaTemplate Useful Methods (Spring Kafka)

`KafkaTemplate` is the main class used to send messages to Kafka. It provides several methods for different use cases.

---

### 1. `send()`

This is the most commonly used method. It sends a message to a Kafka topic asynchronously.

**Syntax:**

```java
kafkaTemplate.send(topic, message);
```

**Example:**

```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void sendMessage() {
    kafkaTemplate.send("orders", "Order Created");
}
```

| | |
|---|---|
| **Topic** | `orders` |
| **Key** | `null` |
| **Value** | `"Order Created"` |

Kafka chooses the partition (typically using a sticky partitioner when no key is provided).

**Send with Key:**

```java
kafkaTemplate.send("orders", "101", "Order Created");
```

| | |
|---|---|
| **Topic** | `orders` |
| **Key** | `101` |
| **Value** | `"Order Created"` |

The key is used to determine the partition (unless a custom partitioner is configured).

**Send to Specific Partition:**

```java
kafkaTemplate.send("orders", 2, "101", "Order Created");
```

This forces the message to Partition 2.

**Send with Timestamp:**

```java
kafkaTemplate.send(
    "orders",
    2,
    System.currentTimeMillis(),
    "101",
    "Order Created"
);
```

Useful when you want to provide a custom timestamp.

**Return Type:**

```
CompletableFuture<SendResult<K, V>>
```

Since sending is asynchronous, you can check the result later.

```java
kafkaTemplate.send("orders", "Order Created")
    .whenComplete((result, ex) -> {
        if (ex == null) {
            System.out.println("Message Sent");
            System.out.println(result.getRecordMetadata().partition());
        } else {
            System.out.println("Failed");
        }
    });
```

**Flow:**

```
  Application
       ↓
KafkaTemplate.send()
       ↓
 Kafka Producer
       ↓
 Kafka Broker
       ↓
 Acknowledgement
```

**Interview Question:**

**Q:** Is `send()` synchronous or asynchronous?

> **Answer:** It is asynchronous. It immediately returns a `CompletableFuture`.

---

### 2. `sendDefault()`

This method sends a message to the default topic. Instead of specifying the topic every time, configure one default topic.

**Configuration:**

```java
@Bean
public KafkaTemplate<String, String> kafkaTemplate(
        ProducerFactory<String, String> producerFactory) {

    KafkaTemplate<String, String> template =
            new KafkaTemplate<>(producerFactory);

    template.setDefaultTopic("orders");

    return template;
}
```

**Usage:**

```java
kafkaTemplate.sendDefault("Order Created");
```

Equivalent to:

```java
kafkaTemplate.send("orders", "Order Created");
```

**With Key:**

```java
kafkaTemplate.sendDefault("101", "Order Created");
```

**When to Use?**

Suppose your application always publishes to one topic. Instead of:

```java
kafkaTemplate.send("orders", order);
kafkaTemplate.send("orders", order2);
kafkaTemplate.send("orders", order3);
```

You can simply write:

```java
kafkaTemplate.sendDefault(order);
kafkaTemplate.sendDefault(order2);
kafkaTemplate.sendDefault(order3);
```

Cleaner and avoids repeating the topic name.

**Interview Question:**

**Q:** What happens if you call `sendDefault()` without configuring a default topic?

> **Answer:** An exception is thrown because `KafkaTemplate` does not know which topic to use.

---

### 3. `execute()`

`execute()` gives direct access to the underlying `KafkaProducer`. It is used when `KafkaTemplate` does not expose a producer feature that you need.

**Syntax:**

```java
kafkaTemplate.execute(producer -> {

    // Direct access to KafkaProducer

    return something;
});
```

**Example:**

```java
String clientId = kafkaTemplate.execute(producer -> {
    return producer.metrics().keySet().toString();
});

System.out.println(clientId);
```

Here, Spring gives you the actual `KafkaProducer`.

**You can access:**

- Metrics
- Partitions
- Producer APIs

**Flow:**

```
  Application
       ↓
KafkaTemplate.execute()
       ↓
 KafkaProducer
       ↓
  Kafka APIs
```

**When is it Used?**

Normally you do not use `execute()` for sending messages. Instead, you use it for advanced producer operations such as:

- Reading producer metrics
- Accessing low-level `KafkaProducer` functionality
- Performing operations not directly exposed by `KafkaTemplate`

**Interview Question:**

**Q:** Why use `execute()` instead of `send()`?

> **Answer:** `send()` is for publishing messages. `execute()` is for advanced operations requiring direct access to the underlying `KafkaProducer`.

---

### 4. `flush()`

Kafka Producer batches messages before sending them. `flush()` forces all buffered records to be sent immediately.

**Without `flush()`:**

```
Message 1 → Buffer
Message 2 → Buffer
Message 3 → Buffer → Batch Sent Later
```

The producer waits based on:

- `batch.size`
- `linger.ms`

**With `flush()`:**

```java
kafkaTemplate.send("orders", "Order 1");
kafkaTemplate.send("orders", "Order 2");
kafkaTemplate.flush();
```

```
Message 1 → Buffer
Message 2 → Buffer
flush() → Immediately Sent
```

**Why is it Needed?**

Suppose you are shutting down the application.

**Without `flush()`:**

```
Application Stops → Buffered Messages Lost
```

**With `flush()`:**

```
Application Stops → flush() → Messages Sent → Shutdown
```

> **Note:** In a normal Spring Boot application, the producer is typically closed gracefully during shutdown, which flushes pending records. Calling `flush()` manually is most useful when you need to ensure all previously sent records have reached the broker before continuing with the next operation.

**Interview Question:**

**Q:** Does `flush()` guarantee that consumers have processed the message?

> **Answer:** No. It only ensures the producer has sent all buffered records and waited for the broker acknowledgements according to the configured `acks` setting. It does **not** guarantee that any consumer has read or processed the messages.

---

### Comparison Table

| Method | Purpose | Most Common Use |
|---|---|---|
| `send()` | Send a message to a specified topic | Normal message publishing |
| `sendDefault()` | Send a message to the configured default topic | Applications using a single topic frequently |
| `execute()` | Access the underlying `KafkaProducer` | Advanced producer operations (metrics, low-level APIs) |
| `flush()` | Force buffered records to be sent immediately | Before shutdown or when immediate delivery is required |

---

### Interview Cheat Sheet

| Method | One-Line Interview Answer |
|---|---|
| `send()` | Asynchronously sends a message to a Kafka topic and returns a `CompletableFuture`. |
| `sendDefault()` | Sends a message to the configured default topic without specifying the topic name each time. |
| `execute()` | Provides direct access to the underlying `KafkaProducer` for advanced operations. |
| `flush()` | Forces all buffered records to be sent immediately; it does **not** guarantee consumer processing. |