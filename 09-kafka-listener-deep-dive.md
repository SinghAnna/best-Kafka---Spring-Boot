# Part 9: `@KafkaListener` Deep Dive — Listener Container, Manual Ack, Concurrency, Error Handling and Batch Processing

> `@KafkaListener` Spring Kafka ka core consumer annotation hai. Ye ek normal Spring bean method ko Kafka message listener bana deta hai, aur listener container consumer lifecycle, polling, offset commits, concurrency aur error handling manage karta hai.

---

## 1. `@KafkaListener` Kya Hai?

`@KafkaListener` ek Spring annotation hai jo method ko Kafka consumer bana deta hai.

```java
@KafkaListener(topics = "order-events")
public void consume(OrderCreatedEvent event) {
    // process event
}
```

Ye method automatically:

```text
1. Kafka topic subscribe karta hai
2. Messages poll karta hai
3. Message deserialize karta hai
4. Method invoke karta hai
5. Offset commit karta hai
6. Errors handle karta hai
```

`@KafkaListener` annotated method ke liye Spring Kafka listener container create karta hai. [587]

---

## 2. Listener Container Kya Hai?

Listener container actual Kafka consumer lifecycle manage karta hai.

```text
@KafkaListener Method
        |
        v
Listener Container
        |
        v
Kafka Consumer
        |
        v
Kafka Broker
```

### Container Responsibilities

```text
1. Kafka consumer create karna
2. Consumer group join karna
3. Partitions assign karna
4. Records poll karna
5. Deserialization karna
6. Listener method invoke karna
7. Offset commit karna
8. Rebalancing handle karna
9. Error handling karna
```

Spring Kafka `ConcurrentMessageListenerContainer` ek ya zyada consumer instances create karke concurrency support karta hai. [580]

---

## 3. Basic `@KafkaListener`

### Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: notification-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafka.event"
```

### Consumer

```java
@Component
@Slf4j
public class OrderCreatedConsumer {

    @KafkaListener(
        topics = "order.order.created.v1",
        groupId = "notification-service-group"
    )
    public void consume(OrderCreatedEvent event) {
        log.info("Received order event: {}", event);
    }
}
```

---

## 4. Important `@KafkaListener` Attributes

| Attribute | Purpose |
|---|---|
| `id` | Listener container ka unique ID |
| `topics` | Subscribe karne wale topics |
| `topicPattern` | Regex-based topic subscription |
| `groupId` | Consumer group ID |
| `containerFactory` | Custom listener container factory |
| `concurrency` | Parallel consumer threads |
| `autoStartup` | Application start par listener start karna |
| `properties` | Listener-specific Kafka properties |
| `errorHandler` | Listener-specific error handler |
| `ackMode` | Per-listener acknowledgement mode |

`@KafkaListener` me `id`, `topics`, `groupId`, `concurrency`, `autoStartup`, `properties`, `errorHandler` aur `ackMode` jaise attributes available hain. [583][587][590]

---

## 5. Complete Listener Example

```java
@Component
@Slf4j
public class OrderCreatedConsumer {

    @KafkaListener(
        id = "order-created-listener",
        topics = "${kafka.topic.order-created}",
        groupId = "notification-service-group",
        concurrency = "3",
        autoStartup = "true",
        properties = {
            "max.poll.records=100",
            "max.poll.interval.ms=300000"
        }
    )
    public void consume(
        OrderCreatedEvent event,
        Acknowledgment acknowledgment
    ) {
        try {
            process(event);
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process event: {}", event, ex);
            throw ex;
        }
    }

    private void process(OrderCreatedEvent event) {
        log.info(
            "Processing order={} for user={}",
            event.orderId(),
            event.userId()
        );
    }
}
```

---

## 6. Manual Acknowledgment

Manual ack me consumer processing complete hone ke baad offset commit karta hai.

### Configuration

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual_immediate
```

### Consumer

```java
@KafkaListener(
    topics = "order.order.created.v1",
    groupId = "notification-service-group"
)
public void consume(
    OrderCreatedEvent event,
    Acknowledgment acknowledgment
) {
    try {
        process(event);

        acknowledgment.acknowledge();
    } catch (Exception ex) {
        log.error("Processing failed", ex);
        throw ex;
    }
}
```

### Important Rule

```text
acknowledge() call nahi hua
= offset commit nahi hua
= restart par message dobara process hoga
```

---

## 7. `MANUAL` vs `MANUAL_IMMEDIATE`

| Ack Mode | Behavior |
|---|---|
| `MANUAL` | Ack queue hota hai; previous poll ke saare records process hone par commit hota hai |
| `MANUAL_IMMEDIATE` | `acknowledge()` call hote hi offset commit hota hai |

`MANUAL` me acks queue hote hain aur last poll ke saare records process hone par commit hote hain; `MANUAL_IMMEDIATE` me listener thread par ack karte hi commit hota hai. [585][590]

### Recommendation

```text
Critical business events:
MANUAL_IMMEDIATE

High-throughput batch processing:
MANUAL ya BATCH
```

---

## 8. Concurrency

Concurrency ka matlab hai ek listener container kitne Kafka consumer threads create karega.

```java
@KafkaListener(
    topics = "order-events",
    groupId = "notification-service-group",
    concurrency = "3"
)
public void consume(OrderCreatedEvent event) {
}
```

### How It Works

```text
Topic partitions = 6
Listener concurrency = 3

Consumer Thread 1 -> Partition 0, Partition 1
Consumer Thread 2 -> Partition 2, Partition 3
Consumer Thread 3 -> Partition 4, Partition 5
```

### Important Rule

```text
Concurrency > Partitions
= Extra threads idle
```

Example:

```text
Partitions = 3
Concurrency = 6

3 threads active
3 threads idle
```

Concurrency partition count se related hoti hai; partitions se zyada concurrency set karne ka practical fayda nahi hota. [588]

---

## 9. Concurrency Best Practices

```text
1. Concurrency ko partition count ke hisaab se set karo
2. Consumer thread pool downstream capacity se match karo
3. Database connection pool capacity check karo
4. External API rate limits check karo
5. CPU-bound processing me cores consider karo
6. I/O-bound processing me higher concurrency possible hai
```

### Example

```text
Topic partitions = 12
Database pool = 20
External API limit = 100 RPS

Safe concurrency = 6 to 12
```

---

## 10. Error Handling

### Basic Error Handling

```java
@KafkaListener(topics = "order-events")
public void consume(OrderCreatedEvent event) {
    try {
        process(event);
    } catch (Exception ex) {
        log.error("Processing failed", ex);
        throw ex;
    }
}
```

Exception throw karne par listener container error handler invoke hota hai.

### Listener-Specific Error Handler

```java
@KafkaListener(
    topics = "order-events",
    errorHandler = "orderListenerErrorHandler"
)
public void consume(OrderCreatedEvent event) {
    process(event);
}
```

```java
@Bean
public KafkaListenerErrorHandler orderListenerErrorHandler() {
    return (message, exception) -> {
        log.error(
            "Listener error for payload={}",
            message.getPayload(),
            exception
        );

        // Return custom recovery result or throw
        throw exception;
    };
}
```

`@KafkaListener` ka `errorHandler` attribute listener-specific `KafkaListenerErrorHandler` bean accept karta hai. [581][582]

---

## 11. Retry and Dead Letter Topic

Transient errors ke liye retry, aur permanent errors ke liye DLQ use hota hai.

### Non-Blocking Retry

```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 1000, multiplier = 2),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
)
@KafkaListener(topics = "order-events")
public void consume(OrderCreatedEvent event) {
    process(event);
}
```

Flow:

```text
order-events
   |
   +--> order-events-retry-0
   +--> order-events-retry-1
   +--> order-events-retry-2
   |
   +--> order-events-dlt
```

### Retry Rules

```text
1. Transient error -> Retry
2. Permanent error -> DLQ
3. Retry me exponential backoff
4. DLQ me original payload + error metadata
5. DLQ monitoring compulsory
```

---

## 12. Batch Listener

Batch listener ek poll ke multiple records ek saath process karta hai.

### Configuration

```yaml
spring:
  kafka:
    listener:
      type: batch
      ack-mode: batch
    consumer:
      max-poll-records: 100
```

### Batch Consumer

```java
@KafkaListener(
    topics = "order-events",
    groupId = "analytics-service-group",
    batch = "true"
)
public void consumeBatch(
    List<OrderCreatedEvent> events,
    Acknowledgment acknowledgment
) {
    try {
        analyticsService.processBatch(events);
        acknowledgment.acknowledge();
    } catch (Exception ex) {
        log.error("Batch processing failed", ex);
        throw ex;
    }
}
```

Batch listener ek hi method me multiple records receive karta hai, jo high-throughput processing ke liye useful hai. [584][589]

---

## 13. Batch Processing Best Practices

```text
1. Batch size controlled rakho
2. Database bulk operations use karo
3. Partial failure handle karo
4. Idempotent processing rakho
5. Batch processing time `max.poll.interval.ms` se kam rakho
6. DLQ strategy define karo
```

### Example

```text
100 records per batch
= 1 database bulk insert
instead of
100 separate inserts
```

---

## 14. Message Headers

Kafka headers me metadata rakha jata hai.

```text
traceId
correlationId
source
schemaVersion
eventType
retryCount
```

### Access Headers

```java
@KafkaListener(topics = "order-events")
public void consume(
    OrderCreatedEvent event,
    @Header(KafkaHeaders.RECEIVED_KEY) String key,
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
    @Header(KafkaHeaders.OFFSET) long offset,
    @Header(name = "traceId", required = false) String traceId
) {
    log.info(
        "Received key={}, topic={}, partition={}, offset={}, traceId={}",
        key,
        topic,
        partition,
        offset,
        traceId
    );
}
```

---

## 15. Dynamic Topic Subscription

```java
@KafkaListener(topicPattern = "order\\..*", groupId = "order-monitor-group")
public void consume(OrderCreatedEvent event) {
    log.info("Received order event: {}", event);
}
```

Ye `order.created`, `order.updated`, `order.cancelled` jaise topics ko match karega.

> Production me dynamic topic patterns carefully use karo, kyunki unexpected topics bhi match ho sakte hain.

---

## 16. Graceful Shutdown

Kubernetes me pod terminate hone par consumer ko gracefully shutdown karna chahiye.

```yaml
server:
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

### Flow

```text
1. Kubernetes SIGTERM bhejta hai
2. Spring graceful shutdown start hota hai
3. Listener new messages fetch karna band karta hai
4. Current messages process hote hain
5. Offsets commit hote hain
6. Consumer group leave karta hai
7. Pod terminate hota hai
```

---

## 17. Consumer Configuration Example

```yaml
spring:
  kafka:
    consumer:
      group-id: notification-service-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafka.event"
        spring.json.use.type.headers: false
        spring.json.value.default.type: com.example.kafka.event.OrderCreatedEvent
        session.timeout.ms: 45000
        heartbeat.interval.ms: 3000
        max.poll.interval.ms: 300000
        max.poll.records: 100
    listener:
      ack-mode: manual_immediate
      concurrency: 3
```

---

## 18. Common Mistakes

### Mistake 1 — Concurrency Partitions Se Zyada Rakhna

```text
Wrong:
3 partitions, concurrency = 10

Correct:
3 partitions, concurrency = 3
```

### Mistake 2 — Auto Commit for Critical Events

```text
Wrong:
Auto commit before processing

Correct:
Manual commit after successful processing
```

### Mistake 3 — Long Processing in Listener

```text
Wrong:
10-minute work inside listener

Correct:
Async processing, batching ya timeout tuning
```

### Mistake 4 — No DLQ

```text
Wrong:
Poison message processing block karti hai

Correct:
Retry + DLQ implement karo
```

### Mistake 5 — Batch Listener Me Partial Failure Ignore Karna

```text
Wrong:
Ek record fail -> poora batch fail

Correct:
Per-record error handling / partial batch strategy
```

---

## 19. Interview Questions

| Question | Answer |
|---|---|
| `@KafkaListener` kya hai? | Method ko Kafka message listener banane wala Spring annotation. |
| Listener container kya hai? | Kafka consumer lifecycle, polling aur offset commit manage karne wala component. |
| Manual acknowledgment kya hai? | Processing complete hone ke baad explicitly offset commit karna. |
| `MANUAL_IMMEDIATE` kya hai? | `acknowledge()` call hote hi offset commit. |
| Concurrency kya hai? | Listener container dwara banaye gaye parallel consumer threads. |
| Batch listener kya hai? | Ek poll ke multiple records ek saath process karna. |

### Scenario-Based

| Scenario | Solution |
|---|---|
| Consumer slow hai | Concurrency, partition count, downstream latency aur batch size check karo |
| Poison message processing block kar rahi hai | Retry + DLQ lagao |
| Critical event duplicate process ho raha hai | Manual ack + idempotent consumer |
| High-throughput processing chahiye | Batch listener + bulk DB operations |
| Kubernetes me graceful shutdown chahiye | Graceful shutdown + proper ack mode |

---

## 20. Key Takeaways

```text
1. @KafkaListener Spring Kafka consumer ka core annotation hai.

2. Listener container consumer lifecycle manage karta hai.

3. Manual acknowledgment critical flows me use hota hai.

4. MANUAL_IMMEDIATE ack par immediate commit karta hai.

5. Concurrency partitions se related hoti hai.

6. Partitions se zyada consumers idle rehte hain.

7. Retry transient failures ke liye hota hai.

8. DLQ permanent failures ke liye hota hai.

9. Batch listener high-throughput processing ke liye useful hai.

10. Graceful shutdown rebalancing aur data loss reduce karta hai.
```

---

## 21. Next Part Preview

**Part 10: JSON Serialization and Deserialization**

Isme hum cover karenge:

```text
- JsonSerializer and JsonDeserializer
- Type headers
- Trusted packages
- Custom ObjectMapper
- Schema evolution
- Versioning
- Serialization failures
- Secure deserialization
- Spring Boot configuration
```