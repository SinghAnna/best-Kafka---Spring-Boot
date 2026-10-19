# Part 3: Producers, Consumers and Consumer Groups — Complete Deep Dive

> Kafka ka core data flow producers, topics, partitions, consumers aur consumer groups ke beech hota hai. Producer events publish karta hai, Kafka unhe partitioned durable log me store karta hai, aur consumer groups unhe parallel aur reliably process karte hain.

---

## 1. Kafka Data Flow

```text
Producer
   |
   | send(event)
   v
Kafka Topic
   |
   +--> Partition 0
   +--> Partition 1
   +--> Partition 2
   |
   v
Consumer Group
   |
   +--> Consumer 1
   +--> Consumer 2
   +--> Consumer 3
```

Producer Kafka me event bhejta hai, Kafka event ko topic ke partition me append karta hai, aur consumer group ke consumers un partitions ko process karte hain. Spring Kafka me producer operations `KafkaTemplate` se wrap hote hain. [500][502]

---

## 2. Kafka Producer

Producer wo application hai jo Kafka topic me events publish karti hai.

```text
Order Service -> Kafka -> order-events
Payment Service -> Kafka -> payment-events
User Service -> Kafka -> user-events
```

### Producer Responsibilities

```text
1. Event create karna
2. Event serialize karna
3. Partition decide karna
4. Event batch karna
5. Kafka broker ko send karna
6. Retry handle karna
7. Delivery result track karna
```

### Producer Flow

```text
Application Code
      |
      v
Serializer
      |
      v
Partitioner
      |
      v
Record Accumulator / Batch
      |
      v
Sender Thread
      |
      v
Kafka Broker
```

---

## 3. Kafka Record Structure

Har Kafka record me ye main parts hote hain:

```text
Topic
Partition
Offset
Key
Value
Headers
Timestamp
```

### Example

```json
{
  "topic": "order-events",
  "partition": 1,
  "offset": 42,
  "key": "order_8721",
  "value": {
    "eventId": "evt_9921",
    "eventType": "ORDER_CREATED",
    "orderId": "order_8721",
    "userId": "user_101",
    "totalAmount": 2499.00
  },
  "headers": {
    "source": "order-service",
    "traceId": "trace-8821",
    "schemaVersion": "1.0"
  },
  "timestamp": 1760874600000
}
```

### Key and Value

| Field | Purpose |
|---|---|
| Key | Partition selection aur ordering |
| Value | Actual event payload |
| Headers | Metadata: trace ID, source, version |
| Timestamp | Event time ya producer time |
| Offset | Partition me record position |

---

## 4. Kafka Partitioner

Partitioner decide karta hai ki event kaunse partition me jayega.

### Default Behavior

```text
Key present:
partition = hash(key) % number_of_partitions

Key absent:
Round-robin / sticky partitioning
```

### Example

```text
Topic partitions = 3

key = order_8721
hash(order_8721) = 10

10 % 3 = 1

Partition = 1
```

### Why Keys Matter

Same key wale records same partition me jate hain.

```text
key = order_8721

ORDER_CREATED       -> Partition 1
PAYMENT_COMPLETED   -> Partition 1
ORDER_SHIPPED       -> Partition 1
```

Isliye same entity ke events ka order preserve hota hai.

### Good Kafka Keys

```text
orderId
userId
paymentId
tenantId
deviceId
```

### Bad Kafka Keys

```text
timestamp
random UUID for every event
null key for ordered business events
```

---

## 5. Producer Batching

Producer same partition ke multiple records ko ek batch me bhejta hai.

```text
Records for Partition 1:

Record 1
Record 2
Record 3

Batch:
[Record 1, Record 2, Record 3]
```

### Benefits

```text
1. Kam network requests
2. Better throughput
3. Better compression
4. Lower broker overhead
```

### Important Producer Settings

| Setting | Meaning |
|---|---|
| `batch.size` | Ek batch ka maximum size |
| `linger.ms` | Batch bhejne se pehle kitna wait karna hai |
| `buffer.memory` | Producer buffer memory |
| `compression.type` | Batch compression |
| `max.in.flight.requests.per.connection` | Parallel in-flight requests |

### Batching Tradeoff

```text
Higher linger.ms
= better batching
= better throughput
= slightly higher latency

Lower linger.ms
= lower latency
= more requests
= potentially lower throughput
```

Kafka producer same partition ke records ko batch karta hai, aur compression full batch par hoti hai — isliye better batching se compression ratio bhi improve ho sakta hai. [499][501]

---

## 6. Producer Compression

Compression network aur storage cost reduce karti hai.

| Compression | Use Case |
|---|---|
| `none` | Very low CPU, high network usage |
| `gzip` | High compression, higher CPU |
| `snappy` | Balanced speed and compression |
| `lz4` | Fast compression |
| `zstd` | Modern, strong compression and speed |

### Recommendation

```text
General production default: lz4 ya zstd
High CPU constraint: snappy ya lz4
Maximum compression: zstd
```

---

## 7. Producer Acknowledgements

`acks` decide karta hai ki producer ko kitne acknowledgements chahiye.

| Setting | Meaning | Durability | Latency |
|---|---|---|---|
| `acks=0` | Broker acknowledgement wait nahi | Lowest | Lowest |
| `acks=1` | Sirf leader acknowledgement | Medium | Medium |
| `acks=all` | Saare in-sync replicas acknowledge | Highest | Highest |

### `acks=all` Example

```text
Producer -> Leader
Leader -> Followers
Followers -> Acknowledge
Leader -> Producer Success
```

`acks=all` strongest durability guarantee deta hai, jab tak kam se kam ek in-sync replica alive hai. [499][501]

---

## 8. Producer Retries and Idempotence

### Retries

Transient failure par producer record dobara bhej sakta hai.

```text
Network timeout
Broker temporarily unavailable
Leader change
```

### Problem: Duplicate Events

```text
Producer sends event
Broker processes event
Acknowledgement lost
Producer retries
Event duplicate ho sakta hai
```

### Solution: Idempotent Producer

```text
enable.idempotence=true
```

Kafka producer ko unique producer ID aur sequence numbers deta hai, jisse retry ke dauran duplicate writes avoid hote hain.

### Recommended Producer Config

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
```

---

## 9. KafkaTemplate in Spring Boot

`KafkaTemplate` Spring Kafka ka high-level producer abstraction hai.

### Dependency

```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
```

### Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
```

### Producer Service

```java
@Service
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public void publishOrderCreated(OrderCreatedEvent event) {
        kafkaTemplate.send(
            "order.order.created.v1",
            event.orderId(),
            event
        );
    }
}
```

### Async Send with Callback

```java
kafkaTemplate.send("order.order.created.v1", event.orderId(), event)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("Kafka publish failed for orderId={}", event.orderId(), ex);
        } else {
            log.info("Published orderId={} partition={} offset={}",
                event.orderId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
        }
    });
```

`KafkaTemplate.send()` by default asynchronous hota hai aur `Future` return karta hai. [500][503]

---

## 10. Kafka Consumer

Consumer Kafka se events read karta hai.

```text
Kafka -> Consumer -> Database / Redis / Email / Analytics
```

### Consumer Responsibilities

```text
1. Kafka se records fetch karna
2. Records deserialize karna
3. Business logic execute karna
4. Errors handle karna
5. Offset commit karna
6. Consumer group participation karna
```

### Consumer Polling Model

Kafka consumer pull-based model use karta hai.

```text
Consumer -> poll() -> Kafka
Kafka -> returns batch of records
Consumer -> process records
Consumer -> commit offset
```

Consumer har poll par last consumed offset se aage records fetch karta hai; restart ke baad wo committed offset se recover karta hai. [497]

---

## 11. Consumer Group

Consumer group ek team hoti hai jo ek topic ko process karti hai.

```text
Topic: order-events
Partitions: 3

Consumer Group: notification-service-group

Consumer 1 -> Partition 0
Consumer 2 -> Partition 1
Consumer 3 -> Partition 2
```

### Core Rule

```text
Ek partition ek time par
ek consumer group me
sirf ek consumer process karta hai.
```

### Scaling Examples

```text
3 partitions + 3 consumers
= full parallelism

3 partitions + 2 consumers
= one consumer handles 2 partitions

3 partitions + 5 consumers
= 2 consumers idle
```

Consumer group partitions ko members me distribute karta hai, jisse horizontal scaling hoti hai. [498][505]

---

## 12. Multiple Consumer Groups

Different consumer groups same topic independently read kar sakte hain.

```text
Topic: order-events

Consumer Group: notification-service-group
   -> Email, SMS

Consumer Group: analytics-service-group
   -> Dashboards

Consumer Group: inventory-service-group
   -> Stock update
```

Benefit:

```text
Naya consumer service add karna easy hai.
Producer code change nahi karna padta.
```

---

## 13. Offset Commit Strategies

| Strategy | Config | Behavior | Risk |
|---|---|---|---|
| Auto commit | `enable.auto.commit=true` | Periodic automatic commit | Processing failure ke baad bhi commit ho sakta hai |
| Manual sync commit | `commitSync()` | Blocking, reliable commit | Higher latency |
| Manual async commit | `commitAsync()` | Non-blocking commit | Failure par callback handle karna hoga |
| Manual per-record commit | `ack.acknowledge()` | Record-level control | More overhead |

### Auto Commit

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-commit-interval: 5000
```

Use case:

```text
Non-critical analytics events
```

### Manual Commit

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual_immediate
```

Use case:

```text
Orders
Payments
Inventory
Financial events
```

Kafka consumer offsets automatically ya manually `commitSync()` / `commitAsync()` se commit kar sakta hai. [497]

---

## 14. Spring Boot Consumer Example

### Configuration

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
    listener:
      ack-mode: manual_immediate
```

### Consumer

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    private final NotificationService notificationService;

    @KafkaListener(
        topics = "order.order.created.v1",
        groupId = "notification-service-group"
    )
    public void consume(
        OrderCreatedEvent event,
        Acknowledgment acknowledgment
    ) {
        try {
            notificationService.sendOrderConfirmation(event);
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process orderId={}", event.orderId(), ex);
            throw ex;
        }
    }
}
```

`@KafkaListener` method ko Kafka listener container me wrap karta hai; manual `ack-mode` me offset `acknowledge()` call hone par commit hota hai. [506]

---

## 15. Consumer Error Handling

### Transient Error

```text
Database temporarily down
Network timeout
External API timeout
```

Action:

```text
Retry
```

### Permanent Error

```text
Invalid JSON
Missing required field
Business rule violation
Unsupported event version
```

Action:

```text
Dead letter topic
```

### Error Handling Rule

```text
Transient error -> Retry
Permanent error -> DLQ
Unknown error -> Alert and investigate
```

---

## 16. Consumer Lag

Consumer lag batata hai ki consumer producer se kitna peeche hai.

```text
Consumer Lag =
Latest Produced Offset - Latest Committed Offset
```

### Example

```text
Latest produced offset = 1000
Latest committed offset = 900

Consumer lag = 100
```

### Lag Increase Means

```text
1. Consumer slow hai
2. Downstream system slow hai
3. Consumer down hai
4. Traffic spike hai
5. Partition skew hai
6. Processing logic heavy hai
```

Consumer lag production Kafka ka critical metric hai, kyunki high lag batata hai ki downstream processing production rate ke saath keep up nahi kar rahi. [466][467]

---

## 17. Consumer Rebalancing

Rebalancing me partitions consumer group members me dobara assign hote hain.

### When It Happens

```text
1. Naya consumer join kare
2. Consumer leave kare
3. Consumer crash kare
4. Deployment ho
5. Session timeout ho
6. `max.poll.interval.ms` exceed ho
```

### Rebalancing Impact

```text
Processing pause
Duplicate processing possibility
Latency spike
Consumer lag increase
```

### Prevention

```text
1. Graceful shutdown
2. Proper timeout tuning
3. Avoid long processing inside listener
4. Cooperative sticky assignor
5. Stable deployment strategy
```

---

## 18. Idempotent Consumer

Kafka at-least-once delivery common hai, isliye consumer duplicates handle kar sakta hona chahiye.

### Pattern

```text
1. Event ID nikalo
2. Check karo event already processed hai?
3. Agar processed hai -> skip
4. Agar nahi -> process
5. Processed event ID save karo
```

### Example Table

```sql
CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
);
```

### Java Logic

```java
@Transactional
public void process(OrderCreatedEvent event) {
    boolean alreadyProcessed = processedEventRepository
        .existsById(event.eventId());

    if (alreadyProcessed) {
        log.info("Duplicate event ignored: {}", event.eventId());
        return;
    }

    orderService.createOrder(event);
    processedEventRepository.save(
        new ProcessedEvent(event.eventId(), Instant.now())
    );
}
```

---

## 19. Producer and Consumer Best Practices

### Producer

```text
1. Meaningful event key use karo
2. `acks=all` use karo critical events ke liye
3. Idempotent producer enable karo
4. Retry limits set karo
5. Delivery timeout set karo
6. Compression enable karo
7. Large payload avoid karo
8. Publish failures monitor karo
```

### Consumer

```text
1. Manual offset commit use karo critical flows me
2. Idempotent consumer banao
3. Retry aur DLQ implement karo
4. Consumer lag monitor karo
5. Long blocking work avoid karo
6. Graceful shutdown implement karo
7. Deserialization errors handle karo
8. Consumer group naming consistent rakho
```

---

## 20. Common Mistakes

### Mistake 1 — Random Key Use Karna

```text
Wrong:
UUID.randomUUID() as key for order events

Correct:
orderId as key
```

### Mistake 2 — Auto Commit for Critical Events

```text
Wrong:
Auto commit before processing completes

Correct:
Manual commit after successful processing
```

### Mistake 3 — No Idempotency

```text
Wrong:
Assume event exactly once aayega

Correct:
Handle duplicate events safely
```

### Mistake 4 — Large Payloads

```text
Wrong:
Entire file or huge object Kafka me bhejna

Correct:
Reference/URL bhejo, data object storage me rakho
```

### Mistake 5 — Ignoring Consumer Lag

```text
Wrong:
Lag alert nahi hai

Correct:
Lag, DLQ and processing latency monitor karo
```

---

## 21. Interview Questions

### Basic

| Question | Answer |
|---|---|
| Producer kya hai? | Kafka topic me events publish karne wali application. |
| Consumer kya hai? | Kafka se events read karne wali application. |
| Consumer group kya hai? | Partitions share karke topic process karne wale consumers ka group. |
| Kafka key kya hai? | Partition selection aur ordering ke liye record identifier. |
| Offset commit kya hai? | Consumer dwara processed position save karna. |
| Consumer lag kya hai? | Latest produced offset aur committed offset ka difference. |

### Intermediate

| Question | Answer |
|---|---|
| Producer batching kya hai? | Same partition ke records ko ek request me bhejna. |
| Idempotent producer kya hai? | Retry ke dauran duplicate writes avoid karne wala producer. |
| Manual offset commit kyun use karte hain? | Successful processing ke baad hi offset commit karne ke liye. |
| Rebalancing kya hai? | Consumer group me partition ownership ka reassignment. |
| Consumer lag high kyun hota hai? | Slow consumer, downstream latency, traffic spike ya partition skew se. |
| Duplicate event kaise handle karein? | Idempotent consumer aur processed-event tracking se. |

### Scenario-Based

| Scenario | Solution |
|---|---|
| Order events ka order maintain karna hai | `orderId` ko Kafka key banao |
| Payment event duplicate process ho raha hai | Idempotent consumer lagao |
| Consumer lag badh raha hai | Consumer health, downstream latency, partition count aur processing logic check karo |
| Kafka publish fail ho raha hai | Retry, timeout, broker health aur producer metrics check karo |
| Naya consumer service add karna hai | Naya consumer group banao |

---

## 22. Key Takeaways

```text
1. Producer Kafka me events publish karta hai.

2. Kafka key partition selection aur ordering control karta hai.

3. Same key wale events same partition me jate hain.

4. Batching throughput improve karti hai.

5. Compression network aur storage cost kam karti hai.

6. Critical events ke liye `acks=all` use karo.

7. Idempotent producer duplicates reduce karta hai.

8. Consumer polling model use karta hai.

9. Consumer group parallel processing enable karta hai.

10. Critical consumers me manual offset commit,
    idempotency, retry aur DLQ hona chahiye.
```

---

## 23. Next Part Preview

**Part 4: Offsets, Commit Strategies and Rebalancing**

Isme hum cover karenge:

```text
- Offset internals
- __consumer_offsets topic
- Auto vs manual commit
- commitSync vs commitAsync
- Rebalancing protocols
- Eager vs cooperative rebalancing
- Consumer lag troubleshooting
- Spring Boot offset configuration
```