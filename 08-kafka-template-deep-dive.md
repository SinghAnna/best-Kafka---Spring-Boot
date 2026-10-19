# Part 8: KafkaTemplate Deep Dive — ProducerFactory, Send APIs, Callbacks, Transactions and Tuning

> `KafkaTemplate` Spring Kafka me Kafka producer operations ka central abstraction hai. Ye `ProducerFactory` se producer create/reuse karta hai, messages ko serialize karta hai, partition assign karta hai, broker ko send karta hai aur delivery result callback deta hai.

---

## 1. KafkaTemplate Kya Hai?

`KafkaTemplate` ek Spring class hai jo Kafka producer ko simplify karti hai.

Simple model:

```text
Application Code
      |
      v
KafkaTemplate
      |
      v
ProducerFactory
      |
      v
KafkaProducer
      |
      v
Kafka Broker
```

`KafkaTemplate` producer factory se construct hota hai aur `send()` ke through messages publish karta hai. [574][578]

### Responsibilities

```text
1. Kafka producer lifecycle manage karna
2. Message serialization
3. Partition selection
4. Message send
5. Async delivery result
6. Callback handling
7. Transaction support
8. Producer metrics integration
```

---

## 2. KafkaTemplate Type Parameters

```java
KafkaTemplate<K, V>
```

| Type Parameter | Meaning | Common Value |
|---|---|---|
| `K` | Key type | `String` |
| `V` | Value type | DTO / Event class |

### Example

```java
KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
```

Matlab:

```text
Key   = String orderId
Value = OrderCreatedEvent JSON
```

---

## 3. ProducerFactory Kya Hai?

`ProducerFactory` Kafka producer instances create aur manage karta hai.

```text
KafkaTemplate
      |
      v
ProducerFactory
      |
      v
KafkaProducer
```

Spring Boot me usually `DefaultKafkaProducerFactory` use hota hai.

`DefaultKafkaProducerFactory` configuration aur serializers ke saath producer factory banata hai; transactions enable karne par ye producer cache maintain karta hai. [574][579]

### Why Factory?

```text
1. Producer creation expensive hota hai
2. Producer reuse performance improve karta hai
3. Configuration centralized rehti hai
4. Transactions aur lifecycle manage hote hain
```

---

## 4. Basic KafkaTemplate Configuration

### application.yml

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
        max.in.flight.requests.per.connection: 5
```

### Automatic Bean

Spring Boot automatically create karta hai:

```text
ProducerFactory
KafkaTemplate
```

Isliye directly inject kar sakte ho:

```java
@Service
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
}
```

---

## 5. Custom KafkaTemplate Configuration

Jab custom control chahiye:

```java
package com.example.kafka.config;

import com.example.kafka.event.OrderCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, OrderCreatedEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate(
        ProducerFactory<String, OrderCreatedEvent> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

---

## 6. `send()` Method Overloads

`KafkaTemplate.send()` ke multiple variants hote hain.

| Method | Use Case |
|---|---|
| `send(topic, data)` | Key ke bina message bhejna |
| `send(topic, key, data)` | Key ke saath message bhejna |
| `send(topic, partition, key, data)` | Specific partition par bhejna |
| `send(topic, partition, timestamp, key, data)` | Timestamp ke saath bhejna |
| `sendDefault(data)` | Default topic par bhejna |
| `sendDefault(key, data)` | Default topic + key |
| `sendDefault(partition, key, data)` | Default topic + partition + key |
| `send(Message<?>)` | Spring Message wrapper |

`sendDefault()` tab use hota hai jab `KafkaTemplate` ko default topic diya gaya ho. [574]

---

## 7. Send Examples

### 7.1 Simple Send

```java
kafkaTemplate.send("order-events", event);
```

### 7.2 Send With Key

```java
kafkaTemplate.send(
    "order-events",
    event.orderId(),
    event
);
```

### 7.3 Send to Specific Partition

```java
kafkaTemplate.send(
    "order-events",
    1,
    event.orderId(),
    event
);
```

> Specific partition manually choose karna usually avoid karo, kyunki partition rebalancing aur scaling complexity badha deta hai.

### 7.4 Send With Headers

```java
Message<OrderCreatedEvent> message = MessageBuilder
    .withPayload(event)
    .setHeader(KafkaHeaders.TOPIC, "order-events")
    .setHeader(KafkaHeaders.KEY, event.orderId())
    .setHeader("traceId", "trace-8821")
    .setHeader("schemaVersion", "1.0")
    .build();

kafkaTemplate.send(message);
```

---

## 8. Async Send and Callback

`KafkaTemplate.send()` asynchronous hota hai aur `CompletableFuture<SendResult<K,V>>` return karta hai.

```java
CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
    kafkaTemplate.send(
        "order.order.created.v1",
        event.orderId(),
        event
    );

future.whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Kafka publish failed", ex);
    } else {
        RecordMetadata metadata = result.getRecordMetadata();

        log.info(
            "Published to topic={}, partition={}, offset={}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }
});
```

### Important Point

```text
send() call successful hona
message broker me successfully deliver hone ka guarantee nahi hai.
```

Isliye callback handle karna important hai.

Spring Kafka me `send()` async hota hai; delivery success/failure async callback se milta hai, aur optionally `ProducerListener` bhi use kar sakte ho. [564][577]

---

## 9. ProducerListener

`ProducerListener` centralized callback hai — har send ke liye alag callback likhne ki zaroorat nahi.

```java
package com.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

@Configuration
@Slf4j
public class KafkaTemplateConfig {

    public KafkaTemplateConfig(
        KafkaTemplate<String, Object> kafkaTemplate
    ) {
        kafkaTemplate.setProducerListener(new ProducerListener<>() {

            @Override
            public void onSuccess(
                String topic,
                Integer partition,
                Object key,
                Object value,
                RecordMetadata recordMetadata
            ) {
                log.info(
                    "Kafka publish success. topic={}, partition={}, offset={}",
                    topic,
                    partition,
                    recordMetadata.offset()
                );
            }

            @Override
            public void onError(
                String topic,
                Integer partition,
                Object key,
                Object value,
                Exception exception
            ) {
                log.error(
                    "Kafka publish failed. topic={}, partition={}, key={}",
                    topic,
                    partition,
                    key,
                    exception
                );
            }
        });
    }
}
```

`ProducerListener` se aapko Future complete hone ka wait kiye bina async success/failure callback mil jata hai. [564][577]

---

## 10. Synchronous Send

Kabhi-kabhi aapko send result ka wait karna ho:

```java
public void publishSync(OrderCreatedEvent event) {
    try {
        SendResult<String, OrderCreatedEvent> result = kafkaTemplate
            .send("order.order.created.v1", event.orderId(), event)
            .get();

        log.info(
            "Published successfully. partition={}, offset={}",
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset()
        );
    } catch (Exception ex) {
        throw new KafkaPublishException(
            "Failed to publish order event",
            ex
        );
    }
}
```

### Tradeoff

```text
Sync send:
+ Delivery confirmation
+ Simpler failure handling
- Higher API latency
- Thread blocking

Async send:
+ Better latency
+ Better throughput
- Callback/error handling zaroori
```

---

## 11. Idempotent Producer

Idempotent producer retry ke dauran duplicate writes avoid karta hai.

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

### Required Conditions

```text
acks = all
retries > 0
max.in.flight.requests.per.connection <= 5
```

Idempotent producer producer ID, epoch aur sequence numbers use karke retries ke bawajood exactly one copy write ensure karta hai. [571][572][576]

### Why Important?

```text
Network timeout
Acknowledgement lost
Producer retry
Duplicate possible

Idempotence:
Duplicate write prevent karti hai
```

---

## 12. Producer Batching

Producer same partition ke records ko batch karta hai.

```text
Record 1 -> Partition 1
Record 2 -> Partition 1
Record 3 -> Partition 1

Batch:
[Record 1, Record 2, Record 3]
```

### Configuration

```yaml
spring:
  kafka:
    producer:
      properties:
        linger.ms: 10
        batch.size: 32768
```

| Setting | Meaning |
|---|---|
| `linger.ms` | Batch bhejne se pehle wait time |
| `batch.size` | Maximum batch size in bytes |
| `buffer.memory` | Producer buffer memory |

Batching latency ke badle throughput improve karti hai; `linger.ms` time-based batching aur `batch.size` size-based batching control karta hai. [575]

---

## 13. Compression

Compression network transfer aur broker storage reduce karti hai.

```yaml
spring:
  kafka:
    producer:
      properties:
        compression.type: lz4
```

| Compression | Tradeoff |
|---|---|
| `none` | No CPU cost, high network usage |
| `gzip` | High compression, high CPU |
| `snappy` | Balanced |
| `lz4` | Fast |
| `zstd` | Modern and efficient |

Compression producer CPU latency add karti hai, lekin request aur disk write size reduce karke throughput improve kar sakti hai. [575]

---

## 14. Kafka Transactions

Kafka transactions multiple partition writes ko atomic banate hain.

```text
Either all messages are written
or none are written
```

### Transactional Producer Config

```java
@Bean
public ProducerFactory<String, OrderCreatedEvent> producerFactory() {
    DefaultKafkaProducerFactory<String, OrderCreatedEvent> factory =
        new DefaultKafkaProducerFactory<>(producerConfigs());

    factory.setTransactionIdPrefix("order-service-tx-");

    return factory;
}
```

### Transactional Send

```java
kafkaTemplate.executeInTransaction(operations -> {
    operations.send("order-events", event);
    operations.send("payment-events", paymentEvent);
    return true;
});
```

### Important Point

```text
Transaction idempotence ke saath kaam karta hai.
```

Transactions, idempotence ke saath use hone par multiple partitions me exactly-once writes provide karte hain. [571][575]

---

## 15. Error Handling Pattern

```java
public CompletableFuture<Void> publish(OrderCreatedEvent event) {
    return kafkaTemplate
        .send("order.order.created.v1", event.orderId(), event)
        .handle((result, ex) -> {
            if (ex != null) {
                log.error(
                    "Kafka publish failed for orderId={}",
                    event.orderId(),
                    ex
                );

                // Options:
                // 1. Retry
                // 2. Save to outbox
                // 3. Alert
                // 4. Dead letter storage

                throw new KafkaPublishException(ex);
            }

            log.info(
                "Published orderId={} to partition={} offset={}",
                event.orderId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset()
            );

            return null;
        });
}
```

### Production Rule

```text
Kafka publish failure ko ignore mat karo.

Options:
1. Retry
2. Outbox table
3. Dead letter storage
4. Alert
5. Circuit breaker
```

---

## 16. Custom Partitioner

Default partitioner key hash use karta hai.

Custom partitioner tab chahiye jab business-specific routing chahiye.

```java
package com.example.kafka.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class TenantPartitioner implements Partitioner {

    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster
    ) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        return Math.abs(key.hashCode()) % partitions.size();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
```

### Register Custom Partitioner

```yaml
spring:
  kafka:
    producer:
      properties:
        partitioner.class: com.example.kafka.config.TenantPartitioner
```

---

## 17. Performance Tuning

| Goal | Configuration |
|---|---|
| Higher throughput | Increase `batch.size`, `linger.ms`, use compression |
| Lower latency | Reduce `linger.ms`, smaller batches |
| Strong durability | `acks=all`, idempotence, retries |
| Lower network usage | Compression |
| Ordered delivery | Same key, `max.in.flight=5` |
| High reliability | `acks=all` + retries + delivery timeout |

### Recommended Production Config

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        linger.ms: 10
        batch.size: 32768
        compression.type: lz4
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
```

---

## 18. Common Mistakes

### Mistake 1 — Send Result Ignore Karna

```text
Wrong:
kafkaTemplate.send(topic, event);

Correct:
Handle CompletableFuture callback
```

### Mistake 2 — Random Key for Ordered Events

```text
Wrong:
UUID.randomUUID() as key

Correct:
orderId / userId / aggregateId as key
```

### Mistake 3 — Sync Send Everywhere

```text
Wrong:
Every API request blocks on Kafka send

Correct:
Async send + callback + outbox for critical flows
```

### Mistake 4 — Idempotence Disable Karna

```text
Wrong:
enable.idempotence=false

Correct:
Critical producer me idempotence enable karo
```

### Mistake 5 — Oversized Messages

```text
Wrong:
Large file Kafka value me bhejna

Correct:
Object storage me file rakho,
Kafka me reference/URL bhejo
```

---

## 19. Interview Questions

| Question | Answer |
|---|---|
| KafkaTemplate kya hai? | Spring Kafka ka high-level Kafka producer abstraction. |
| ProducerFactory kya hai? | Kafka producer instances create/manage karne wali factory. |
| `send()` async kyun hai? | Producer batching aur non-blocking I/O ke liye. |
| ProducerListener kya hai? | Kafka send success/failure ka centralized async callback. |
| Idempotent producer kya hai? | Retry ke dauran duplicate writes avoid karne wala producer. |
| Kafka transaction kya hai? | Multiple partition writes ko atomic banane wala mechanism. |

### Scenario-Based

| Scenario | Solution |
|---|---|
| Kafka publish failure track karna hai | `whenComplete` callback ya `ProducerListener` |
| Order events ka order maintain karna hai | `orderId` key use karo |
| Duplicate publish avoid karna hai | Idempotent producer enable karo |
| Throughput badhana hai | Batching, compression aur partitioning tune karo |
| DB write + Kafka publish atomic chahiye | Outbox pattern use karo |

---

## 20. Key Takeaways

```text
1. KafkaTemplate Spring Kafka producer ka core abstraction hai.

2. ProducerFactory producer lifecycle manage karta hai.

3. send() asynchronous hota hai.

4. Delivery result callback me milta hai.

5. ProducerListener centralized callback deta hai.

6. Idempotent producer retry duplicates avoid karta hai.

7. Batching throughput improve karti hai.

8. Compression network/storage cost kam karti hai.

9. Transactions multi-partition atomic writes dete hain.

10. Critical flows me publish failure ko
    retry/outbox/alert se handle karo.
```

---

## 21. Next Part Preview

**Part 9: `@KafkaListener` Deep Dive**

Isme hum cover karenge:

```text
- Listener container architecture
- @KafkaListener attributes
- Consumer group configuration
- Manual acknowledgment
- Concurrency
- Error handlers
- Retry and DLT
- Batch listeners
- Message headers
- Dynamic topic listeners
```