# Part 7: Spring Kafka Setup — Dependencies, Configuration and First Producer/Consumer

> Is part me hum Spring Boot application me Apache Kafka integrate karenge. Hum Maven dependencies, Docker Compose, `application.yml`, `KafkaTemplate`, `@KafkaListener`, JSON serialization aur ek complete end-to-end producer-consumer flow banayenge.

---

## 1. What We Will Build

```text
POST /api/v1/orders
        |
        v
Order Service
        |
        | Publish ORDER_CREATED
        v
Kafka Topic: order.order.created.v1
        |
        v
Notification Service
        |
        v
Process event + log confirmation
```

### End Goal

```text
1. Kafka Docker me start hoga
2. Spring Boot producer event publish karega
3. Spring Boot consumer event consume karega
4. JSON serialization use hoga
5. Manual offset commit use hoga
6. End-to-end flow test hoga
```

---

## 2. Project Structure

```text
kafka-spring-boot-guide/
├── docker/
│   └── docker-compose.yml
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/kafka/
│       │       ├── KafkaSpringBootGuideApplication.java
│       │       ├── config/
│       │       │   └── KafkaTopicConfig.java
│       │       ├── controller/
│       │       │   └── OrderController.java
│       │       ├── consumer/
│       │       │   └── OrderCreatedConsumer.java
│       │       ├── producer/
│       │       │   └── OrderEventProducer.java
│       │       └── event/
│       │           └── OrderCreatedEvent.java
│       └── resources/
│           └── application.yml
└── pom.xml
```

---

## 3. Maven Dependencies

`pom.xml`:

```xml
<dependencies>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
  </dependency>

  <dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
  </dependency>

  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-validation</artifactId>
  </dependency>

  <dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <optional>true</optional>
  </dependency>

  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
  </dependency>

  <dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

### Important Dependency

```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
```

Ye dependency Spring Kafka integration, `KafkaTemplate`, `@KafkaListener`, serializers aur listener containers provide karti hai.

---

## 4. Docker Compose for Kafka

`docker/docker-compose.yml`:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093

      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:29093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - kafka_data:/var/lib/kafka/data

volumes:
  kafka_data:
```

### Start Kafka

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Verify Kafka

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Kafka Docker image me console producer aur consumer CLI tools included hote hain. [566]

---

## 5. `application.yml`

```yaml
spring:
  application:
    name: kafka-spring-boot-guide

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
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        linger.ms: 10
        compression.type: lz4

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

    listener:
      ack-mode: manual_immediate

kafka:
  topic:
    order-created: order.order.created.v1

server:
  port: 8080
```

### Important Configuration

| Property | Purpose |
|---|---|
| `bootstrap-servers` | Kafka broker address |
| `acks: all` | Strong producer durability |
| `enable.idempotence` | Duplicate producer writes avoid |
| `JsonSerializer` | Java object to JSON |
| `JsonDeserializer` | JSON to Java object |
| `spring.json.trusted.packages` | Safe deserialization packages |
| `enable-auto-commit: false` | Manual offset commit |
| `ack-mode: manual_immediate` | Acknowledge par immediate commit |

Spring Boot `spring.kafka.properties.*` se direct Kafka client properties pass ki jati hain, aur standard properties ke liye strongly typed `KafkaProperties` use hota hai. [569]

---

## 6. Main Application Class

`KafkaSpringBootGuideApplication.java`:

```java
package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSpringBootGuideApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringBootGuideApplication.class, args);
    }
}
```

---

## 7. Kafka Topic Configuration

`config/KafkaTopicConfig.java`:

```java
package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.order-created}")
    private String orderCreatedTopic;

    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder
            .name(orderCreatedTopic)
            .partitions(3)
            .replicas(1)
            .build();
    }
}
```

### Production Note

```text
Local development:
partitions = 3
replicas = 1

Production:
partitions = workload-based
replicas = 3
min.insync.replicas = 2
```

---

## 8. Event DTO

`event/OrderCreatedEvent.java`:

```java
package com.example.kafka.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderCreatedEvent(
    String eventId,
    String eventType,
    String orderId,
    String userId,
    List<OrderItem> items,
    BigDecimal totalAmount,
    Instant occurredAt,
    EventMetadata metadata
) {

    public record OrderItem(
        @NotBlank String productId,
        @NotNull @Positive Integer quantity
    ) {}

    public record EventMetadata(
        String source,
        String traceId,
        String schemaVersion
    ) {}

    public static OrderCreatedEvent create(
        String orderId,
        String userId,
        List<OrderItem> items,
        BigDecimal totalAmount,
        String traceId
    ) {
        return new OrderCreatedEvent(
            UUID.randomUUID().toString(),
            "ORDER_CREATED",
            orderId,
            userId,
            items,
            totalAmount,
            Instant.now(),
            new EventMetadata(
                "order-service",
                traceId,
                "1.0"
            )
        );
    }
}
```

---

## 9. Kafka Producer

`producer/OrderEventProducer.java`:

```java
package com.example.kafka.producer;

import com.example.kafka.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Value("${kafka.topic.order-created}")
    private String orderCreatedTopic;

    public CompletableFuture<SendResult<String, OrderCreatedEvent>> publishOrderCreated(
        OrderCreatedEvent event
    ) {
        log.info(
            "Publishing ORDER_CREATED event. orderId={}, eventId={}",
            event.orderId(),
            event.eventId()
        );

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
            kafkaTemplate.send(
                orderCreatedTopic,
                event.orderId(),
                event
            );

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error(
                    "Failed to publish ORDER_CREATED event. orderId={}, eventId={}",
                    event.orderId(),
                    event.eventId(),
                    ex
                );
            } else {
                log.info(
                    "ORDER_CREATED published. topic={}, partition={}, offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            }
        });

        return future;
    }
}
```

### Why `orderId` as Key?

```text
Same orderId -> Same partition -> Ordered events
```

`KafkaTemplate` producer ko wrap karta hai aur `send()` method se messages Kafka topic me publish karta hai. [561]

---

## 10. REST Controller

`controller/OrderController.java`:

```java
package com.example.kafka.controller;

import com.example.kafka.event.OrderCreatedEvent;
import com.example.kafka.producer.OrderEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    @PostMapping
    public ResponseEntity<OrderCreatedEvent> createOrder(
        @Valid @RequestBody CreateOrderRequest request,
        @RequestHeader(value = "X-Trace-Id", required = false) String traceId
    ) {
        String finalTraceId = traceId != null
            ? traceId
            : UUID.randomUUID().toString();

        OrderCreatedEvent event = OrderCreatedEvent.create(
            request.orderId(),
            request.userId(),
            request.items(),
            request.totalAmount(),
            finalTraceId
        );

        orderEventProducer.publishOrderCreated(event);

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(event);
    }

    public record CreateOrderRequest(
        String orderId,
        String userId,
        java.util.List<OrderCreatedEvent.OrderItem> items,
        java.math.BigDecimal totalAmount
    ) {}
}
```

---

## 11. Kafka Consumer

`consumer/OrderCreatedConsumer.java`:

```java
package com.example.kafka.consumer;

import com.example.kafka.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    @KafkaListener(
        topics = "${kafka.topic.order-created}",
        groupId = "notification-service-group"
    )
    public void consume(
        OrderCreatedEvent event,
        Acknowledgment acknowledgment
    ) {
        try {
            log.info(
                "Received ORDER_CREATED event. orderId={}, userId={}, totalAmount={}",
                event.orderId(),
                event.userId(),
                event.totalAmount()
            );

            processOrderCreatedEvent(event);

            acknowledgment.acknowledge();

            log.info(
                "Successfully processed and committed orderId={}",
                event.orderId()
            );
        } catch (Exception ex) {
            log.error(
                "Failed to process ORDER_CREATED event. orderId={}",
                event.orderId(),
                ex
            );

            throw ex;
        }
    }

    private void processOrderCreatedEvent(OrderCreatedEvent event) {
        // In real application:
        // 1. Save notification record
        // 2. Send email/SMS
        // 3. Update analytics
        // 4. Trigger downstream workflow

        log.info(
            "Sending order confirmation for user={} order={}",
            event.userId(),
            event.orderId()
        );
    }
}
```

`@KafkaListener` bean method ko listener container me wrap karta hai, jo message delivery, consumer group coordination aur offset commit handle karta hai. [561]

---

## 12. Run the Application

### Step 1 — Start Kafka

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Step 2 — Start Spring Boot

```bash
./mvnw spring-boot:run
```

### Step 3 — Publish Event

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order_8721",
    "userId": "user_101",
    "items": [
      {
        "productId": "prod_55",
        "quantity": 2
      }
    ],
    "totalAmount": 2499.00
  }'
```

### Expected Logs

Producer:

```text
Publishing ORDER_CREATED event. orderId=order_8721
ORDER_CREATED published. topic=order.order.created.v1 partition=0 offset=0
```

Consumer:

```text
Received ORDER_CREATED event. orderId=order_8721
Sending order confirmation for user=user_101 order=order_8721
Successfully processed and committed orderId=order_8721
```

---

## 13. Verify Using Kafka CLI

### Check Topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order.order.created.v1
```

### Read Messages

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order.order.created.v1 \
  --from-beginning
```

### Check Consumer Group

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group
```

---

## 14. Common Setup Errors

### Error 1 — Connection Refused

```text
Connection refused: localhost/127.0.0.1:9092
```

Fix:

```bash
docker compose ps
docker compose logs kafka
```

Check:

```text
Kafka container running hai?
Port 9092 exposed hai?
`bootstrap-servers` correct hai?
```

### Error 2 — Topic Not Found

```text
Topic order.order.created.v1 not found
```

Fix:

```text
KafkaTopicConfig verify karo
Application restart karo
Topic manually create karo
```

### Error 3 — Deserialization Failure

```text
No type information in headers
```

Fix:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.use.type.headers: false
        spring.json.value.default.type: com.example.kafka.event.OrderCreatedEvent
```

`JsonDeserializer` ko target type information chahiye hoti hai — type headers ya default type configuration se. [564][567]

### Error 4 — Consumer Not Receiving Messages

```text
Check:
1. Consumer group ID
2. Topic name
3. Kafka bootstrap server
4. Consumer container health
5. Serializer/deserializer mismatch
```

---

## 15. Production Best Practices

### Configuration

```text
1. Kafka bootstrap servers environment-specific rakho
2. Secrets environment variables/secret manager se lo
3. Producer me acks=all use karo
4. Idempotent producer enable karo
5. Consumer me manual offset commit use karo
6. Trusted packages restrict karo
7. Auto topic creation disable rakho
```

### Reliability

```text
1. Producer send callback handle karo
2. Consumer processing failure handle karo
3. Retry aur DLQ plan karo
4. Consumer lag monitor karo
5. Graceful shutdown implement karo
```

### Observability

```text
1. Producer success/failure metrics
2. Consumer processing latency
3. Consumer lag
4. DLQ message count
5. Rebalancing events
```

---

## 16. Interview Questions

| Question | Answer |
|---|---|
| Spring Kafka kya hai? | Spring applications me Apache Kafka integrate karne wala project. |
| KafkaTemplate kya hai? | Kafka producer operations ka high-level Spring abstraction. |
| `@KafkaListener` kya hai? | Kafka topic se messages consume karne wala annotation-based listener. |
| JsonSerializer kya karta hai? | Java object ko JSON bytes me serialize karta hai. |
| JsonDeserializer kya karta hai? | JSON bytes ko Java object me deserialize karta hai. |
| Manual offset commit kyun use karte hain? | Successful processing ke baad hi offset commit karne ke liye. |

---

## 17. Key Takeaways

```text
1. Spring Kafka Kafka integration ko simplify karta hai.

2. KafkaTemplate producer operations provide karta hai.

3. @KafkaListener consumer operations provide karta hai.

4. JSON serialization common event format hai.

5. Producer me key important hoti hai ordering ke liye.

6. Manual offset commit critical consumers ke liye better hai.

7. Docker se local Kafka easily start hota hai.

8. Serializer mismatch common integration error hai.

9. Producer callback se delivery result track hota hai.

10. End-to-end producer-consumer flow
    production Kafka design ka foundation hai.
```

---

## 18. Next Part Preview

**Part 8: KafkaTemplate Deep Dive**

Isme hum cover karenge:

```text
- KafkaTemplate internals
- ProducerFactory
- DefaultKafkaProducerFactory
- send() overloads
- sendDefault()
- Callback handling
- ProducerListener
- Transactions
- Custom serializers
- Error handling
- Performance tuning
```