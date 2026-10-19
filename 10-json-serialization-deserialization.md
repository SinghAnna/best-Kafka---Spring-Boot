# Part 10: JSON Serialization and Deserialization — Complete Deep Dive

> Kafka me Java objects directly nahi jaate — unhe bytes me serialize karna padta hai. Spring Kafka ka `JsonSerializer` Java object ko JSON me convert karta hai aur `JsonDeserializer` JSON ko wapas Java object me. Production me type headers, trusted packages, schema versioning aur error handling properly configure karna zaroori hai.

---

## 1. Serialization Kya Hai?

Serialization ka matlab hai Java object ko bytes me convert karna.

```text
Java Object
    |
    | JsonSerializer
    v
JSON Bytes
    |
    v
Kafka Broker
```

Deserialization reverse process hai:

```text
Kafka Broker
    |
    v
JSON Bytes
    |
    | JsonDeserializer
    v
Java Object
```

### Why Needed?

Kafka network aur disk par byte arrays store/transfer karta hai, isliye producer ko object serialize karna hota hai aur consumer ko deserialize.

---

## 2. Serialization Options

| Format | Pros | Cons | Best For |
|---|---|---|---|
| JSON | Human-readable, easy debugging | Larger payload, slower | General APIs and events |
| Avro | Compact, strong schema evolution | Schema Registry required | Enterprise event streaming |
| Protobuf | Compact, fast, strongly typed | Schema/tooling required | High-performance systems |
| String | Simple | No structure, weak contract | Simple text messages |
| Custom binary | Very compact | Hard to debug/maintain | Specialized systems |

Spring Kafka me JSON sabse common starting point hai; production-scale event contracts ke liye Avro/Protobuf + Schema Registry often preferred hota hai. [591][593]

---

## 3. Basic JSON Configuration

### Producer

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

### Consumer

```yaml
spring:
  kafka:
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafka.event"
```

---

## 4. Event DTO

```java
package com.example.kafka.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

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
        String productId,
        Integer quantity
    ) {}

    public record EventMetadata(
        String source,
        String traceId,
        String schemaVersion
    ) {}
}
```

### Serialized JSON

```json
{
  "eventId": "evt_9921",
  "eventType": "ORDER_CREATED",
  "orderId": "order_8721",
  "userId": "user_101",
  "items": [
    {
      "productId": "prod_55",
      "quantity": 2
    }
  ],
  "totalAmount": 2499.00,
  "occurredAt": "2026-10-19T14:30:00Z",
  "metadata": {
    "source": "order-service",
    "traceId": "trace-8821",
    "schemaVersion": "1.0"
  }
}
```

---

## 5. Type Headers

By default, Spring Kafka `JsonSerializer` ek `__TypeId__` header add karta hai jisme Java class ka fully qualified name hota hai.

```text
Header:
__TypeId__ = com.example.kafka.event.OrderCreatedEvent
```

Consumer is header se decide karta hai ki JSON ko kis Java class me deserialize karna hai. [595][598]

### Producer Type Headers

```yaml
spring:
  kafka:
    producer:
      properties:
        spring.json.add.type.headers: true
```

### Consumer Type Headers

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.use.type.headers: true
        spring.json.trusted.packages: "com.example.kafka.event"
```

---

## 6. Type Headers Disable Karna

Kabhi-kabhi aap fixed event type use karte ho aur type headers expose nahi karna chahte.

```yaml
spring:
  kafka:
    producer:
      properties:
        spring.json.add.type.headers: false

    consumer:
      properties:
        spring.json.use.type.headers: false
        spring.json.value.default.type: com.example.kafka.event.OrderCreatedEvent
```

### Benefit

```text
1. Smaller headers
2. Less coupling between producer/consumer Java class names
3. Better cross-language compatibility
```

### Tradeoff

```text
Consumer ko expected type explicitly batana padta hai.
```

---

## 7. Trusted Packages

`JsonDeserializer` security ke liye trusted packages check karta hai.

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "com.example.kafka.event"
```

Matlab:

```text
Sirf com.example.kafka.event package ki classes
deserialize ho sakti hain.
```

### Multiple Packages

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "com.example.kafka.event,com.example.payment.event"
```

### Never Do This in Production

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "*"
```

`*` matlab saari Java classes trusted hain — ye deserialization attack risk badha deta hai.

Trusted packages ek security whitelist hain; `JsonDeserializer` type headers se aane wali classes ko untrusted packages se instantiate karne se reject kar deta hai. [595][598][600]

---

## 8. Custom ObjectMapper

Custom `ObjectMapper` se dates, nulls, naming strategy aur unknown properties control karte hain.

### ObjectMapper Bean

```java
package com.example.kafka.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
```

### Producer Factory with Custom Mapper

```java
package com.example.kafka.config;

import com.example.kafka.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    public ProducerFactory<String, OrderCreatedEvent> producerFactory(
        ObjectMapper kafkaObjectMapper
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        JsonSerializer<OrderCreatedEvent> jsonSerializer =
            new JsonSerializer<>(kafkaObjectMapper);

        return new DefaultKafkaProducerFactory<>(
            props,
            new StringSerializer(),
            jsonSerializer
        );
    }

    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate(
        ProducerFactory<String, OrderCreatedEvent> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

Custom `ObjectMapper` ko directly `JsonSerializer`/`JsonDeserializer` constructor me inject kiya ja sakta hai. [596][597]

---

## 9. Consumer Factory with Custom Mapper

```java
package com.example.kafka.config;

import com.example.kafka.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory(
        ObjectMapper kafkaObjectMapper
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "notification-service-group");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JsonDeserializer<OrderCreatedEvent> jsonDeserializer =
            new JsonDeserializer<>(kafkaObjectMapper);

        jsonDeserializer.addTrustedPackages("*com.example.kafka.event");

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            jsonDeserializer
        );
    }
}
```

---

## 10. Serialization Failure Handling

### Common Failure

```text
JsonProcessingException
InvalidFormatException
UnrecognizedPropertyException
ClassNotFoundException
```

### Producer Failure

```text
Object serialize nahi hua
-> Message Kafka me send nahi hua
-> Producer error handle karo
```

### Consumer Failure

```text
JSON invalid
Class mismatch
Unknown field
Type header mismatch
-> Deserialization exception
-> Retry/DLQ strategy
```

### Best Practice

```text
Unknown fields ignore karo,
lekin invalid schema silently ignore mat karo.
```

```java
ObjectMapper mapper = new ObjectMapper()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
```

---

## 11. Schema Versioning

Event schema time ke saath change hota hai.

### Version Header

```text
schemaVersion = 1.0
```

### Event Example

```json
{
  "eventId": "evt_9921",
  "eventType": "ORDER_CREATED",
  "schemaVersion": "1.0",
  "orderId": "order_8721",
  "userId": "user_101",
  "totalAmount": 2499.00
}
```

### New Field Add Karna

```json
{
  "eventId": "evt_9921",
  "eventType": "ORDER_CREATED",
  "schemaVersion": "1.1",
  "orderId": "order_8721",
  "userId": "user_101",
  "totalAmount": 2499.00,
  "couponCode": null
}
```

### Compatibility Rules

| Change | Backward Compatible? | Forward Compatible? |
|---|---|---|
| Optional field add karna | Usually yes | Usually yes |
| Required field add karna | Risky | Risky |
| Field remove karna | Risky | Risky |
| Field type change karna | Usually breaking | Usually breaking |
| Default value add karna | Usually safe | Usually safe |

Schema evolution me backward compatibility ka matlab hai new schema old data read kar sake, aur forward compatibility ka matlab hai old schema new data read kar sake. [591][602]

---

## 12. Schema Evolution Best Practices

```text
1. Event me schemaVersion field rakho.
2. Naye fields optional rakho.
3. Existing field ka type change avoid karo.
4. Field remove karne se pehle deprecation phase rakho.
5. Consumer ko unknown fields tolerate karne do.
6. Breaking change ke liye naya event type/version use karo.
7. Schema Registry use karo for Avro/Protobuf/JSON Schema.
8. Contract tests add karo.
```

### Safe Evolution Example

```text
v1:
orderId
userId
totalAmount

v1.1:
orderId
userId
totalAmount
couponCode (optional)
```

### Breaking Change Example

```text
v1:
amount: number

v2:
amount: string

Better:
New event type/version publish karo
```

---

## 13. Avro vs JSON

| Feature | JSON | Avro |
|---|---|---|
| Readability | High | Low |
| Payload size | Larger | Smaller |
| Schema enforcement | Optional | Strong |
| Schema Registry | Optional | Usually required |
| Evolution | Manual discipline | Built-in rules |
| Cross-language support | Easy | Strong |
| Best for | Simple services, debugging | Enterprise event contracts |

Avro schema evolution aur compatibility rules built-in provide karta hai, isliye large event-driven systems me commonly use hota hai. [593][594][601]

---

## 14. Production Checklist

### Serialization

- [ ] Event DTO immutable record/class hai.
- [ ] Event me `eventId` hai.
- [ ] Event me `eventType` hai.
- [ ] Event me `schemaVersion` hai.
- [ ] Event me `occurredAt` timestamp hai.
- [ ] Producer aur consumer same serialization contract use karte hain.
- [ ] Sensitive fields encrypt/mask hote hain.

### Security

- [ ] `spring.json.trusted.packages` restricted hai.
- [ ] `trusted.packages=*` production me use nahi ho raha.
- [ ] Deserialization errors monitored hain.
- [ ] Unknown fields policy defined hai.
- [ ] Schema changes review process me hain.

### Reliability

- [ ] Producer serialization failure handled hai.
- [ ] Consumer deserialization failure handled hai.
- [ ] Retry/DLQ strategy defined hai.
- [ ] Schema compatibility tests hain.
- [ ] Event versioning documented hai.

---

## 15. Common Mistakes

### Mistake 1 — `trusted.packages=*`

```text
Wrong:
Security risk

Correct:
Sirf event package whitelist karo
```

### Mistake 2 — JPA Entity Directly Kafka Me Bhejna

```text
Wrong:
Lazy loading, circular references, tight coupling

Correct:
Dedicated event DTO banao
```

### Mistake 3 — Schema Versioning Ignore Karna

```text
Wrong:
Field silently add/remove/change

Correct:
Versioned, backward-compatible schema changes
```

### Mistake 4 — Unknown Field Failure

```text
Wrong:
Consumer naye field par crash

Correct:
Unknown fields ignore karo
```

### Mistake 5 — Large JSON Payload

```text
Wrong:
Poora file JSON me bhejna

Correct:
Reference/URL bhejo, file object storage me rakho
```

---

## 16. Interview Questions

| Question | Answer |
|---|---|
| Serialization kya hai? | Java object ko bytes me convert karna. |
| Deserialization kya hai? | Bytes ko wapas Java object me convert karna. |
| JsonSerializer kya hai? | Spring Kafka ka JSON object-to-bytes serializer. |
| JsonDeserializer kya hai? | Spring Kafka ka JSON bytes-to-object deserializer. |
| Type header kya hai? | Kafka message header jisme serialized class ka type info hota hai. |
| Trusted packages kya hai? | Deserialization ke liye allowed Java packages ki security whitelist. |

### Scenario-Based

| Scenario | Solution |
|---|---|
| Consumer unknown field par crash ho raha hai | `FAIL_ON_UNKNOWN_PROPERTIES=false` |
| Producer aur consumer alag services me hain | Stable event contract + schema versioning |
| Security improve karni hai | Trusted packages restrict karo |
| Event schema change karna hai | Optional fields, versioning aur compatibility tests |
| Enterprise event contracts chahiye | Avro/Protobuf + Schema Registry |

---

## 17. Key Takeaways

```text
1. Kafka me objects bytes me travel karte hain.

2. JSON human-readable aur simple hai.

3. JsonSerializer Java object ko JSON banata hai.

4. JsonDeserializer JSON ko Java object banata hai.

5. Type headers deserialization type decide karte hain.

6. Trusted packages deserialization security dete hain.

7. Production me `trusted.packages=*` avoid karo.

8. Custom ObjectMapper dates aur unknown fields control karta hai.

9. Schema versioning backward compatibility ke liye zaroori hai.

10. Large-scale systems me Avro/Protobuf +
    Schema Registry better contract management deta hai.
```

---

## 18. Next Part Preview

**Part 11: Kafka Configuration with `application.yml`**

Isme hum cover karenge:

```text
- Producer configuration
- Consumer configuration
- Listener configuration
- SSL/SASL configuration
- Environment-specific profiles
- Externalized configuration
- Kubernetes ConfigMap/Secret integration
- Common configuration mistakes
```