# Part 4: Offsets, Commit Strategies and Rebalancing — Complete Deep Dive

> Kafka consumer reliability ka core offset management hai. Offset batata hai ki consumer ne partition ka kitna data process kiya, commit strategy decide karti hai ki restart ke baad processing kahan se resume hogi, aur rebalancing partitions ko consumers me dobara distribute karti hai.

---

## 1. Offset Kya Hai?

Offset ek partition ke andar message ki unique position hai.

```text
Partition 0

Offset 0 -> ORDER_CREATED
Offset 1 -> PAYMENT_COMPLETED
Offset 2 -> ORDER_SHIPPED
Offset 3 -> ORDER_DELIVERED
```

Offset ek counter ki tarah hai:

```text
Offset 0 = pehla message
Offset 1 = doosra message
Offset 2 = teesra message
```

Consumer offset track karta hai taaki restart, crash ya rebalance ke baad wahi se processing resume kar sake. [497][527]

---

## 2. Offset Types

| Offset Type | Meaning |
|---|---|
| Produced Offset | Kafka me latest available message position |
| Consumed Offset | Consumer ne abhi tak jo message process kiya |
| Committed Offset | Consumer ne Kafka ko bataya ki “maine yahan tak process kar liya” |

### Example

```text
Latest produced offset = 1000
Consumer processed offset = 950
Consumer committed offset = 940

Consumer lag = 1000 - 940 = 60
Uncommitted processed records = 950 - 940 = 10
```

---

## 3. Offset Commit Kyun Zaroori Hai?

Agar consumer offset commit nahi karega:

```text
Consumer restart hoga
-> Kafka ko nahi pata hoga consumer kahan tak pahuncha
-> Consumer default offset reset strategy follow karega
```

Agar consumer commit kar deta hai:

```text
Consumer restart hoga
-> Kafka committed offset se resume karega
```

### Example

```text
Committed Offset = 940

Consumer restart
-> Offset 941 se processing resume
```

---

## 4. `__consumer_offsets` Topic

Kafka committed offsets ko internal topic me store karta hai:

```text
__consumer_offsets
```

Ye topic Kafka internally manage karta hai.

Isme store hota hai:

```text
Consumer group ID
Topic name
Partition number
Committed offset
Metadata
Timestamp
```

### Example Logical Record

```text
Group: notification-service-group
Topic: order-events
Partition: 0
Committed Offset: 940
```

Committed offsets Kafka ke internal `__consumer_offsets` topic me store hote hain. [527]

---

## 5. Offset Reset Strategies

Jab consumer ke paas valid committed offset nahi hota, tab `auto.offset.reset` decide karta hai ki processing kahan se start hogi.

| Strategy | Behavior |
|---|---|
| `earliest` | Partition ke beginning se read karo |
| `latest` | Sirf naye messages read karo |
| `none` | Valid offset na ho to error do |

### `earliest`

```text
Consumer naya group hai
-> Partition start se read karega
```

### `latest`

```text
Consumer naya group hai
-> Sirf naye messages read karega
```

### `none`

```text
Committed offset missing
-> Consumer fail hoga
```

### Recommended

```text
Critical business consumers: earliest
Real-time monitoring consumers: latest
Controlled deployments: none
```

---

## 6. Auto Offset Commit

Auto commit me Kafka consumer periodically offset commit karta hai.

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-commit-interval: 5000
```

Matlab:

```text
Har 5 second me offset commit
```

### Problem

```text
Consumer message process kar raha hai
Auto commit ho gaya
Consumer crash
Message processed nahi hua
Lekin offset committed hai
= Message loss possible
```

### When to Use

```text
Non-critical analytics
Metrics
Logs
Clickstream
```

### When Not to Use

```text
Orders
Payments
Inventory
Notifications
Financial events
```

---

## 7. Manual Offset Commit

Manual commit me consumer khud decide karta hai ki offset kab commit karna hai.

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual_immediate
```

### Flow

```text
Consumer receives event
Consumer processes event
Processing successful
Consumer acknowledges
Offset committed
```

### Best Practice

```text
Critical business events:
Manual offset commit use karo
```

---

## 8. `commitSync()` vs `commitAsync()`

| Method | Behavior | Reliability | Latency |
|---|---|---|---|
| `commitSync()` | Blocking commit | High | Higher |
| `commitAsync()` | Non-blocking commit | Retry logic khud handle karna hota hai | Lower |

### `commitSync()`

```java
consumer.commitSync();
```

```text
Current thread block hota hai
Commit success hone tak wait karta hai
```

### `commitAsync()`

```java
consumer.commitAsync();
```

```text
Current thread block nahi hota
Commit background me hota hai
Callback me success/failure milta hai
```

`commitSync()` commit acknowledge hone tak block karta hai, jabki `commitAsync()` background me commit karta hai aur consumer processing continue kar sakta hai. [521][527][533]

---

## 9. Spring Kafka Ack Modes

Spring Kafka listener container `ackMode` se decide karta hai ki offset kab commit honge.

| Ack Mode | Meaning |
|---|---|
| `BATCH` | Poll batch process hone ke baad commit |
| `RECORD` | Har record process hone ke baad commit |
| `TIME` | Fixed time interval par commit |
| `COUNT` | Fixed record count par commit |
| `COUNT_TIME` | Count ya time, jo pehle ho |
| `MANUAL` | Listener `acknowledge()` call karta hai; commit next poll se pehle |
| `MANUAL_IMMEDIATE` | Listener `acknowledge()` call karta hai; immediate commit |

Spring Kafka ka default `ackMode` `BATCH` hota hai, aur `MANUAL_IMMEDIATE` me listener acknowledge karte hi commit perform hota hai. [524][525][531]

### Recommended

```text
Critical business consumer:
MANUAL_IMMEDIATE

High-throughput non-critical consumer:
BATCH

Per-record strict processing:
RECORD
```

---

## 10. Spring Boot Manual Commit Example

### Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
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

            log.info(
                "Processed and committed orderId={}",
                event.orderId()
            );
        } catch (Exception ex) {
            log.error(
                "Processing failed for orderId={}",
                event.orderId(),
                ex
            );

            throw ex;
        }
    }
}
```

### Important Point

```text
acknowledge() call nahi kiya
= offset commit nahi hoga
= restart par event dobara process hoga
```

---

## 11. At-Most-Once, At-Least-Once, Exactly-Once

| Semantics | Offset Strategy | Risk |
|---|---|---|
| At-most-once | Process se pehle commit | Data loss possible |
| At-least-once | Process ke baad commit | Duplicate possible |
| Exactly-once | Idempotency + transactions | Complex setup |

### At-Most-Once

```text
Offset commit
-> Process
```

Agar process fail:

```text
Message lost
```

### At-Least-Once

```text
Process
-> Offset commit
```

Agar commit se pehle crash:

```text
Message dobara process hoga
```

### Exactly-Once

```text
Process
-> Idempotency
-> Transactional commit
```

---

## 12. Idempotent Consumer

At-least-once me duplicates aane possible hain, isliye consumer idempotent hona chahiye.

### Duplicate Scenario

```text
Consumer processes event
Offset commit fail
Consumer restart
Same event dobara process
```

### Solution

```text
Event ID track karo
Agar already processed -> skip
Agar nahi -> process and save event ID
```

### Database Table

```sql
CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
);
```

### Java Example

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
        new ProcessedEvent(
            event.eventId(),
            Instant.now()
        )
    );
}
```

---

## 13. Consumer Group Kya Hai?

Consumer group ek team hoti hai jo ek ya multiple topics ko process karti hai.

```text
Topic: order-events
Partitions: 6

Consumer Group: notification-service-group

Consumer 1 -> Partition 0, Partition 1
Consumer 2 -> Partition 2, Partition 3
Consumer 3 -> Partition 4, Partition 5
```

### Core Rule

```text
Ek partition ek time par
ek consumer group me
sirf ek consumer process karta hai.
```

### Scaling Rule

```text
Consumers <= Partitions
= Full parallelism possible

Consumers > Partitions
= Extra consumers idle
```

---

## 14. Group Coordinator

Group Coordinator ek Kafka broker hota hai jo consumer group manage karta hai.

Responsibilities:

```text
1. Consumer group members track karna
2. Member join/leave handle karna
3. Rebalancing trigger karna
4. Group state maintain karna
5. Offset commit requests handle karna
```

---

## 15. Rebalancing Kya Hai?

Rebalancing ka matlab hai partitions ka consumer group members me dobara assignment.

```text
Before:

Consumer 1 -> Partition 0, 1
Consumer 2 -> Partition 2, 3
Consumer 3 -> Partition 4, 5

Consumer 3 down

After:

Consumer 1 -> Partition 0, 1, 4
Consumer 2 -> Partition 2, 3, 5
```

### When Rebalancing Happens

```text
1. Naya consumer join kare
2. Existing consumer leave kare
3. Consumer crash ho jaye
4. Deployment ho
5. Session timeout ho
6. `max.poll.interval.ms` exceed ho
7. Subscription change ho
8. Topic partition count change ho
```

Rebalancing consumer group members ke beech partition ownership assign ya reassign karne ka process hai. [488][495]

---

## 16. Eager Rebalancing

Eager rebalancing me rebalance start hote hi consumers apne saare partitions revoke kar dete hain.

```text
Before rebalance:

Consumer 1 -> P0, P1
Consumer 2 -> P2, P3
Consumer 3 -> P4, P5

Rebalance start:

Consumer 1 -> Revoke P0, P1
Consumer 2 -> Revoke P2, P3
Consumer 3 -> Revoke P4, P5

New assignment:

Consumer 1 -> P0, P1
Consumer 2 -> P2, P3, P4
Consumer 3 -> P5
```

### Problem

```text
Saare partitions revoke hote hain
Processing pause hoti hai
Latency spike aata hai
```

---

## 17. Cooperative Rebalancing

Cooperative rebalancing me sirf affected partitions move hote hain.

```text
Before:

Consumer 1 -> P0, P1
Consumer 2 -> P2, P3
Consumer 3 -> P4, P5

Consumer 3 down

After:

Consumer 1 -> P0, P1, P4
Consumer 2 -> P2, P3, P5
```

Sirf `P4` aur `P5` move hue.

```text
P0, P1, P2, P3 continue processing
```

Cooperative sticky rebalancing incremental hota hai aur unaffected partitions ko consumers ke paas rehne deta hai. [530][534]

---

## 18. Sticky Assignor

Sticky Assignor do goals rakhta hai:

```text
1. Balanced assignment
2. Existing assignment preserve karna
```

Example:

```text
Before:

Consumer 1 -> P0, P1
Consumer 2 -> P2, P3

Consumer 2 down

After:

Consumer 1 -> P0, P1, P2, P3
```

Sticky assignor balanced assignment banata hai aur rebalance ke dauran existing partition assignments ko preserve karne ki koshish karta hai. [522]

---

## 19. Cooperative Sticky Assignor

Ye production me commonly preferred strategy hai.

```yaml
spring:
  kafka:
    consumer:
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Benefits

```text
1. Kam disruption
2. Incremental rebalancing
3. Unaffected partitions processing continue karte hain
4. Better latency stability
```

### Migration Warning

Eager strategy se cooperative-sticky par direct switch karna rolling upgrade issues de sakta hai; migration ke liye proper rolling upgrade path follow karna hota hai. [528][529]

---

## 20. Important Consumer Timeout Settings

| Setting | Meaning |
|---|---|
| `session.timeout.ms` | Broker ko consumer alive maanne ka timeout |
| `heartbeat.interval.ms` | Consumer heartbeat frequency |
| `max.poll.interval.ms` | Do consecutive polls ke beech maximum allowed time |
| `max.poll.records` | Ek poll me maximum records |

### Example Configuration

```yaml
spring:
  kafka:
    consumer:
      properties:
        session.timeout.ms: 45000
        heartbeat.interval.ms: 3000
        max.poll.interval.ms: 300000
        max.poll.records: 100
```

### Rule

```text
heartbeat.interval.ms < session.timeout.ms
```

Example:

```text
heartbeat.interval.ms = 3000
session.timeout.ms = 45000
```

---

## 21. Rebalancing Storm

Rebalancing storm tab hota hai jab consumer group baar-baar rebalance karta hai.

### Causes

```text
1. Consumer repeatedly crashing
2. Kubernetes pod restarts
3. Long processing time
4. `max.poll.interval.ms` exceed
5. GC pauses
6. Network instability
7. Frequent deployments
```

### Symptoms

```text
Consumer lag spike
Processing pause
Duplicate events
High rebalance count
API latency increase
```

### Fix

```text
1. Consumer crash root cause fix karo
2. Processing time reduce karo
3. `max.poll.interval.ms` tune karo
4. `max.poll.records` reduce karo
5. Graceful shutdown add karo
6. Cooperative sticky assignor use karo
```

---

## 22. Offset Reset Commands

### Describe Consumer Group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group
```

### Reset to Earliest

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events \
  --reset-offsets \
  --to-earliest \
  --execute
```

### Reset to Latest

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events \
  --reset-offsets \
  --to-latest \
  --execute
```

> Offset reset production me carefully karo — ye replay ya data skip dono cause kar sakta hai.

---

## 23. Production Checklist

### Offset Management

- [ ] Critical consumers me manual offset commit hai.
- [ ] `enable-auto-commit=false` set hai.
- [ ] `ack-mode=manual_immediate` configured hai.
- [ ] Idempotent consumer implemented hai.
- [ ] Offset reset process documented hai.
- [ ] Consumer lag monitoring hai.

### Rebalancing

- [ ] Cooperative sticky assignor use ho raha hai.
- [ ] Graceful shutdown implemented hai.
- [ ] `session.timeout.ms` tuned hai.
- [ ] `heartbeat.interval.ms` tuned hai.
- [ ] `max.poll.interval.ms` tuned hai.
- [ ] Rebalance metrics monitored hain.
- [ ] Deployment strategy rebalancing-aware hai.

---

## 24. Common Mistakes

### Mistake 1 — Auto Commit for Payments

```text
Wrong:
Payment event auto commit

Correct:
Manual commit after successful payment processing
```

### Mistake 2 — No Idempotency

```text
Wrong:
Assume event exactly once

Correct:
Handle duplicates with eventId
```

### Mistake 3 — Long Processing in Listener

```text
Wrong:
10-minute processing inside poll loop

Correct:
Async processing, batching ya timeout tuning
```

### Mistake 4 — Frequent Rebalances Ignore Karna

```text
Wrong:
Rebalance metrics nahi hain

Correct:
Rebalance count, duration aur lag monitor karo
```

### Mistake 5 — Direct Eager to Cooperative Migration

```text
Wrong:
Live group me direct strategy switch

Correct:
Documented rolling migration path follow karo
```

---

## 25. Interview Questions

### Basic

| Question | Answer |
|---|---|
| Offset kya hai? | Partition me message ki position. |
| Committed offset kya hai? | Consumer dwara Kafka ko bataya gaya processed position. |
| `__consumer_offsets` kya hai? | Kafka ka internal topic jisme consumer group offsets store hote hain. |
| Auto commit kya hai? | Consumer dwara periodic offset commit. |
| Manual commit kya hai? | Consumer dwara explicit offset commit. |
| Rebalancing kya hai? | Consumer group me partitions ka reassignment. |

### Intermediate

| Question | Answer |
|---|---|
| `commitSync` vs `commitAsync`? | `commitSync` blocking aur reliable hai; `commitAsync` non-blocking hai. |
| At-least-once kaise achieve hota hai? | Process ke baad offset commit karke. |
| Duplicate event kaise handle karein? | Idempotent consumer aur processed-event tracking se. |
| Cooperative rebalancing kya hai? | Incremental rebalancing jisme sirf affected partitions move hote hain. |
| Sticky assignor kya karta hai? | Balanced assignment banata hai aur existing assignments preserve karta hai. |
| Rebalancing storm kya hai? | Consumer group ka repeated rebalancing. |

### Scenario-Based

| Scenario | Solution |
|---|---|
| Payment event duplicate process ho raha hai | Idempotent consumer lagao |
| Consumer restart par events skip ho rahe hain | Offset commit strategy aur `auto.offset.reset` check karo |
| Rebalancing baar-baar ho rahi hai | Consumer crash, timeouts, GC aur deployment pattern check karo |
| Consumer lag badh raha hai | Consumer throughput, downstream latency aur partition parallelism check karo |
| Critical events reliably process karne hain | Manual commit + idempotency + retry + DLQ lagao |

---

## 26. Key Takeaways

```text
1. Offset partition me message ki position hai.

2. Committed offset consumer recovery point hai.

3. Kafka committed offsets
   `__consumer_offsets` me store karta hai.

4. Auto commit simple hai,
   lekin critical flows me risky hai.

5. Manual commit reliability deta hai.

6. `commitSync` blocking hota hai,
   `commitAsync` non-blocking.

7. At-least-once me idempotent consumer zaroori hai.

8. Rebalancing partition ownership change karta hai.

9. Cooperative sticky rebalancing disruption kam karta hai.

10. Consumer lag aur rebalance metrics
    production observability ka core hai.
```

---

## 27. Next Part Preview

**Part 5: Kafka CLI — Topics, Producers, Consumers and Consumer Groups**

Isme hum cover karenge:

```text
- kafka-topics.sh
- kafka-console-producer.sh
- kafka-console-consumer.sh
- kafka-consumer-groups.sh
- kafka-configs.sh
- Topic creation and inspection
- Consumer group lag debugging
- Offset reset commands
- Production debugging workflow
```