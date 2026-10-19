# Part 5: Kafka CLI — Topics, Producers, Consumers, Consumer Groups and Debugging

> Kafka CLI Kafka cluster ko inspect, debug aur operate karne ka fastest tool hai. Isse hum topics create karte hain, messages test karte hain, consumer lag check karte hain, topic configuration change karte hain aur incident ke time root cause identify karte hain.

---

## 1. Kafka CLI Tools

| Tool | Purpose |
|---|---|
| `kafka-topics.sh` | Topics create, list, describe, delete aur alter karna |
| `kafka-console-producer.sh` | Terminal se Kafka me messages produce karna |
| `kafka-console-consumer.sh` | Kafka se messages consume karke terminal me dekhna |
| `kafka-consumer-groups.sh` | Consumer groups, offsets aur lag inspect/reset karna |
| `kafka-configs.sh` | Topic/broker/user configuration manage karna |
| `kafka-dump-log.sh` | Kafka log segment files inspect karna |
| `kafka-get-offsets.sh` | Topic partitions ke latest offsets dekhna |

Kafka quickstart me basic workflow same hota hai: topic create karo, console producer se events likho, aur console consumer se unhe read karo. [537]

---

## 2. Basic Setup

### Kafka Container Me Enter Karna

```bash
docker exec -it kafka bash
```

### Kafka Bin Directory

```bash
cd /opt/kafka/bin
```

### Bootstrap Server

Local development me:

```text
localhost:9092
```

Docker network me:

```text
kafka:29092
```

---

## 3. Topic Commands

### 3.1 List All Topics

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### 3.2 Create a Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1
```

Production example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic order.order.created.v1 \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000
```

### 3.3 Describe a Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order-events
```

Output me dekho:

```text
Topic: order-events
PartitionCount: 3
ReplicationFactor: 1

Topic: order-events  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
Topic: order-events  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
Topic: order-events  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

Important fields:

| Field | Meaning |
|---|---|
| PartitionCount | Total partitions |
| ReplicationFactor | Har partition ki copies |
| Leader | Current leader broker |
| Replicas | Assigned replica brokers |
| Isr | In-sync replicas |

### 3.4 Delete a Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic order-events
```

> Production me delete command restricted hona chahiye.

### 3.5 Increase Partitions

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic order-events \
  --partitions 6
```

> Partition count increase ho sakta hai, lekin decrease nahi hota.

---

## 4. Console Producer

### Basic Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events
```

Ab terminal me message type karo:

```text
Hello Kafka
```

### Key and Value Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --property parse.key=true \
  --property key.separator=:
```

Input format:

```text
order_8721:{"orderId":"order_8721","amount":2499}
```

### JSON Event Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic order.order.created.v1 \
  --property parse.key=true \
  --property key.separator=:
```

Input:

```text
order_8721:{"eventId":"evt_9921","eventType":"ORDER_CREATED","orderId":"order_8721","userId":"user_101","totalAmount":2499.00}
```

---

## 5. Console Consumer

### Read From Beginning

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning
```

### Read Only New Messages

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events
```

### Read With Key and Value

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" -> "
```

### Read Specific Number of Messages

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning \
  --max-messages 10
```

Console consumer Kafka topic se records read karke standard output par print karta hai, aur testing/debugging ke liye useful hota hai. [547]

---

## 6. Consumer Group Commands

### 6.1 List Consumer Groups

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### 6.2 Describe Consumer Group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group
```

Sample output:

```text
GROUP                        TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
notification-service-group   order-events     0          940             1000            60
notification-service-group   order-events     1          930             1000            70
notification-service-group   order-events     2          980             1000            20
```

### Important Columns

| Column | Meaning |
|---|---|
| `CURRENT-OFFSET` | Consumer ka committed offset |
| `LOG-END-OFFSET` | Partition ka latest produced offset |
| `LAG` | Consumer kitna peeche hai |

Formula:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

`kafka-consumer-groups.sh --describe` consumer group ki position aur log end se kitna peeche hai, dono dikhata hai. [538][540]

---

## 7. Consumer Lag Debugging

### Step 1 — Identify Lag

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group
```

### Step 2 — Check Consumer Members

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group \
  --members
```

### Step 3 — Check Partition Assignment

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-service-group \
  --state
```

### Step 4 — Check Application Logs

```text
1. Consumer alive hai?
2. Exceptions hain?
3. Downstream DB/API slow hai?
4. Rebalancing ho rahi hai?
5. Processing thread stuck hai?
```

---

## 8. Offset Reset Commands

> **Warning:** Offset reset production me carefully karo. Ye replay ya message skip dono cause kar sakta hai.

### Reset to Earliest

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

Actual execute:

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

### Reset to Specific Offset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events:0 \
  --reset-offsets \
  --to-offset 1000 \
  --execute
```

### Shift Offset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events \
  --reset-offsets \
  --shift-by -10 \
  --execute
```

Matlab:

```text
Current offset se 10 messages peeche
```

### Reset to Timestamp

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic order-events \
  --reset-offsets \
  --to-datetime 2026-10-19T10:00:00.000 \
  --execute
```

Offset reset options me `--to-earliest`, `--to-latest`, `--to-offset`, `--shift-by` aur `--to-datetime` included hain. [546][548]

---

## 9. Reset Offset Best Practices

```text
1. Pehle --dry-run chalao
2. Consumer group ko stop karo
3. Impact document karo
4. Backup/audit log rakho
5. Idempotent consumer ensure karo
6. Off-peak time me karo
7. Reset ke baad lag monitor karo
```

### Important Rule

```text
Running consumer group ke offsets
directly reset karne se error aa sakta hai.
```

Isliye:

```text
1. Consumer stop karo
2. Offset reset karo
3. Consumer restart karo
```

---

## 10. Kafka Topic Configuration Commands

### Describe Topic Config

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name order-events \
  --describe
```

### Change Retention

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name order-events \
  --alter \
  --add-config retention.ms=604800000
```

Matlab:

```text
7 days retention
```

### Change Max Message Size

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name order-events \
  --alter \
  --add-config max.message.bytes=128000
```

### Remove Config Override

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name order-events \
  --alter \
  --delete-config retention.ms
```

`kafka-configs.sh` se topic configurations ko view, alter aur delete kiya ja sakta hai; `max.message.bytes` jaise overrides bhi isse set hote hain. [536][543][545]

---

## 11. Security-Enabled Kafka CLI

Agar Kafka SASL/SSL enabled hai, to `--command-config` use karo.

### Client Config File

`kafka-client.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="kafka-user" \
  password="your-password";
```

### List Topics

```bash
kafka-topics.sh \
  --bootstrap-server kafka.example.com:9093 \
  --command-config kafka-client.properties \
  --list
```

### Produce Message

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka.example.com:9093 \
  --command-config kafka-client.properties \
  --topic order-events
```

### Describe Consumer Group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka.example.com:9093 \
  --command-config kafka-client.properties \
  --describe \
  --group notification-service-group
```

---

## 12. Debugging Workflow

### Scenario: Consumer Lag High

```text
1. Consumer group describe karo
2. Lag identify karo
3. Consumer members check karo
4. Partition assignment check karo
5. Application logs check karo
6. Downstream DB/API latency check karo
7. Consumer CPU/memory check karo
8. Rebalancing metrics check karo
9. Root cause fix karo
10. Lag recovery monitor karo
```

### Scenario: Message Kafka Me Nahi Dikh Raha

```text
1. Topic exist karta hai?
2. Producer correct topic par bhej raha hai?
3. Producer publish success hua?
4. Topic partitions check karo
5. Console consumer se message verify karo
6. Serializer/deserializer mismatch check karo
7. Security/ACL check karo
```

### Scenario: Consumer Message Process Nahi Kar Raha

```text
1. Consumer group active hai?
2. Consumer pod/container running hai?
3. Group me consumer assigned hai?
4. Deserialization error hai?
5. Processing exception hai?
6. DLQ me messages hain?
7. Rebalancing loop hai?
```

---

## 13. Useful One-Line Commands

### Count of Topics

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list | wc -l
```

### Check Specific Topic Exists

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list | grep order-events
```

### Check All Consumer Groups

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Check Lag for All Groups

```bash
for group in $(kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list); do
  kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group "$group"
done
```

---

## 14. Production Safety Rules

```text
1. Production Kafka CLI access restricted rakho.

2. Delete aur reset commands audit log me rakho.

3. Offset reset se pehle dry-run karo.

4. Topic deletion default deny karo.

5. Auto topic creation disable rakho.

6. SASL/SSL enabled clusters me command-config use karo.

7. CLI commands ko runbook me document karo.

8. Production changes ke liye approval process rakho.
```

---

## 15. Common Mistakes

### Mistake 1 — Wrong Bootstrap Server

```text
Local:
localhost:9092

Docker network:
kafka:29092
```

### Mistake 2 — Running Consumer Ke Offset Reset Karna

```text
Wrong:
Consumer running + reset offsets

Correct:
Consumer stop -> reset -> restart
```

### Mistake 3 — `--dry-run` Skip Karna

```text
Wrong:
Direct --execute

Correct:
Pehle --dry-run, phir --execute
```

### Mistake 4 — Topic Auto-Create Par Depend Karna

```text
Wrong:
Producer se topic automatically banwana

Correct:
Explicit topic creation with partitions,
replication and retention
```

### Mistake 5 — Security Config Git Me Rakhna

```text
Wrong:
Password plain text me commit

Correct:
Secret manager/env variable use karo
```

---

## 16. Interview Questions

| Question | Answer |
|---|---|
| Kafka CLI kya hai? | Kafka cluster ko inspect, debug aur operate karne ke command-line tools. |
| Topic kaise create karte hain? | `kafka-topics.sh --create` command se. |
| Consumer lag kaise check karte hain? | `kafka-consumer-groups.sh --describe --group <group>` se. |
| Offset reset kaise karte hain? | `kafka-consumer-groups.sh --reset-offsets` se. |
| `--dry-run` kya karta hai? | Actual change kiye bina reset ka preview dikhata hai. |
| Topic config kaise change karte hain? | `kafka-configs.sh --alter` se. |

---

## 17. Key Takeaways

```text
1. Kafka CLI debugging aur operations ka core tool hai.

2. `kafka-topics.sh` topics manage karta hai.

3. `kafka-console-producer.sh` testing ke liye messages produce karta hai.

4. `kafka-console-consumer.sh` messages inspect karta hai.

5. `kafka-consumer-groups.sh` lag aur offsets debug karta hai.

6. `kafka-configs.sh` topic configurations manage karta hai.

7. Offset reset se pehle dry-run aur consumer stop karo.

8. Production CLI access restricted aur audited hona chahiye.

9. Lag debugging me group, members,
   partition assignment aur downstream health check karo.

10. CLI commands ko runbook me document karo.
```

---

## 18. Next Part Preview

**Part 6: Kafka vs Redis Streams vs RabbitMQ**

Isme hum compare karenge:

```text
- Architecture
- Delivery guarantees
- Ordering
- Scalability
- Persistence
- Replay
- Consumer model
- Best use cases
- Spring Boot integration
- Real-world decision framework
```