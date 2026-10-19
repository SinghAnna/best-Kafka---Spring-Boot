# Part 2: Kafka Architecture — Broker, Topic, Partition, Replica, Leader, ISR and KRaft

> Kafka ek distributed event-streaming system hai jiska core brokers, replicated partitions, consumer groups aur metadata management par bana hai. Is part me hum Kafka ke andar data kaise store hota hai, replication kaise hoti hai, leader election kaise hoti hai aur KRaft mode kaise kaam karta hai — sab deeply samjhenge.

---

## 1. Kafka Architecture Overview

```text
Producers
    |
    v
Kafka Cluster
    |
    +--> Broker 1
    +--> Broker 2
    +--> Broker 3
    |
    v
Consumer Groups
```

Kafka cluster multiple brokers se banta hai. Producer events Kafka topics me publish karta hai, topics partitions me divide hote hain, aur partitions brokers par replicas ke saath store hote hain. Consumers consumer groups me milke in partitions ko process karte hain. [474][478]

### High-Level Components

| Component | Role |
|---|---|
| Producer | Events publish karta hai |
| Consumer | Events read/process karta hai |
| Consumer Group | Partition ownership share karta hai |
| Topic | Events ka logical category |
| Partition | Append-only ordered log |
| Broker | Kafka server |
| Replica | Partition ki copy |
| Controller | Cluster metadata manage karta hai |
| KRaft | Kafka ka internal Raft-based metadata system |

---

## 2. Kafka Broker

Broker ek Kafka server hai.

Broker ke responsibilities:

```text
1. Events store karna
2. Producer requests handle karna
3. Consumer fetch requests handle karna
4. Partition replicas maintain karna
5. Replication participate karna
6. Consumer group coordination me help karna
7. Disk, network aur memory resources manage karna
```

### Kafka Cluster Example

```text
Kafka Cluster
   |
   +--> Broker 1
   +--> Broker 2
   +--> Broker 3
```

Production me usually kam se kam 3 brokers rakhe jate hain, kyunki replication factor 3 aur fault tolerance ke liye ye common baseline hai.

---

## 3. Kafka Topic

Topic events ka logical category hai.

Examples:

```text
order-events
payment-events
user-events
notification-events
clickstream-events
```

Topic khud data store nahi karta — actual data uske partitions me store hota hai.

```text
Topic: order-events
        |
        +--> Partition 0
        +--> Partition 1
        +--> Partition 2
```

### Topic Naming Best Practice

```text
<domain>.<entity>.<event-type>.v<version>
```

Examples:

```text
order.order.created.v1
payment.payment.completed.v1
user.user.updated.v1
cache.user.profile.invalidated.v1
```

---

## 4. Kafka Partition

Partition topic ka physical, append-only log hota hai.

```text
Topic: order-events

Partition 0:
Offset 0 -> Event A
Offset 1 -> Event B
Offset 2 -> Event C

Partition 1:
Offset 0 -> Event D
Offset 1 -> Event E

Partition 2:
Offset 0 -> Event F
```

### Partition Properties

```text
1. Events append-only hote hain
2. Har event ka offset hota hai
3. Ek partition ke andar order guaranteed hota hai
4. Partitions ke beech global order guaranteed nahi hota
5. Partition parallelism enable karta hai
6. Partition consumer scaling ka unit hai
```

### Why Partitions Matter

```text
1 topic with 1 partition
= limited parallelism

1 topic with 12 partitions
= up to 12 consumers can process in parallel
```

### Partition Count

Partition count decide karte waqt consider karo:

```text
Expected throughput
Consumer parallelism requirement
Key distribution
Broker capacity
Rebalancing impact
Future growth
```

---

## 5. Kafka Offset

Offset partition ke andar message ki unique position hai.

```text
Partition 0

Offset 0 -> ORDER_CREATED
Offset 1 -> PAYMENT_COMPLETED
Offset 2 -> ORDER_SHIPPED
```

Consumer offset commit karta hai, jisse restart ke baad wo wahi se processing resume kar sakta hai. [458]

### Offset Commit Example

```text
Consumer processes:
Offset 0 -> Success
Offset 1 -> Success
Offset 2 -> Success

Commits offset 2
```

Agar consumer crash ho jaye:

```text
Restart -> Read from committed offset
```

---

## 6. Partition Replication

Replication ka matlab hai partition data ki multiple copies rakhna.

```text
Topic: order-events
Partition 0

Broker 1 -> Leader Replica
Broker 2 -> Follower Replica
Broker 3 -> Follower Replica
```

### Replication Factor

```text
replication-factor = 3
```

Matlab:

```text
Partition 0 ki 3 copies hain
```

### Why Replication?

```text
Broker 1 down ho jaye
-> Broker 2 ya Broker 3 leader ban sakta hai
-> Data available rehta hai
```

### Topic Creation with Replication

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic order-events \
  --partitions 6 \
  --replication-factor 3
```

---

## 7. Leader and Follower Replica

Har partition ka ek **leader replica** hota hai.

```text
Partition 0

Broker 1 -> Leader
Broker 2 -> Follower
Broker 3 -> Follower
```

### Leader Responsibilities

```text
1. Producer writes handle karna
2. Consumer reads serve karna
3. Follower replication coordinate karna
4. High watermark maintain karna
```

### Follower Responsibilities

```text
1. Leader se data replicate karna
2. Leader ke saath sync rehna
3. Leader fail hone par leader banne ke liye ready rehna
```

---

## 8. ISR — In-Sync Replicas

ISR wo replicas ka set hai jo leader ke saath sufficiently synced hain.

```text
Partition 0

Leader: Broker 1
ISR: Broker 1, Broker 2, Broker 3
```

Agar Broker 3 slow ya unavailable ho jaye:

```text
Leader: Broker 1
ISR: Broker 1, Broker 2
```

### ISR Importance

Kafka dynamically ISR maintain karta hai; ISR ke members hi generally leader election ke eligible candidates hote hain. [490]

---

## 9. `acks` and Durability

Producer `acks` setting decide karta hai ki write successful maane se pehle kitne acknowledgements chahiye.

| `acks` | Meaning | Durability | Performance |
|---|---|---|---|
| `acks=0` | Broker acknowledgement wait nahi | Lowest | Highest |
| `acks=1` | Leader acknowledgement sufficient | Medium | Medium |
| `acks=all` | All in-sync replicas acknowledge | Highest | Lower |

### `acks=all` Example

```text
Producer -> Leader
Leader -> Followers replicate
Followers acknowledge
Leader -> Producer success
```

`acks=all` ke saath `min.insync.replicas` decide karta hai ki write successful hone ke liye kam se kam kitne in-sync replicas chahiye. [490][494]

---

## 10. `min.insync.replicas`

Ye setting define karti hai ki `acks=all` ke saath write accept hone ke liye minimum kitne in-sync replicas chahiye.

```text
replication-factor = 3
min.insync.replicas = 2
```

Matlab:

```text
Leader + at least 1 follower in sync
= write accepted
```

Agar sirf leader available ho:

```text
ISR size = 1
min.insync.replicas = 2

Producer write fails
```

### Why?

Kyunki sirf leader available hone par data loss ka risk badh jata hai.

### Production Recommendation

```text
replication-factor = 3
min.insync.replicas = 2
acks = all
```

---

## 11. High Watermark

High watermark wo highest offset hai jo saare in-sync replicas par successfully replicate ho chuka hai.

```text
Partition 0

Leader has:
Offset 0, 1, 2, 3, 4

Follower 1 has:
Offset 0, 1, 2, 3

Follower 2 has:
Offset 0, 1, 2, 3

High Watermark = Offset 3
```

Consumers high watermark tak ke messages read karte hain.

```text
Consumers can read:
Offset 0 to 3

Offset 4 not yet visible
```

---

## 12. Leader Election

Agar partition leader fail ho jaye, Kafka naya leader choose karta hai.

```text
Before failure:

Broker 1 -> Leader
Broker 2 -> Follower
Broker 3 -> Follower

After Broker 1 failure:

Broker 2 -> New Leader
Broker 3 -> Follower
```

### Leader Election Rules

```text
1. Prefer ISR member
2. ISR empty ho to unclean leader election policy decide karti hai
3. Unclean leader election allow ho to out-of-sync replica leader ban sakta hai
4. Isse availability badhti hai, lekin data loss risk bhi badhta hai
```

Kafka ISR-based leader selection use karta hai, aur strict `min.insync.replicas` rule ke under ISR empty ho to high watermark advance nahi hota. [484][490]

---

## 13. Unclean Leader Election

```text
unclean.leader.election.enable=false
```

Matlab:

```text
Sirf in-sync replica hi leader ban sakta hai
```

Benefit:

```text
Data consistency and durability better
```

Tradeoff:

```text
Agar koi ISR member available nahi,
partition unavailable ho sakta hai
```

```text
unclean.leader.election.enable=true
```

Matlab:

```text
Out-of-sync replica bhi leader ban sakta hai
```

Benefit:

```text
Availability better
```

Risk:

```text
Data loss possible
```

### Production Rule

```text
Critical business data:
unclean.leader.election.enable=false
```

---

## 14. Kafka Metadata

Kafka metadata me ye information hoti hai:

```text
Brokers
Topics
Partitions
Replicas
Partition leaders
ISR state
Topic configurations
ACLs
Consumer group metadata
```

### Example Metadata

```text
Topic: order-events

Partition 0:
Leader: Broker 1
Replicas: Broker 1, Broker 2, Broker 3
ISR: Broker 1, Broker 2

Partition 1:
Leader: Broker 2
Replicas: Broker 2, Broker 3, Broker 1
ISR: Broker 2, Broker 3, Broker 1
```

---

## 15. ZooKeeper-Based Kafka Architecture

Purane Kafka deployments me ZooKeeper cluster metadata manage karta tha.

```text
Kafka Brokers
      |
      v
  ZooKeeper
      |
      +--> Broker registration
      +--> Controller election
      +--> Topic metadata
      +--> ACL metadata
```

### Problems

```text
1. Extra system operate karna
2. Two systems ki monitoring
3. Complex failure handling
4. Scalability limitations
5. Operational overhead
```

---

## 16. KRaft Mode

KRaft ka full form hai **Kafka Raft**.

KRaft Kafka ki ZooKeeper dependency remove karta hai.

```text
Old Architecture:
Kafka + ZooKeeper

New Architecture:
Kafka with built-in KRaft controllers
```

KRaft controller nodes ek Raft quorum banate hain jo Kafka metadata log manage karta hai. Metadata me topics, partitions, ISR, configurations aur broker state hota hai. [486][489]

### KRaft Architecture

```text
Kafka Cluster
   |
   +--> Controller Nodes
   |       |
   |       +--> Active Controller
   |       +--> Follower Controllers
   |
   +--> Broker Nodes
           |
           +--> Broker 1
           +--> Broker 2
           +--> Broker 3
```

### KRaft Metadata Log

KRaft metadata ko special internal topic me store karta hai:

```text
__cluster_metadata
```

Is log me store hota hai:

```text
Broker registrations
Topic creation
Partition creation
Replica assignment
Leader changes
ISR changes
Configuration changes
ACL changes
```

### Active Controller

```text
Controller Nodes
   |
   +--> Active Controller
   +--> Follower Controller 1
   +--> Follower Controller 2
```

Active controller metadata changes handle karta hai, aur follower controllers metadata log replicate karte hain. Active controller fail hone par follower controller active ban sakta hai. [486][487]

---

## 17. KRaft Quorum

KRaft majority-based quorum use karta hai.

```text
3 Controller Nodes
= 1 node failure tolerate kar sakta hai

5 Controller Nodes
= 2 node failures tolerate kar sakta hai
```

Formula:

```text
Minimum controllers required =
(floor(total_controllers / 2) + 1)
```

Examples:

```text
3 controllers -> 2 required
5 controllers -> 3 required
7 controllers -> 4 required
```

KRaft ke liye majority of controller nodes running hona zaroori hai; 3-node controller cluster ek failure survive kar sakta hai. [486]

---

## 18. Combined and Dedicated KRaft Modes

### Combined Mode

Ek server broker aur controller dono ka role play karta hai.

```text
Server 1 -> Broker + Controller
Server 2 -> Broker + Controller
Server 3 -> Broker + Controller
```

Use case:

```text
Small to medium clusters
Simplified deployment
```

### Dedicated Mode

Controller aur broker alag nodes hote hain.

```text
Controller Nodes:
Controller 1
Controller 2
Controller 3

Broker Nodes:
Broker 1
Broker 2
Broker 3
Broker 4
Broker 5
```

Use case:

```text
Large production clusters
Better isolation
Independent scaling
```

---

## 19. Consumer Group and Partition Assignment

Consumer group me partitions consumers me distribute hote hain.

```text
Topic: order-events
Partitions: 6

Consumer Group: notification-service-group
Consumers: 3

Consumer 1 -> Partition 0, Partition 1
Consumer 2 -> Partition 2, Partition 3
Consumer 3 -> Partition 4, Partition 5
```

### Rule

```text
Ek partition ek time par
ek consumer group me
sirf ek consumer process karta hai.
```

### More Consumers Than Partitions

```text
Partitions = 3
Consumers = 5

Consumer 1 -> Partition 0
Consumer 2 -> Partition 1
Consumer 3 -> Partition 2
Consumer 4 -> Idle
Consumer 5 -> Idle
```

### Fewer Consumers Than Partitions

```text
Partitions = 6
Consumers = 2

Consumer 1 -> Partition 0, 1, 2
Consumer 2 -> Partition 3, 4, 5
```

---

## 20. Rebalancing

Rebalancing ka matlab hai partitions ka consumer group members me dobara assign hona.

### When Rebalancing Happens

```text
1. Naya consumer join kare
2. Existing consumer leave kare
3. Consumer crash ho jaye
4. Consumer group subscription change ho
5. Topic partition count change ho
```

### Rebalancing Flow

```text
1. Consumer group join/leave hota hai
2. Group Coordinator ko pata chalta hai
3. Consumer group leader elect hota hai
4. Assignment strategy partitions assign karti hai
5. Naya assignment consumers ko distribute hota hai
6. Consumers processing resume karte hain
```

Rebalancing me Group Coordinator consumers ko coordinate karta hai, ek consumer temporary group leader banta hai, aur wo assignment strategy se partition-to-consumer mapping banata hai. [492]

### Rebalancing Impact

```text
Short processing pause
Possible duplicate processing
Latency spike
Consumer lag increase
```

### Reduce Rebalancing Problems

```text
1. Frequent deployments avoid karo
2. `max.poll.interval.ms` tune karo
3. `session.timeout.ms` tune karo
4. Long processing avoid karo
5. Cooperative sticky assignor consider karo
6. Graceful shutdown implement karo
```

---

## 21. Partition Assignment Strategies

| Strategy | Behavior |
|---|---|
| Range Assignor | Partitions ko ranges me assign karta hai |
| Round Robin Assignor | Partitions ko evenly distribute karta hai |
| Sticky Assignor | Existing assignments preserve karne ki koshish karta hai |
| Cooperative Sticky Assignor | Incremental rebalancing with fewer disruptions |

Production me commonly **Cooperative Sticky Assignor** prefer kiya jata hai, kyunki ye rebalancing disruption kam karta hai.

---

## 22. Kafka Data Storage Model

Kafka partition ko append-only log ki tarah treat karta hai.

```text
Partition Log

Offset 0 -> Event
Offset 1 -> Event
Offset 2 -> Event
Offset 3 -> Event
Offset 4 -> Event
```

### Log Segments

Large partition logs multiple segment files me divide hote hain.

```text
Partition 0
   |
   +--> segment-00000000000000000000.log
   +--> segment-00000000000000000100.log
   +--> segment-00000000000000000200.log
```

### Benefits

```text
1. Fast sequential writes
2. Efficient reads
3. Easy retention deletion
4. Efficient compaction support
5. High throughput
```

---

## 23. Retention Policies

Kafka events ko turant delete nahi karta — retention policy follow karta hai.

### Time-Based Retention

```text
retention.ms=604800000
```

Matlab:

```text
7 days retention
```

### Size-Based Retention

```text
retention.bytes=1073741824
```

Matlab:

```text
1 GB per partition retention
```

### Log Compaction

Log compaction me har key ka latest value retain hota hai.

```text
Before compaction:

key=user:101 value=profile-v1
key=user:101 value=profile-v2
key=user:101 value=profile-v3

After compaction:

key=user:101 value=profile-v3
```

Use case:

```text
User profile updates
Configuration changes
Entity state snapshots
```

---

## 24. Broker Failure Scenario

### Initial State

```text
Topic: order-events
Partition 0

Broker 1 -> Leader
Broker 2 -> Follower
Broker 3 -> Follower
```

### Broker 1 Fails

```text
Broker 1 down
```

### Recovery

```text
1. Controller detects broker failure
2. Partition 0 ka naya leader elect hota hai
3. Broker 2 ya Broker 3 leader ban sakta hai
4. Producers naye leader ko write karte hain
5. Consumers naye leader se read karte hain
```

### Important Point

```text
Replication factor 3
+ min.insync.replicas 2
+ acks all
= Strong durability
```

---

## 25. Production Architecture Example

```text
                    Producers
                        |
        +---------------+----------------+
        |               |                |
Order Service   Payment Service   User Service
        |               |                |
        +-------+-------+--------+-------+
                |
                v
        Kafka Cluster
                |
    +-----------+-----------+
    |           |           |
Broker 1    Broker 2    Broker 3
    |           |           |
    +-----------+-----------+
                |
        Consumer Groups
                |
    +-----------+-----------+
    |           |           |
Inventory   Notification  Analytics
Service     Service       Service
```

### Production Settings

```text
Broker count = 3+
Default replication factor = 3
min.insync.replicas = 2
Producer acks = all
Idempotent producer = true
Auto topic creation = false
Unclean leader election = false
Monitoring = enabled
```

---

## 26. Common Misconceptions

### Misconception 1 — Kafka Message Receive Hote Hi Delete Kar Deta Hai

```text
Wrong:
Consumer message read -> Kafka deletes message

Correct:
Kafka retention policy ke hisaab se message retain karta hai
```

### Misconception 2 — Kafka Global Ordering Deta Hai

```text
Wrong:
Topic ke saare events globally ordered

Correct:
Ordering sirf partition level par guaranteed hai
```

### Misconception 3 — Replication Factor 3 Matlab 3 Different Topics

```text
Wrong:
3 topics

Correct:
Ek hi partition ki 3 copies
```

### Misconception 4 — `acks=all` Matlab Zero Data Loss

```text
Wrong:
Guarantee

Correct:
Strong durability, lekin producer retries,
broker failure, disk failure aur operational
mistakes ko bhi handle karna hota hai
```

### Misconception 5 — KRaft Sirf ZooKeeper Replacement Hai

```text
Incomplete:
KRaft = ZooKeeper replacement

Better:
KRaft Kafka ka built-in Raft-based
metadata and controller architecture hai
```

---

## 27. Interview Questions

### Basic

| Question | Answer |
|---|---|
| Kafka broker kya hai? | Kafka server jo events store karta hai aur producer/consumer requests handle karta hai. |
| Topic kya hai? | Events ka logical category. |
| Partition kya hai? | Topic ka append-only ordered log segment. |
| Offset kya hai? | Partition me message ki position. |
| Replica kya hai? | Partition ki copy. |
| Leader replica kya hai? | Partition ka primary replica jo writes aur reads handle karta hai. |

### Intermediate

| Question | Answer |
|---|---|
| ISR kya hai? | In-Sync Replicas — leader ke saath sufficiently synced replicas ka set. |
| `min.insync.replicas` kya hai? | `acks=all` write ke liye required minimum in-sync replicas. |
| High watermark kya hai? | Highest offset jo saare ISR replicas par replicate ho chuka hai. |
| Leader election kya hai? | Leader fail hone par naye leader choose hone ka process. |
| Rebalancing kya hai? | Consumer group me partitions ka reassignment. |
| KRaft kya hai? | Kafka ka built-in Raft-based metadata and controller system. |

### Scenario-Based

| Scenario | Answer |
|---|---|
| Broker down ho jaye | Replica leader ban jata hai, producers/consumers naye leader par failover karte hain |
| Data loss avoid karna hai | `acks=all`, RF=3, `min.insync.replicas=2`, unclean leader election false |
| Consumer scale karna hai | Partition count aur consumer group capacity check karo |
| Rebalancing latency badha rahi hai | Graceful shutdown, timeouts aur cooperative sticky assignor review karo |
| Kafka metadata manage karna hai | KRaft controller quorum deploy karo |

---

## 28. Key Takeaways

```text
1. Kafka cluster multiple brokers se banta hai.

2. Topic logical category hai,
   partition physical append-only log hai.

3. Har partition ki replicas hoti hain.

4. Har partition ka ek leader hota hai.

5. ISR leader ke saath synced replicas ka set hai.

6. `acks=all` + `min.insync.replicas=2`
   strong durability deta hai.

7. High watermark consumers ko safe read boundary deta hai.

8. KRaft ZooKeeper ki jagah Kafka ke internal
   Raft-based controller system ko use karta hai.

9. Consumer group partitions ko share karta hai.

10. Rebalancing partition ownership change karta hai,
    isliye production me carefully manage karna chahiye.
```

---

## 29. Next Part Preview

**Part 3: Producers, Consumers and Consumer Groups**

Isme hum cover karenge:

```text
- Producer internals
- Kafka message keys
- Partitioner
- Batching and compression
- Consumer polling model
- Consumer group coordination
- Offset commit strategies
- Rebalancing in depth
- Spring Boot producer and consumer basics
```