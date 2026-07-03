# Kafka Core Concepts — Complete Notes

---

## 1. Kafka Cluster

A **Kafka Cluster** is a collection of one or more Kafka Brokers working together.

```
        Kafka Cluster
  +-----------+-----------+-----------+
  | Broker 1  | Broker 2  | Broker 3  |
  +-----------+-----------+-----------+
```

### Why multiple brokers?
- **Scalability** — handle more data by adding brokers
- **High Availability** — survive broker failures
- **Fault Tolerance** — replicate data across brokers
- **Load Balancing** — distribute partitions evenly

---

## 2. Kafka Broker

A **Broker** is simply a Kafka Server.

### Responsibilities
- Receive messages from Producers
- Store messages on disk
- Serve messages to Consumers
- Replicate data
- Participate in leader election
- Manage partitions

> Think of a Broker as a **warehouse** that stores Kafka data.

---

## 3. What exists inside a Broker?

```
Broker
│
├── Topics
├── Partitions
├── Replicas
├── Log Files
├── Log Segments
├── Records (Messages)
├── Request Handler Threads
├── Replica Fetcher Threads
└── Controller Role (if elected)
```

### Example

```
Broker 1
  Orders-P0   (Leader)
  Orders-P1   (Follower)
  Payments-P0 (Leader)
  Users-P2    (Follower)
```

A broker can contain:
- Multiple Topics
- Multiple Partitions
- Multiple Leaders
- Multiple Followers

---

## 4. Topic

A **Topic** is a logical category of messages.

```
Orders
Payments
Notifications
```

---

## 5. Partition

A Topic is divided into **Partitions**.

```
Orders Topic
  P0
  P1
  P2
```

### Why Partitions?
- Parallel processing
- Scalability
- Better throughput

---

## 6. Can one Broker contain multiple Partitions?

**YES.**

```
Broker 1
  Orders-P0
  Orders-P3
  Payments-P2
  Users-P5
```

A broker may contain **hundreds or thousands** of partitions.

---

## 7. Replica

A **Replica** is simply a copy of a Partition.

```
Replication Factor = 3

Orders-P0
  Broker1
  Broker2
  Broker3
```

There are **three copies** of the same partition.

### Replica Types

Every replica has a role:

```
Replica
├── Leader Replica
└── Follower Replica
```

> **Leader** and **Follower** are roles. **Replica** means copy.

---

## 8. Leader Replica

The **Leader** handles:
- **Producer Writes**
- **Consumer Reads**

```
Producer
  ↓
Leader
  ↓
Followers
```

---

## 9. Follower Replica

The **Follower**:
- Copies data from the Leader
- Cannot accept Producer writes
- Normally does not serve Consumer reads
- Becomes Leader if elected

---

## 10. One Logical Partition vs Multiple Copies

Suppose:
- Topic = `Orders`
- Partition = `P0`
- Replication Factor = `2`

```
Broker1: Orders-P0 (Leader)
Broker2: Orders-P0 (Follower)
```

> Many beginners think there are **two Partition 0s**. **Wrong.**

There is only **ONE logical partition**. There are **TWO physical copies**.

```
Orders-P0
        |
---------------------------
|                          |
Broker1                 Broker2
Leader Replica          Follower Replica
```

---

## 11. Why Replicas?

### Without replication

```
Broker1
  Orders-P0

Broker crashes → Everything unavailable.
```

### With replication

```
Broker1: Leader
Broker2: Follower
Broker3: Follower

Leader crashes → Follower becomes new Leader.
```

No data loss (assuming an up-to-date replica is promoted).

---

## 12. Who chooses the new Leader?

The **Kafka Controller**.

### Controller responsibilities:
- Detects broker failure
- Chooses new Leader from **ISR**
- Updates metadata
- Notifies brokers

---

## 13. Why only ISR?

Suppose:

```
Leader     Offset = 100
Follower A Offset = 100
Follower B Offset = 90
```

Follower B is **behind**. If Kafka chooses Follower B, offsets **91–100** are lost.

Therefore Kafka chooses the new Leader from the **In-Sync Replicas (ISR)** by default.

---

## 14. Message Ordering

Kafka guarantees ordering **only within a Partition**.

```
P0
  Offset0 → A
  Offset1 → B
  Offset2 → C

Consumer reads: A → B → C (Always.)
```

### Across multiple Partitions

```
P0: A, C, E
P1: B, D, F

Consumer may receive: A, B, D, C, E, F
```

> **Global ordering is NOT guaranteed.**

---

## 15. Why?

Each partition has its own **independent Offset sequence**.

```
P0: 0, 1, 2
P1: 0, 1, 2
```

Offsets are **independent** across partitions.

---

## 16. Consumer

A **Consumer** reads messages from Brokers and keeps track of Offsets.

```
Consumer → Broker → Partition → Offset
```

---

## 17. Consumer Group

Multiple consumers can work together in a **Consumer Group**.

```
Consumer Group
  Consumer A
  Consumer B
  Consumer C
```

Kafka distributes partitions among them.

---

## 18. Why Partitions improve Performance

### Only one Partition

```
Consumer: 101 → 102 → 103 → 104
```

Everything is **sequential**.

### Three Partitions

```
P0: 101, 104
P1: 102, 105
P2: 103, 106

Consumer A → P0
Consumer B → P1
Consumer C → P2
```

Now processing happens **simultaneously**:

```
Consumer A: 101 → 104
Consumer B: 102 → 105
Consumer C: 103 → 106
```

> **Parallel Processing.**

---

## 19. One Consumer can read Multiple Partitions

```
Consumer A
  P0
  P2
  P5
```

**Allowed.** The consumer maintains a **separate Offset** for each partition.

```
Consumer A
  P0 Offset = 120
  P2 Offset = 40
  P5 Offset = 300
```

---

## 20. One Partition cannot have Multiple Consumers (Same Consumer Group)

### Correct

```
P0 → Consumer A
```

### Wrong

```
P0 → Consumer A → Consumer B
```

### Reason

Both consumers would process the same message (e.g., `Order101`), resulting in **duplicate processing**. Kafka prevents this.

---

## 21. Relationship

| Relationship | Allowed? |
|--------------|----------|
| One Consumer → Many Partitions | ✅ Allowed |
| One Partition → Many Consumers (same group) | ❌ Not Allowed |

---

## 22. Assignment Example

```
Topic: P0, P1, P2, P3

Consumers: Consumer A, Consumer B

Kafka assigns:
  Consumer A → P0, P2
  Consumer B → P1, P3
```

> Every partition belongs to **exactly one consumer** within a consumer group.

---

## 23. Why can one Consumer read multiple Partitions?

Because the Consumer maintains **independent offsets** for each partition.

```
Consumer A
  P0 Offset = 100
  P2 Offset = 50

Consumer polls:
  Read P0 → Read P2 → Read P0 → Read P2
```

Ordering **inside each partition** is preserved.

---

## 24. Easy Formula

```
Topic
  ↓
Partitions
  ↓
Replicas
  ↓
Leader / Follower

Leader   → Handles Reads & Writes
Follower → Replicates Data

Broker         → Stores many Partition Replicas
Consumer       → Reads one or many Partitions
Consumer Group → One Partition → Exactly One Consumer
```

---

## 25. Interview Revision — Most Important Points

| Concept | Definition |
|---------|------------|
| **Broker** | Kafka server that stores partition replicas and serves producers/consumers. |
| **Topic** | Logical category of messages. |
| **Partition** | Unit of scalability and ordering. |
| **Replica** | Copy of a partition. |
| **Leader Replica** | Handles all reads and writes. |
| **Follower Replica** | Replicates data from the leader; can become leader after failover. |
| **Replication Factor** | Total number of copies of a partition (leader + followers). |
| **ISR (In-Sync Replicas)** | Replicas fully caught up with the leader; eligible for leader election. |
| **Controller** | Detects broker failures and performs leader election. |
| **Consumer** | Reads messages from assigned partitions while tracking offsets. |
| **Consumer Group** | Enables parallel processing by distributing partitions among consumers. |
| **Ordering Guarantee** | Preserved within a partition, not across multiple partitions. |
| **Partition Assignment Rules** | ✅ One consumer can own multiple partitions. ❌ One partition cannot be assigned to multiple consumers within the same consumer group. |