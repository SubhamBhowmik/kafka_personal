# Kafka Architecture Overview

```
                  Kafka Cluster
        +-----------------------------+
        |                             |
        |      Controller             |
        |                             |
        +-----------------------------+
           |         |          |
           |         |          |
    +-------------+ +-------------+ +-------------+
    | Broker 1    | | Broker 2    | | Broker 3    |
    |             | |             | |             |
    | Orders-P0   | | Orders-P1   | | Orders-P2   |
    | Leader      | | Leader      | | Leader      |
    | Follower    | | Follower    | | Follower    |
    +-------------+ +-------------+ +-------------+
```

- A **Kafka Cluster** contains multiple **brokers**.
- Each **broker** stores some **partitions**.
- Every partition has **one Leader** and **zero or more Followers**.

---

## 1. Broker

A **broker** is simply a Kafka server. It stores data, receives producer requests, and serves consumers.

**Example:**
```
Broker 1
Broker 2
Broker 3
```

> Think of brokers like workers inside a warehouse. If one worker dies, others continue working.

**Responsibilities:**
- Store messages
- Receive producer requests
- Send data to consumers
- Replicate data
- Participate in leader election

---

## 2. Cluster

A **Kafka cluster** is a collection of brokers.

**Example:**
```
Broker 1
Broker 2
Broker 3
Broker 4
```

**Why multiple brokers?**
- High availability
- Fault tolerance
- Better performance
- Scalability

> Instead of one powerful machine, Kafka uses many inexpensive machines.

---

## 3. Leader

Each partition has exactly **one** leader.

**Example — Orders Topic, Partition 0:**
```
Leader   → Broker 1
Followers → Broker 2, Broker 3
```

- All **reads and writes** happen through the leader.
- Producers never write directly to followers.
- Consumers also read from the leader (unless follower reads are enabled).

**Flow:**
```
Producer
    |
    V
  Leader
    |
    V
Followers
```

> Leader is the boss of that partition.

---

## 4. Follower

Followers **copy data** from the leader.

**Example:**
```
Leader                    Followers (copy)
Message A  ──────────►   Message A
Message B  ──────────►   Message B
Message C  ──────────►   Message C
```

- Followers **do not** accept producer writes.
- Their purpose is **backup**.
- If the leader crashes, a follower becomes the new leader → no data loss (if replication succeeded).

---

## 5. ISR (In-Sync Replicas)

Not every follower is considered healthy. Kafka keeps a list called **ISR**.

**Example:**
```
Leader
  ├── Follower 1 ✓
  ├── Follower 2 ✓
  └── Follower 3 (lagging)

ISR = { Leader, Follower1, Follower2 }
```

Follower3 is **excluded** because it is too slow. Kafka only elects a new leader from the ISR — otherwise data loss could occur.

**Interview question:** *Can an out-of-sync replica become leader?*
- Normally **No**. Only ISR replicas are eligible.

---

## 6. Controller

One broker acts as the **Controller**.

**Example:**
```
Broker 1
Broker 2  ← Controller
Broker 3
```

The Controller manages **cluster metadata**.

**Responsibilities:**
- Detect broker failure
- Leader election
- Partition assignment
- Metadata updates

> Think of the Controller as the manager.

**Failure Scenario:**
```
Broker1 (Leader) — crashes
        │
        ▼
Controller notices
        │
        ▼
Controller chooses Broker2
        │
        ▼
Clients receive updated metadata
```

---

## 7. Metadata

**Metadata** means information about the cluster — not the actual messages.

**Metadata contains:**
- Brokers
- Topics
- Partitions
- Leaders
- ISR
- Replication factor

**Example — Orders Topic:**
```
Partition 0 → Broker 1
Partition 1 → Broker 2
Partition 2 → Broker 3
```

Producers fetch metadata before sending data; otherwise they wouldn't know where the leader is.

---

## 8. High Watermark

This is an **interview favorite**.

```
Leader                     Follower
┌───┐                      ┌───┐
│ A │                      │ A │
│ B │                      │ B │
│ C │                      │ C │
│ D │ ⟋ Has follower copied D? → No.
│ E │
└───┘                      └───┘
         ^
    High Watermark
```

Only data replicated to **all ISR replicas** is exposed. Consumers read only until the **High Watermark**.

This prevents reading data that could disappear if the leader crashes.

**Interview question:** *Why can't consumers read the latest message immediately?*
- Because Kafka exposes only **committed messages** (up to the High Watermark).

---

## 9. Log Storage

Kafka stores everything as **logs**. Every partition is simply an **append-only log**.

**Example:**
```
Offset
 0 → Order1
 1 → Order2
 2 → Order3
 3 → Order4
```

- New messages are always **appended**.
- Kafka never inserts in the middle.

**Advantages:**
- Fast writes
- Sequential disk access
- Easy replication

---

## 10. Segment Files

Kafka doesn't store one huge log file. Instead, each partition is split into **segments**.

```
Partition
  ├── segment-0.log
  ├── segment-1.log
  ├── segment-2.log
  └── segment-3.log
```

Each segment has a fixed size (e.g., 1 GB). When full, a new segment is created.

**Advantages:**
- Easy deletion
- Easy retention
- Faster indexing
- Better recovery

Each segment includes:
- `.log`
- `.index`
- `.timeindex`

---

## 11. ZooKeeper (Old Architecture)

Older Kafka versions required **ZooKeeper**.

**ZooKeeper stored:**
- Broker registration
- Controller information
- Topic metadata
- Leader election
- Cluster coordination

**Old Architecture:**
```
Producer → Broker → ZooKeeper
```

**Problems:**
- Extra system
- Operational complexity
- Slower
- Separate cluster to maintain

---

## 12. KRaft (New Architecture)

Modern Kafka replaces ZooKeeper with **KRaft** (Kafka Raft Metadata mode). Kafka manages metadata itself.

**New Architecture:**
```
Producer → Broker → KRaft Controller
```

**Benefits:**
- No ZooKeeper
- Faster startup
- Easier deployment
- Simpler management
- Better scalability

> From **Kafka 4.0** onward, ZooKeeper mode has been removed, so KRaft is the standard architecture.

---

## 13. Leader Election

Suppose **Broker1** (Leader) suddenly crashes.

**Before:**
```
Broker1 → Leader
Broker2 → Follower
Broker3 → Follower
```

**After:**
```
Broker2 → Leader
Broker3 → Follower
```

**Who decides?** The Controller.
**Who can become leader?** Only replicas inside ISR.

**Steps:**
```
Leader crashes
        ↓
Controller detects failure
        ↓
Choose ISR replica
        ↓
New leader selected
        ↓
Metadata updated
        ↓
Producers receive new metadata
        ↓
Producers continue writing
```

> Usually this completes within milliseconds.

---

## Complete Flow

```
Producer
    |
    V
Gets Metadata
    |
    V
Finds Leader
    |
    V
Leader writes message
    |
    ├───────────┬───────────┐
    │           │           │
    V           V           V
Follower 1  Follower 2  Follower 3
    │           │           │
    └───────┬───┘───────────┘
            │
            V
   All ISR replicated
            │
            V
  High Watermark moves
            │
            V
  Consumers can read
```

---

## Frequently Asked Interview Questions

| Question | Answer |
|----------|--------|
| Why does Kafka use partitions? | To enable parallelism, scalability, and higher throughput. |
| Can there be two leaders for the same partition? | No. A partition always has exactly one leader at a time. |
| Why are followers needed? | For replication, fault tolerance, and high availability. |
| What is ISR? | The set of replicas that are fully caught up with the leader and eligible for leader election. |
| What is the High Watermark? | The highest offset that has been replicated to all ISR replicas. Consumers only read up to this point. |
| Why are segment files used? | To simplify retention, deletion, indexing, and recovery instead of maintaining one huge log file. |
| What is the difference between ZooKeeper and KRaft? | ZooKeeper is an external coordination service used by older Kafka versions, while KRaft integrates metadata management and controller functionality directly into Kafka, eliminating the ZooKeeper dependency. |
| What happens if the leader crashes? | The controller detects the failure, elects a new leader from the ISR, updates metadata, and producers/consumers continue using the new leader. |

---

> These concepts form the foundation of Kafka architecture and are among the most frequently tested topics in backend and distributed systems interviews.