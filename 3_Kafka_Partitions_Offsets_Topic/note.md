# 2. Topics, Partitions & Offsets

---

## 1. Topics

### What is a Topic?

A **Topic** is a logical category where producers send messages and consumers read messages. Think of it like a database table.

```
Orders Topic
------------
Order1
Order2
Order3
```

- **Producers** write to Topics.
- **Consumers** read from Topics.
- A Topic is divided into **Partitions**.

---

### Topic Creation

Two ways to create topics:

#### 1. Auto Creation (❌ Not Recommended)

If enabled, the flow looks like:

```
Producer
   |
Topic doesn't exist
   |
Kafka creates it automatically
```

**Problems:**
- Wrong partition count
- Wrong replication factor
- Typo creates a new topic unintentionally

> **Production → Usually Disabled.**

#### 2. Manual Creation (✅ Recommended)

**Example:**
```bash
kafka-topics.sh \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

> Always create topics manually in production.

---

### Topic Configuration

Important configurations:

| Config | Meaning |
|---|---|
| `partitions` | Number of partitions |
| `replication.factor` | Number of replicas |
| `retention.ms` | Message lifetime |
| `retention.bytes` | Max storage |
| `cleanup.policy` | `delete` / `compact` |
| `min.insync.replicas` | Minimum ISR required |

---

### Internal Topics

Kafka creates some topics automatically.

**Examples:**
- **`__consumer_offsets`** — Stores consumer group committed offsets
- Kafka Streams also creates internal topics.

---

### Topic Naming

**Good naming convention:**
```
orders
payments
inventory
user-events
customer.notifications
```

**Avoid:**
```
Topic1    ❌
Test      ❌
ABC       ❌
xyz       ❌
```

**Best practices:**
- lowercase
- meaningful
- hyphen (`-`) preferred
- environment prefix if needed

**Example:**
```
prod.orders
dev.orders
```

---

### Topic Retention

**Retention** decides how long Kafka keeps messages.

**Example:**
```
Retention = 7 days

Day 1  ✓
Day 2  ✓
Day 5  ✓
Day 8  ❌ (deleted)
```

**Configured using:**
- `retention.ms`
- `retention.hours`
- `retention.bytes`

> ⚠ **Very Important:** Retention is based on time/storage, **not** whether consumers have read the message.

---

### Topic Compaction

Instead of deleting old records, Kafka keeps the **latest value for each key**.

**Example:**
```
Key   Value
101   Ram
101   Shyam
101   Rahul

After compaction:
101 → Rahul
```

**Useful for:**
- User Profile
- Configuration
- Latest State
- Cache rebuild

---

### Topic Deletion

Delete unused topics:

```bash
kafka-topics.sh --delete --topic orders
```

Only possible if `delete.topic.enable=true`.

---

### ⭐ Interview Questions — Topics

#### Q1: Why should auto topic creation be disabled?

**Answer:**
- Prevent accidental topics due to typos
- Control partitions
- Control replication
- Better production management

#### Q2: Difference between Retention and Compaction?

| Retention | Compaction |
|---|---|
| Deletes old messages | Keeps latest record per key |
| Time/size based | Key based |
| Good for events | Good for latest state |

---

## 2. Partitions

### What is a Partition?

A **Topic** is divided into multiple **Partitions**.

```
Orders Topic
   P0     P1     P2
   │      │      │
[Log]  [Log]  [Log]
```

Each partition is an **append-only log**.

---

### Why Partitions?

**Without partitions:**
```
Producer
   ↓
One partition
   ↓
One consumer
```
→ **Low throughput.**

**With partitions:**
```
P0 → Consumer 1
P1 → Consumer 2
P2 → Consumer 3
```

**Benefits:**
- ✅ Parallel processing
- ✅ Higher throughput
- ✅ Scalability
- ✅ Load balancing

---

### Partition Strategy

The **producer** decides which partition receives the message.

**Methods:**
1. Key based
2. Round Robin
3. Sticky (Default)
4. Custom

---

### Parallelism

Each partition can be consumed independently.

**Example:**
```
6 Partitions
     ↓
6 Consumers
     ↓
6 messages processed simultaneously
```

> More partitions ⇒ More parallelism (up to practical limits).

---

### Ordering

Kafka guarantees ordering **only within a partition**.

**Correct (within a partition):**
```
P0:  1  2  3  4
     └─────────→ Guaranteed order
```

**Across partitions:**
```
P0:  1  3
P1:  2  4
```
→ Order is **not** guaranteed.

> **Interview Rule:** If strict ordering is required, ensure related events go to the same partition (typically by using the same key).

---

### Partition Key

The producer provides a key.

**Example:**
```
CustomerID = 101
Kafka hashes it: hash(101) % partitions
```

> **Same key → Same partition → Ordering preserved.**

---

### Round Robin

When **no key is provided**, Kafka distributes messages one after another.

```
Msg1 → P0
Msg2 → P1
Msg3 → P2
Msg4 → P0
```

- Best for even load.
- No ordering guarantee for related messages.

---

### Sticky Partitioner

(Default behavior for unkeyed records in modern Kafka producers.)

The producer temporarily keeps sending records to one partition, then switches.

**Example:**
```
P0                     P2
Msg1                   Msg4
Msg2        Switch     Msg5
Msg3         ──────→
```

**Benefits:**
- Larger batches
- Better compression
- Higher throughput
- Fewer network requests

---

### Custom Partitioner

You write your own logic.

**Example:**
```
Premium Customer  →  Partition 0
Normal Customer   →  Partition 1
```

> Used when business rules determine partition selection.

---

### Increasing Partitions

Possible anytime.

**Example:**
```
Before:  3 partitions
After:   6 partitions
```

> ⚠ **Warning:** Existing key-to-partition mapping can change. Ordering for future messages with the same key may no longer align with historical data. Increase partitions only when necessary.

---

### Partition Reassignment

Moves partitions between brokers.

**Reasons:**
- Broker added
- Load balancing
- Broker maintenance
- Storage issues

> No application changes required.

---

### ⭐ Interview Questions — Partitions

#### Q1: Why can't one partition be consumed by two consumers in the same consumer group?

**Answer:** Because Kafka guarantees that only one consumer in a consumer group reads a given partition to preserve ordering and avoid duplicate processing.

#### Q2: What happens if the number of consumers is greater than the number of partitions?

**Answer:** Extra consumers remain idle because each partition can be assigned to only one consumer within a consumer group.

---

## 3. Offsets

### What is an Offset?

**Offset** is the unique sequence number of a message inside a partition.

**Example:**
```
Partition
Offset 0   │ Msg A
Offset 1   │ Msg B
Offset 2   │ Msg C
Offset 3   │ Msg D
```

- Offsets start from **0**.
- Unique **within a partition**, not across the entire topic.

---

### Offset Storage

Kafka stores committed offsets inside:

```
__consumer_offsets
```

This is an internal Kafka topic.

---

### Current Offset

The **next** offset the consumer will read.

**Example:**
```
Current Offset = 120

Consumer will read: 120, 121, 122, ...
```

---

### Committed Offset

Last **successfully saved** offset.

After restart, the consumer resumes from the committed offset.

---

### End Offset

Offset **after** the last available message in a partition.

**Example:**
```
Messages:    0  1  2  3
End Offset = 4
```

> If current offset equals end offset, the consumer is **caught up**.

---

### Offset Reset

Used when **no committed offset exists**.

**Values:**

| Value | Behavior |
|---|---|
| `earliest` | Read from beginning |
| `latest` | Read only new messages |
| `none` | Throw exception if no offset exists |

- **`earliest`** — Reads all existing messages. Useful for replay or new services.
- **`latest`** — Reads only newly arriving messages. Useful for real-time consumers.

---

### Auto Commit

Kafka automatically commits offsets at a configured interval (`auto.commit.interval.ms`).

**Example:** Every 5 seconds.

> ⚠ **Simple but risky.** If the consumer crashes between processing a message and committing its offset, messages may be lost or reprocessed (at-least-once / at-most-once semantics can be violated). Many production systems prefer manual offset commits for reliability.

---

### Manual Commit

The application commits offsets only **after** successful processing.

> ✅ **Safer.** Preferred in production for reliable processing.

---

### Offset Replay

The consumer intentionally reads old messages again.

**Useful for:**
- Debugging
- Reprocessing
- Recovery
- Analytics

Done by seeking to an earlier offset or resetting offsets.

---

### Offset Retention

Kafka does not keep committed offsets forever.

- If a consumer group stays inactive beyond the configured retention period, its stored offsets may be removed.
- When it reconnects, `auto.offset.reset` determines where it starts reading.

---

### ⭐ Interview Questions — Offsets

#### Q1: What is the difference between Current Offset and Committed Offset?

| Current Offset | Committed Offset |
|---|---|
| Position currently being read | Last saved checkpoint |
| Changes while consuming | Changes only after commit |
| In-memory during processing | Persisted in Kafka (`__consumer_offsets`) |

#### Q2: What is the difference between earliest and latest?

| `earliest` | `latest` |
|---|---|
| Reads all existing messages | Reads only newly arriving messages |
| Useful for replay or new services | Useful for real-time consumers |

---

## 🔥 Must-Remember Interview Points

- ✅ **Topic** = Logical container of messages.
- ✅ Topic → Multiple **Partitions**.
- ✅ Ordering is guaranteed **only within a partition**.
- ✅ Same key always maps to the same partition (unless partition count changes).
- ✅ More partitions = More **parallelism** and **scalability**.
- ✅ One partition → **One consumer** per consumer group.
- ✅ **Offset** is unique within a partition, starting at **0**.
- ✅ Committed offsets are stored in the internal topic **`__consumer_offsets`**.
- ✅ `auto.offset.reset` is used **only** when no valid committed offset exists.
- ✅ **Manual offset commit** is generally preferred over auto commit in production.
- ✅ Retention deletes messages based on time or size; **compaction** retains the latest value for each key.