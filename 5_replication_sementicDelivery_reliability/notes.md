Kafka - Replication ⭐⭐⭐⭐⭐ (Interview Notes)

## Replication Factor (RF)

**Definition**

Replication Factor is the number of copies of a partition stored across different brokers.

- Every partition has **1 Leader Replica**.
- Remaining replicas are **Follower Replicas**.

**Example**

Topic: Orders, RF = 3

Partition-0:
| Broker  | Role     |
|---------|----------|
| Broker1 | Leader   |
| Broker2 | Follower |
| Broker3 | Follower |

**Why Replication?**
- High Availability
- Fault Tolerance
- Prevent Data Loss
- Leader Failover

**Important Points**
- RF cannot be greater than the number of brokers.

  **Example:** 3 Brokers → Maximum RF = 3

- Replication happens **partition-wise**, not topic-wise.

**Interview Notes**

| RF   | Implication                                                        |
|------|--------------------------------------------------------------------|
| RF=1 | No fault tolerance; broker failure → data unavailable               |
| RF=3 | One broker can fail safely; data still available                    |

**Tricky Questions**

**Q1.** Can Replication Factor be changed after topic creation?
- **Yes.** Use partition reassignment tools or admin APIs to increase/decrease replication (subject to cluster capacity).

**Q2.** Does RF improve consumer throughput?
- **No.** Replication improves **availability**, not read/write throughput.

---

## Leader Replica

**Definition**

Leader Replica is the replica that handles:
- All **Reads**
- All **Writes**

Every partition has exactly **one** leader.

```
 Producer
    |
    v
 Leader Replica
    |
  Replication
    |
 Follower 1   Follower 2
```

**Responsibilities**
- Accept producer requests
- Serve consumer fetch requests
- Replicate data to followers
- Track ISR

**Important Notes**
- Consumers **always read from the leader** (standard Kafka behavior).
- Followers **never serve clients** unless specifically configured in certain scenarios.

**Tricky Questions**

**Q1.** Can a partition have two leaders?
- **No.** Exactly one leader per partition.

**Q2.** If leader crashes, what happens?
- One ISR follower becomes the new leader.

---

## Follower Replica

**Definition**

Follower Replicas continuously copy data from the leader. They **never accept producer writes**.

**Responsibilities**
- Fetch new records
- Stay synchronized
- Become leader if current leader fails

**Important Notes**
- Followers **pull** data from the leader — they do not receive pushed updates.

**Tricky Questions**

**Q1.** Can consumers read from followers?
- Normally, **no**. Consumers read from leaders.

**Q2.** Can followers become leaders?
- **Yes.** Only if they belong to the ISR (normally).

---

## ISR (In-Sync Replicas)

**Definition**

ISR is the set of replicas that are fully synchronized (or sufficiently caught up) with the leader.

**Example**

RF = 3
```
 Leader
 Follower A  ✔
 Follower B  ✔
 ISR = {Leader, A, B}
```

If B falls behind:
```
 Leader
 Follower A ✔
 Follower B ✖
 ISR = {Leader, A}
```

**Why ISR Matters**
- Kafka elects a new leader from ISR during normal failover.

**Important Notes**
- Leader is always part of ISR.
- Slow followers are removed from ISR.
- Once caught up, they rejoin ISR.

**Tricky Questions**

**Q1.** Can ISR contain only one replica?
- **Yes.** Leader alone can remain in ISR if all followers are behind.

**Q2.** Does ISR mean identical byte-for-byte at every instant?
- **Not necessarily.** Followers may have a small acceptable lag but are still considered in sync based on Kafka's thresholds.

---

## Replica Synchronization

**Definition**

Followers continuously fetch new records from the leader to stay synchronized.

```
 Producer
    ↓
  Leader
    ↓
 Follower Fetch Request
    ↓
 Follower Copies Data
```

**Process**
1. Producer writes to Leader.
2. Leader appends record.
3. Followers fetch data.
4. Followers append record.
5. Offset advances.

**Important Notes**
- Replication is **asynchronous**.
- Followers periodically send fetch requests.

**Benefits**
- Fast recovery
- High availability
- Fault tolerance

**Tricky Questions**

**Q1.** Does the leader push data to followers?
- **No.** Followers pull (fetch) data from the leader.

**Q2.** Does producer wait for all followers?
- **Depends on acks.**
  - `acks=0` → No waiting
  - `acks=1` → Wait for leader only
  - `acks=all` → Wait according to `min.insync.replicas`

---

## Preferred Leader Election

**Definition**

Kafka tries to make the preferred replica (usually the first replica assigned to the partition) the leader again after failures or maintenance.

**Example**

```
 Original:                  Broker1 crashes:           Broker1 returns:
 Broker1 → Leader (Pref.)   Broker2 → Leader      →   Preferred Leader Election
 Broker2 → Follower                                   Broker1 → Leader again
 Broker3 → Follower
```

**Why?**
- Balanced leadership
- Even CPU usage
- Predictable partition placement

**Important Notes**
- It is not mandatory that the preferred replica always be the leader.
- Election happens only if the preferred replica is eligible (typically in ISR).

**Tricky Questions**

**Q1.** Why perform preferred leader election?
- To rebalance leaders across brokers after failures or maintenance.

**Q2.** Will preferred leader election cause downtime?
- Usually only a brief leadership switch; clients retry automatically.

---

## Unclean Leader Election

**Definition**

Occurs when Kafka elects a replica **outside the ISR** as leader because no ISR replicas are available.

**Example**

RF = 3
```
 Leader
 Follower A (ISR)
 Follower B (Out of ISR)
 ```
Leader and Follower A crash. Only Follower B remains.

With unclean leader election enabled:
```
 Follower B → Leader
```

**Advantage**
- Cluster becomes available quickly.

**Disadvantage**
- Possible **data loss** — the new leader may not have the latest records.

**Configuration**
| Setting                           | Behavior                                                                                |
|-----------------------------------|-----------------------------------------------------------------------------------------|
| `unclean.leader.election.enable` = `false` (recommended in production) | Prioritize consistency; partition may remain unavailable until an ISR replica returns. |
| `unclean.leader.election.enable` = `true` | Prioritize availability; risk losing acknowledged-but-not-replicated data.              |

**Interview Notes**
- Use unclean leader election **only when availability is more important than strict durability**.

**Tricky Questions**

**Q1.** Why is unclean leader election dangerous?
- It can elect a stale replica, causing loss of recent committed data.

**Q2.** If `unclean.leader.election.enable=false` and all ISR replicas fail, what happens?
- No new leader is elected until an ISR replica comes back; the partition remains temporarily unavailable.

---

# Kafka – Delivery Semantics ⭐⭐⭐⭐⭐

**Definition**

Delivery semantics define the guarantee Kafka provides about how many times a message is delivered from producer to consumer.

**Three guarantees:**
| Guarantee      | Description                                        |
|----------------|----------------------------------------------------|
| At Most Once   | Message may be lost, but never duplicated.         |
| At Least Once  | No message loss, but duplicates are possible.      |
| Exactly Once   | No loss and no duplicates.                         |

---

## At Most Once

**Definition**

A message is delivered **zero or one time**. If a failure occurs before the message is safely written or processed, the message is lost.

**Flow**
```
 Producer → Kafka → Consumer
               ✖ Failure
         Message Lost
```

**Characteristics**
- No duplicate messages
- Possible message loss
- Fastest delivery
- Lowest reliability

**How it happens**
- Producer does not retry sending
- Consumer commits offset **before** processing

**Use Cases**
- Monitoring logs
- Metrics collection
- Analytics where losing a few messages is acceptable

---

## At Least Once

**Definition**

A message is delivered **one or more times**. Kafka guarantees the message is not lost, but it may be processed multiple times.

**Flow**
```
 Producer → Kafka → Consumer
                 ↓
         Processing succeeds
                 ↓
         Offset committed

 Failure before commit → Message is read again
```

**Characteristics**
- No message loss
- Duplicate messages possible
- Most commonly used delivery guarantee

**How it happens**
- Producer retries sending
- Consumer commits offset **after** successful processing

**Prevent Duplicate Effects**
- Use idempotent processing
- Store processed message IDs
- Use UPSERT instead of INSERT

**Use Cases**
- Payment notifications
- Email sending
- Order processing

---

## Exactly Once (EOS)

**Definition**

Each message is processed **exactly one time**, even if failures occur. No duplicates and no message loss.

**Characteristics**
- Highest reliability
- More overhead than other modes
- Requires Kafka transactions

**Requirements**
- Enable Idempotent Producer
- Use Transactions
- Commit consumer offsets within the same transaction
- Supported by Kafka Streams and transactional producers

**Configuration**
- `enable.idempotence=true`
- `transactional.id=my-app-1`
- `acks=all`

**Use Cases**
- Banking transactions
- Financial systems
- Inventory management
- Money transfers

---

## Duplicate Messages

**Definition**

The same message is delivered or processed more than once.

**Causes**
- Producer retries after timeout
- Consumer crashes before committing offset
- Network failures
- Broker acknowledgement lost

**Example**
```
 Producer sends Message A
    ↓
 Broker stores message
    ↓
 ACK lost
    ↓
 Producer retries
    ↓
 Broker receives same message again
```

**Prevention**
- Idempotent Producer
- Transactions
- Idempotent consumer logic
- Unique message IDs

---

## Message Loss

**Definition**

A produced message never reaches the consumer.

**Causes**
- Producer retries disabled
- `acks=0`
- Broker failure before replication
- Consumer commits offset before processing
- Unclean leader election (in some configurations)

**Prevention**
- `acks=all`
- Replication Factor ≥ 3
- `min.insync.replicas` ≥ 2
- Enable producer retries
- Commit offsets after processing
- Use transactions for critical applications

---

## Comparison Table

| Delivery Guarantee | Message Loss  | Duplicate Messages | Reliability | Performance |
|--------------------|---------------|-------------------|-------------|-------------|
| At Most Once       | ✅ Possible   | ❌ No             | Low         | Fastest     |
| At Least Once      | ❌ No         | ✅ Possible       | High        | Medium      |
| Exactly Once       | ❌ No         | ❌ No             | Highest     | Slowest     |

**Interview Notes ⭐⭐⭐**
- **At Most Once** = Lose messages, no duplicates.
- **At Least Once** = No loss, duplicates possible.
- **Exactly Once** = No loss, no duplicates (uses transactions + idempotence).
- Kafka's default behavior is generally **At Least Once** when producer retries are enabled and offsets are committed after processing.
- **Exactly Once** is supported only for Kafka-to-Kafka processing using Kafka transactions and compatible consumers/producers.

**Tricky Interview Questions**

**Q1.** Why can duplicate messages occur even if Kafka stores only one copy?
- **Answer:** If the producer doesn't receive the broker's ACK (e.g., due to a network issue), it retries sending the same message. Without idempotence, Kafka may append the retried message again, resulting in duplicates.

**Q2.** Can Kafka guarantee Exactly Once if the consumer writes to an external database?
- **Answer:** Not by itself. Kafka's Exactly Once Semantics apply to Kafka's own transactional ecosystem. When interacting with external systems, you need additional techniques such as idempotent database operations, transactional outbox patterns, or distributed transaction strategies to achieve end-to-end exactly-once behavior.

---

# Kafka – Reliability ⭐⭐⭐⭐⭐

**Definition**

Reliability in Kafka means ensuring that messages are delivered correctly, consistently, and remain available even when producers, consumers, or brokers fail.

Kafka achieves reliability using:
- Replication
- Idempotence
- Transactions
- High Watermark
- Fault Tolerance
- Ordering Guarantees

---

## Idempotence

**Definition**

An **Idempotent Producer** ensures that even if the producer retries sending a message, Kafka stores it only once. This prevents duplicate messages caused by retries.

**Why it is needed**

Without idempotence:
```
 Producer → Broker
         ACK lost
 Producer retries
 Broker stores same message again

 Result:
 Message A
 Message A   (Duplicate)
```

With idempotence:
```
 Producer → Broker
 ACK lost
 Producer retries
 Broker recognizes duplicate
 Stores only one copy
```

**How it works**

Kafka assigns:
- **Producer ID (PID)** → Unique ID for each producer.
- **Sequence Number** → Incremented for every message sent to a partition.

The broker tracks the last sequence number received. If it receives the same sequence number again, it discards the duplicate.

**Configuration**
- `enable.idempotence=true`
- `acks=all`
- `retries=Integer.MAX_VALUE`

**Benefits**
- Prevents duplicate writes
- Safe producer retries
- Required for Kafka Transactions

---

## Transactions

**Definition**

Transactions allow multiple Kafka operations to succeed or fail as a **single atomic unit**.

Either:
- All messages are committed, **or**
- None of them are visible.

**Example**
```
 Read Topic A
    ↓
 Process
    ↓
 Write Topic B   Write Topic C
    ↓
 Commit Offset
```
If a failure occurs before commit:
- Nothing is committed.
- No partial writes.

**Configuration**
- `transactional.id=my-producer`
- `enable.idempotence=true`

**Transaction Flow**
```
 beginTransaction()
    ↓
 send(topicA)
 send(topicB)
    ↓
 commitTransaction()

 If an error occurs:
 abortTransaction()
```

**Use Cases**
- Banking
- Order processing
- Inventory updates
- Kafka Streams

---

## Read Committed

**Definition**

Consumers read only committed transactional messages. Aborted or unfinished transaction messages are hidden.

**Configuration**
- `isolation.level=read_committed`

**Example**
```
 Transaction Started
 Message A
 Message B
 Transaction Aborted

 Consumer sees: Nothing
```

**Advantages**
- No dirty reads
- Used with Exactly Once processing
- Recommended for transactional consumers

---

## Read Uncommitted

**Definition**

Consumers read **all** messages, including messages from aborted or incomplete transactions.

**Configuration**
- `isolation.level=read_uncommitted`

**Example**
```
 Producer starts transaction
 Message A
 Message B
 Transaction Aborted

 Consumer still reads: A, B
 (even though the transaction never committed)
```

**Advantages**
- Lower latency
- Simpler processing

**Disadvantages**
- Consumers may process invalid or rolled-back data

---

## Ordering Guarantees

**Definition**

Kafka guarantees that messages within the **same partition** are read in the exact order they were written.

**Example**
```
 Partition 0: M1 → M2 → M3 → M4

 Consumer reads: M1, M2, M3, M4 (order preserved)
```

**Important Points**
- Ordering is guaranteed **only within a single partition**.
- There is **no global ordering** across multiple partitions.

**Example**
```
 Partition 0: A, B, C
 Partition 1: X, Y, Z

 Possible read order: A, X, B, Y, C, Z
```

**How to preserve order**
- Use the same message key so related records go to the same partition.
- Avoid multiple in-flight requests if strict ordering is required.

---

## High Watermark (HW)

**Definition**

The **High Watermark** is the highest offset that has been successfully replicated to **all In-Sync Replicas (ISR)**. Consumers can read only up to the High Watermark.

**Example**
```
 Leader offsets: 0, 1, 2, 3, 4, 5
 Follower replicated only till 4

 High Watermark = 4
 Consumers cannot read Offset 5 until it is replicated to all ISR.
```

**Why it is important**

Without HW:
```
 Leader writes Offset 5 → Consumer reads it
 Leader crashes
 Follower becomes leader (doesn't have Offset 5)
 Consumer has seen data that no longer exists.
```

High Watermark prevents this inconsistency.

**Benefits**
- Prevents phantom reads
- Ensures consumers see only durable data
- Protects against leader failures

---

## Fault Tolerance

**Definition**

Fault Tolerance is Kafka's ability to continue operating even if brokers, producers, or consumers fail.

**How Kafka achieves it**

| Mechanism               | Description                                                                  |
|-------------------------|------------------------------------------------------------------------------|
| **1. Replication**      | Every partition has multiple replicas (Leader + Followers). If leader fails, a follower becomes leader. |
| **2. ISR**              | Only replicas that are fully caught up belong to the ISR. Leader election happens from ISR members. |
| **3. Producer Retries** | If sending fails, retry automatically.                                       |
| **4. Consumer Offsets** | Consumers continue from the last committed offset after a restart.           |
| **5. Leader Election**  | When leader crashes, a follower is promoted to leader. Service continues with minimal interruption. |

**Best Practices**
- Replication Factor ≥ 3
- `acks=all`
- `min.insync.replicas` ≥ 2
- Enable idempotence
- Use transactions for critical workloads
- Monitor ISR and Under-Replicated Partitions (URPs)

---

## Comparison Table

| Feature            | Purpose                                      | Key Benefit                                  |
|--------------------|----------------------------------------------|----------------------------------------------|
| Idempotence        | Prevent duplicate writes                     | No duplicate messages on producer retries    |
| Transactions       | Atomic writes across topics/offsets          | Exactly Once processing                      |
| Read Committed     | Read only committed transactions             | No dirty reads                               |
| Read Uncommitted   | Read all messages                            | Lower latency, may see aborted data          |
| Ordering Guarantees| Preserve message order                       | Order guaranteed within a partition          |
| High Watermark     | Expose only fully replicated records         | Prevents reading data that could be lost     |
| Fault Tolerance    | Continue during failures                     | High availability and durability             |

**Interview Notes ⭐⭐⭐**
- **Idempotence** prevents duplicate writes caused by producer retries.
- **Transactions** require idempotence and provide atomic writes plus offset commits.
- **read_committed** hides aborted transactional messages; **read_uncommitted** shows them.
- Kafka guarantees ordering **per partition**, not across partitions.
- **High Watermark** is the highest offset replicated to all ISR — consumers read only up to this point.
- **Replication, ISR, leader election, retries, and offset management** together provide Kafka's fault tolerance.

**Tricky Interview Questions**

**Q1.** Does enabling `enable.idempotence=true` guarantee Exactly Once Semantics?
- **Answer:** No. It only prevents duplicate writes from a producer. Exactly Once Semantics also require transactions and consumers configured with `isolation.level=read_committed` when processing transactional data.

**Q2.** Why don't consumers read records beyond the High Watermark?
- **Answer:** Because those records may exist only on the leader and not yet be replicated to all ISR. If the leader fails before replication completes, those records could be lost. The High Watermark ensures consumers only see data that is safely replicated and durable.