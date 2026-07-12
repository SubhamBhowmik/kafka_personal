# Kafka Consumer Groups – Interview Notes ⭐⭐⭐⭐⭐

---

## 1. Consumer Lag

### Definition

Consumer Lag is the difference between the latest data available in a partition and the data that the consumer has successfully committed.

```
Consumer Lag = Log End Offset (LEO) - Committed Offset
```

### What Is Latest Offset?

Latest Offset is also called the **Log End Offset (LEO)**.

It is **NOT** the offset of the last message.

Instead, it is the **next offset** that Kafka will assign to a newly produced message.

#### Example

```
Partition Offset: 0  1  2  3  4  5  6  7  8  9

There are 10 messages.
Last message offset = 9
Log End Offset (LEO) = 10
```

**Why?** Because the next produced message will receive offset 10.

### Committed Offset

The committed offset represents the **next message the consumer should read** after a successful commit.

#### Example

```
LEO = 10
Committed Offset = 7

Consumer has already processed offsets: 0 1 2 3 4 5 6
Remaining messages: 7 8 9

Lag: 10 - 7 = 3
```

#### Another Example

```
Partition: 0 1 2 3 4 5 6 7 8 9 10 11 12

Producer has written up to offset 12.
Therefore: LEO = 13

Committed Offset: 9

Lag: 13 - 9 = 4

Remaining messages: 9, 10, 11, 12
```

### Interview Tip ⭐⭐⭐⭐⭐

| ❌ Incorrect | ✅ Correct |
|---|---|
| Latest Offset = Last Message Offset | Last Message Offset = Offset of the most recently written message |
| | Log End Offset (LEO) = Next offset that Kafka will assign |

Kafka uses **LEO** while calculating consumer lag.

---

## 2. Sticky Assignment vs Cooperative Rebalancing

*This is one of Kafka's most frequently asked interview topics.*

### Sticky Assignment

#### Definition

Sticky Assignment is a partition assignment strategy. Its goals are:
- Keep the load balanced.
- Move **as few partitions as possible** during rebalance.

It tries to preserve existing assignments.

#### Example

```
Before:
Consumer A → P0  P1  P2
Consumer B → P3  P4  P5

Consumer C joins.
```

**Without Sticky Assignment:**

```
A → P0  P1
B → P2  P3
C → P4  P5
```

Many partitions move.

**With Sticky Assignment:**

```
A → P0  P1
B → P3  P4
C → P2  P5
```

Only the necessary partitions move.

#### Advantages

- Less partition movement
- Better cache locality
- Less state transfer
- Faster recovery

### Cooperative Rebalancing

#### Definition

Cooperative Rebalancing is a **rebalance protocol**. Instead of revoking every partition from every consumer, Kafka moves only the partitions that actually need to move.

Also called: **Incremental Rebalancing**

#### Traditional (Eager) Rebalance

```
Before:
A → P0  P1  P2
B → P3  P4  P5

Consumer C joins.

Step 1: A → nothing   B → nothing   (All consumers stop)

Step 2: A → P0  P1    B → P2  P3    C → P4  P5

During this time, nobody consumes messages.
```

#### Cooperative Rebalance

```
Before:
A → P0  P1  P2
B → P3  P4  P5

Consumer C joins.

Step 1: A → P0  P1    B → P3  P4    C → none
(Only partitions that must move are revoked. Consumers continue consuming.)

Step 2: A → P0  P1    B → P3  P4    C → P2  P5
```

Minimal disruption.

### Sticky Assignment vs Cooperative Rebalancing

| Aspect | Sticky Assignment | Cooperative Rebalancing |
|---|---|---|
| Type | Assignment strategy | Rebalance protocol |
| Decides | Who gets which partitions | How partitions move |
| Minimizes | Partition movement | Consumer downtime |
| Pause | May still pause all consumers | Consumers continue processing unaffected partitions |
| Assignor | StickyAssignor | CooperativeStickyAssignor |

### Interview Questions ⭐⭐⭐⭐⭐

**Q1. Does Sticky Assignment eliminate rebalancing?**

No. It only minimizes partition movement.

**Q2. Does Cooperative Rebalancing decide partition ownership?**

No. It only changes how partitions are transferred. The assignor still decides ownership.

### One-Line Summary

- **Sticky Assignment:** Keep partition ownership stable.
- **Cooperative Rebalancing:** Move partitions incrementally without stopping everyone.

---

## 3. How to Reduce Consumer Lag

Consumer lag occurs when:

```
Producer Speed > Consumer Processing Speed
```

**Goal:** Increase consumer throughput or decrease producer throughput.

### 1. Increase Consumers

If a topic has multiple partitions, add more consumers in the same consumer group.

```
Before:
P0  P1  P2  P3
     ↓
Consumer A

After:
P0 → A    P1 → B    P2 → C    P3 → D
```

**Note:** Maximum active consumers = Number of partitions.

### 2. Increase Partitions

More partitions allow more consumers to process data in parallel.

```
4 Partitions → Maximum 4 consumers
Increase to 12 Partitions → Maximum 12 consumers
```

### 3. Optimize Business Logic

**Common bottlenecks:**
- Slow database queries
- REST API calls
- Heavy computation
- Blocking I/O
- Expensive serialization

**Improve by:**
- Batch database writes
- Cache data
- Use asynchronous processing
- Reduce network calls

### 4. Increase max.poll.records

```
Default: max.poll.records = 500
Example: max.poll.records = 2000
```

Each `poll()` returns more records. Ensure processing completes within `max.poll.interval.ms`.

### 5. Tune Fetch Settings

Important properties:
- `fetch.min.bytes`
- `fetch.max.bytes`
- `max.partition.fetch.bytes`

Larger fetches reduce network overhead.

### 6. Process Records in Batches

| Instead of | Use |
|---|---|
| Read 1 → Process → Commit | Read 500 → Process → Commit |

Batching is more efficient.

### 7. Scale Consumer Instances

Run multiple application instances. Kafka distributes partitions automatically.

### 8. Poll Frequently

| Bad | Good |
|---|---|
| `poll()`; Process for 30 seconds; | `poll()`; Hand records to worker threads; `poll()` again quickly; |

This maintains heartbeats and avoids unnecessary rebalances.

### 9. Use Multithreading Correctly

```
Consumer Thread → Blocking Queue → Worker Threads
```

**Important:** KafkaConsumer is **not thread-safe**. Only the polling thread should call:
- `poll()`
- `commitSync()`
- `commitAsync()`
- `seek()`
- `close()`

### 10. Optimize Offset Commits

| Bad | Better |
|---|---|
| Process one message → `commitSync()` | Process 100 messages → `commitSync()` |

Or use `commitAsync()` when appropriate.

### 11. Increase Resources

If consumers are CPU or memory constrained:
- Add CPU
- Increase RAM
- Use SSDs
- Improve network bandwidth

### 12. Apply Backpressure

If producers write faster than consumers can process:
- Slow producers
- Use rate limiting/backpressure
- Let consumers catch up

### Interview Questions ⭐⭐⭐⭐⭐

**Q1. If consumer lag increases, should you always add more consumers?**

No. Adding consumers helps only if:
- There are enough partitions.
- The bottleneck is parallelism.

If the bottleneck is the database, external APIs, CPU, or inefficient processing, optimize those first.

**Q2. Can adding more consumers reduce lag for a topic with only one partition?**

No. Only one consumer in a consumer group can consume from a single partition at a time. To increase parallelism, increase the number of partitions.

---

## 4. Quick Revision

| Topic | Key Point |
|---|---|
| Consumer Lag | LEO - Committed Offset |
| Latest Offset | Log End Offset (next offset to be assigned) |
| LEO | Not the last message offset |
| Sticky Assignment | Keeps partition ownership stable with minimal movement |
| Cooperative Rebalancing | Transfers only required partitions incrementally |
| Sticky vs Cooperative | Who gets partitions vs How partitions are moved |
| Increase Consumers | Improves parallelism (limited by partition count) |
| Increase Partitions | Allows more active consumers |
| Optimize Processing | Remove DB/API bottlenecks |
| `max.poll.records` | Fetch more records per poll |
| Batch Processing | Process many records before committing |
| Multithreading | KafkaConsumer must remain single-threaded |
| Batch/Async Commits | Reduce commit overhead |
| Scale Resources | Add CPU, RAM, faster disks |
| Backpressure | Slow producers if consumers cannot keep up |

**Interview mantra:** Consumer Lag = Producer Speed − Consumer Processing Capacity. Reduce lag by increasing consumer throughput (more partitions, more consumers, faster processing) or by reducing the rate at which producers generate messages.

---

## 5. seek() in Kafka Consumer ⭐⭐⭐⭐⭐

### Definition

`seek()` is a method of `KafkaConsumer` that manually changes the consumer's read position (offset) for a partition.

Normally, Kafka resumes reading from the committed offset, but `seek()` lets you override that and start reading from any valid offset.

```java
consumer.seek(TopicPartition partition, long offset);
```

### Why Do We Use seek()?

| Default | With seek() |
|---|---|
| Consumer starts from committed offset | Consumer starts from the offset you specify |

This is useful when you want to:
- Reprocess old messages
- Skip corrupted messages
- Replay data
- Recover from failures
- Debug production issues

### Example

Suppose a partition contains:

```
Offset    Message
0         A
1         B
2         C
3         D
4         E
5         F
6         G
```

The committed offset is: `Committed Offset = 5`

Normally, the consumer starts from offset 5 and reads: `5 → F`, `6 → G`.

#### Using seek()

```java
TopicPartition tp = new TopicPartition("orders", 0);
consumer.assign(List.of(tp));
consumer.seek(tp, 2);
```

Now the consumer starts from offset 2 and reads:

```
2 → C
3 → D
4 → E
5 → F
6 → G
```

Even though the committed offset was 5.

### Common seek() Methods

**1. Seek to a specific offset**

```java
consumer.seek(tp, 100);
```

Start reading from offset 100.

**2. Seek to the beginning**

```java
consumer.seekToBeginning(List.of(tp));
```

Reads from offset 0. Useful for replaying all messages.

**3. Seek to the end**

```java
consumer.seekToEnd(List.of(tp));
```

Moves to the current Log End Offset (LEO), so only new messages arriving afterward will be consumed.

### Real-World Example: Reprocessing a DLQ

Suppose your application failed while processing offset 250. You fixed the bug and now want to reprocess from offset 250 instead of waiting for new data.

```java
consumer.assign(List.of(tp));
consumer.seek(tp, 250);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    // Process records from offset 250 onward
}
```

### Does seek() Change the Committed Offset?

**No.**

`seek()` only changes the current reading position of the consumer.

```
Committed Offset = 50
seek(20)

The consumer starts reading from 20, but the committed offset in Kafka is still 50.
```

Only after you commit again does Kafka update the committed offset.

### seek() vs Committed Offset

| seek() | Committed Offset |
|---|---|
| Changes current read position | Stores recovery point in Kafka |
| Temporary until next commit | Persistent across restarts |
| Local to the consumer instance | Stored in the `__consumer_offsets` topic |
| Used for replay/skip/debugging | Used for normal restart behavior |

### Important Notes

- `seek()` works on **assigned partitions only**.
- You should call it after `assign()` or after a rebalance when partitions have been assigned.
- If you call `seek()` before a partition is assigned, Kafka throws an exception.
- `KafkaConsumer` is not thread-safe, so `seek()` must be called from the same thread that calls `poll()`.

### Interview Questions ⭐⭐⭐⭐⭐

**Q1. Does seek() permanently change the consumer offset?**

**Answer:** No. It changes only the current read position. The committed offset remains unchanged until you explicitly commit.

**Q2. What is the difference between auto.offset.reset and seek()?**

| auto.offset.reset | seek() |
|---|---|
| Used only when there is no committed offset | Can be used at any time after partition assignment |
| Automatic behavior (earliest or latest) | Manual control over the exact offset |
| Applies when starting consumption | Can jump to any valid offset during execution |

### Easy Way to Remember

Think of a video player:
- **Committed Offset** = The last saved playback position.
- **seek()** = Dragging the progress bar to any timestamp.
- **Commit** = Clicking Save Progress so the next time you open the video, it resumes from the new position.