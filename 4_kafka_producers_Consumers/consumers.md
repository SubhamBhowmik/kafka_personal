# Kafka Consumer ⭐⭐⭐⭐⭐

A Kafka Consumer is an application that reads (consumes) messages from Kafka topics. Consumers keep track of which records they have already processed using offsets.

---

## 1. Consumer Architecture

### Definition

Kafka Consumer Architecture describes how a consumer connects to brokers, fetches records, processes them, commits offsets, and maintains membership in a consumer group.

### Architecture Flow

```
+----------------------+
| Kafka Consumer App   |
+----------+-----------+
           |
           |
     KafkaConsumer API
           |
           |
      Poll Loop
           |
           |
   Fetch Request to Broker
           |
           |
+----------------------+
| Kafka Broker         |
| Topic -> Partition   |
+----------------------+
           |
      Records Returned
           |
     Process Records
           |
 Commit Offset (Auto/Manual)
```

### Components

**Consumer Application** — Your Java application.

```java
public class OrderConsumer {
    public static void main(String[] args) {
    }
}
```

**KafkaConsumer** — Main class responsible for:
- Joining consumer group
- Fetching records
- Offset management
- Heartbeats
- Rebalancing

**Consumer Group** — Multiple consumers work together.

```
Topic: P0  P1  P2  P3
           │
Consumer Group
           │
Consumer1 → P0, P1
Consumer2 → P2, P3
```

**Broker** — Stores data. Consumer always reads from **Leader Partition**. Never from follower.

**Offset Manager** — Stores processed offsets in `__consumer_offsets`.

### Consumer Flow

```
Start Consumer
     │
     ▼
Join Consumer Group
     │
     ▼
Partition Assignment
     │
     ▼
Send Heartbeats
     │
     ▼
Poll()
     │
     ▼
Broker Fetches Records
     │
     ▼
Application Processes
     │
     ▼
Commit Offset
     │
     ▼
Poll Again
```

### Interview Notes

- ✔ Consumer never pushes data. Broker never sends data automatically. Consumer always **pulls** data.
- ✔ Consumer stores reading progress using **offsets**.
- ✔ Broker doesn't know whether a message is processed. Consumer tells Kafka by committing offsets.

### Tricky Interview Questions

**Q1. Does Kafka push records to consumers?**
No. Consumers always pull records using `poll()`.

**Q2. Can Consumer read from Follower Replica?**
No. Consumer reads only from the Leader.

---

## 2. KafkaConsumer API

### Definition

Kafka provides `KafkaConsumer` class for consuming messages.

```
Package: org.apache.kafka.clients.consumer.KafkaConsumer
```

### Creating Consumer

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "order-group");

KafkaConsumer<String, String> consumer =
    new KafkaConsumer<>(props);
```

### Subscribe

```java
consumer.subscribe(List.of("orders"));
```

### Poll

```java
ConsumerRecords<String, String> records =
    consumer.poll(Duration.ofMillis(100));
```

Returns `ConsumerRecords`.

### Read Records

```java
for (ConsumerRecord<String, String> record : records) {
    System.out.println(record.key());
    System.out.println(record.value());
    System.out.println(record.partition());
    System.out.println(record.offset());
}
```

### Close

```java
consumer.close();
```

### Important APIs

| API | Purpose |
|---|---|
| `subscribe()` | Subscribe to topic |
| `assign()` | Manual partition assignment |
| `poll()` | Fetch records |
| `commitSync()` | Sync offset commit |
| `commitAsync()` | Async commit |
| `seek()` | Move offset |
| `position()` | Current offset |
| `pause()` | Stop consuming temporarily |
| `resume()` | Resume consumption |
| `wakeup()` | Interrupt poll safely |
| `close()` | Shutdown |

### Interview Notes

- `subscribe()` → Consumer Group Management
- `assign()` → Manual Assignment
- **Cannot use both together.**

### Tricky Questions

**Q1. Difference between subscribe() and assign()?**
`subscribe()` uses Consumer Group with automatic rebalancing. `assign()` is manual partition assignment with no rebalancing.

**Q2. Which method safely interrupts poll()?**
`consumer.wakeup()`

---

## 3. Poll Loop

### Definition

The Poll Loop is the continuous loop in which a consumer repeatedly calls `poll()` to fetch new records, process them, and optionally commit offsets.

Kafka consumers are **poll-driven**. If you stop calling `poll()`, the consumer eventually leaves the consumer group.

### Basic Poll Loop

```java
while (true) {
    ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
    }

    consumer.commitSync();
}
```

### What Happens During poll()?

`poll()` does much more than just fetch records. It:
- Sends heartbeats (when applicable)
- Joins the consumer group if needed
- Receives partition assignments
- Detects rebalances
- Fetches records from brokers
- Returns available records

### Poll Loop Lifecycle

```
poll()
  │
  ▼
Heartbeat
  │
  ▼
Fetch Request
  │
  ▼
Broker Returns Records
  │
  ▼
Process Records
  │
  ▼
Commit Offset
  │
  ▼
poll() Again
```

### Why Poll Frequently?

If `poll()` is not called within `max.poll.interval.ms`, Kafka assumes the consumer is stuck and removes it from the group, triggering a rebalance.

### Best Practices

- Keep processing reasonably fast.
- Offload long-running work to worker threads if needed.
- Continue calling `poll()` regularly.
- Handle exceptions inside the loop.

### Tricky Questions

**Q1. What happens if you stop calling poll()?**
After `max.poll.interval.ms` expires, Kafka removes the consumer from the group and triggers a rebalance.

**Q2. Does poll() only fetch records?**
No. It also participates in group management, partition assignment, and heartbeat coordination.

---

## 4. Fetch Requests

### Definition

A Fetch Request is the network request a consumer sends to a broker asking for new records from its assigned partitions.

Kafka is **pull-based**, so brokers never push messages.

### Flow

```
Consumer
  │
  ▼
Fetch Request
  │
  ▼
Broker
  │
  ▼
Read Log Segments
  │
  ▼
Return Records
```

### Fetch Configuration

| Property | Default | Description |
|---|---|---|
| `fetch.min.bytes` | 1 | Minimum data before broker responds. Higher values improve throughput but increase latency. |
| `fetch.max.bytes` | 50MB | Maximum data returned in one fetch request. |
| `max.partition.fetch.bytes` | 1MB | Maximum bytes fetched from a single partition. |
| `fetch.max.wait.ms` | 500 | Max time broker waits before responding if insufficient data is available. |

### Interview Notes

- Small fetch size → Lower latency, more network calls.
- Large fetch size → Better throughput, higher memory usage.

### Tricky Questions

**Q1. Why doesn't Kafka push records to consumers?**
A pull model lets consumers control their consumption rate, avoiding overload and providing natural backpressure.

**Q2. Which configuration limits records fetched from a single partition?**
`max.partition.fetch.bytes`

---

## 5. Heartbeats

### Definition

Heartbeats are periodic messages sent by a consumer to the group coordinator indicating that it is alive.

Without heartbeats, Kafka assumes the consumer has failed.

### Flow

```
Consumer
  │
  ▼
Heartbeat
  │
  ▼
Group Coordinator
  │
  ▼
Consumer Alive
```

### Why Are Heartbeats Important?

- Detect failed consumers.
- Maintain group membership.
- Avoid unnecessary downtime.
- Trigger rebalances only when needed.

### Related Configurations

- `heartbeat.interval.ms`
- `session.timeout.ms`

Typically, the heartbeat interval should be much smaller than the session timeout.

### Interview Notes

Heartbeats confirm liveness but do **not** indicate that message processing is making progress. That is monitored using `max.poll.interval.ms`.

### Tricky Questions

**Q1. Are heartbeats the same as polling?**
No. Heartbeats maintain group membership, while `poll()` retrieves records and drives consumer progress.

**Q2. What happens if heartbeats stop?**
After the session timeout expires, the consumer is considered dead and a rebalance is triggered.

---

## 6. Session Timeout

### Definition

`session.timeout.ms` defines how long the coordinator waits without receiving heartbeats before considering the consumer dead.

### Example

```
session.timeout.ms = 10000
```

If no heartbeat arrives for 10 seconds:
- Consumer is removed.
- Partitions are reassigned.
- Rebalance starts.

### Flow

```
Heartbeat Lost
  │
  ▼
Wait Session Timeout
  │
  ▼
Consumer Removed
  │
  ▼
Rebalance
```

### Tricky Questions

**Q1. Does session timeout monitor message processing time?**
No. It only monitors heartbeat activity.

**Q2. Which timeout detects a crashed consumer?**
`session.timeout.ms`

---

## 7. Max Poll Interval

### Definition

`max.poll.interval.ms` is the maximum allowed time between two successful `poll()` calls.

It detects consumers that are **alive but stuck** processing records.

### Example

```
max.poll.interval.ms = 5 minutes
```

If processing takes longer than 5 minutes and no `poll()` is called, the consumer is removed from the group.

### Difference from Session Timeout

| Session Timeout | Max Poll Interval |
|---|---|
| Detects **dead** consumers | Detects **stuck** consumers |
| Based on heartbeats | Based on `poll()` calls |

### Tricky Questions

**Q1. A consumer is sending heartbeats but never calls poll(). Will Kafka remove it?**
Yes. After `max.poll.interval.ms` expires, Kafka removes it from the group.

**Q2. Which timeout protects against infinite processing loops?**
`max.poll.interval.ms`

---

## 8. Auto Commit

### Definition

With Auto Commit enabled, Kafka periodically commits the latest returned offsets automatically.

```
Configuration:
enable.auto.commit = true

Commit interval:
auto.commit.interval.ms = 5000
```

### Risk

If offsets are committed before processing finishes and the application crashes, messages may be **skipped** after restart.

### Flow

```
Poll Records
  │
  ▼
Auto Commit
  │
  ▼
Application Crash
  │
  ▼
Some Records May Be Lost
```

### Best Use Cases

- Simple consumers
- Logging
- Metrics
- Non-critical workloads

### Tricky Questions

**Q1. Can Auto Commit cause message loss?**
Yes. If offsets are committed before processing completes, a crash can lead to skipped messages.

**Q2. Is Auto Commit exactly-once?**
No. It generally provides at-least-once behavior and can introduce duplicates or skips depending on timing.

---

## 9. Manual Commit

### Definition

The application explicitly commits offsets after successful processing.

```
Configuration:
enable.auto.commit = false
```

### Synchronous Commit

```java
consumer.commitSync();
```

Blocks until Kafka acknowledges the commit.

### Asynchronous Commit

```java
consumer.commitAsync();
```

Returns immediately without waiting.

### commitSync vs commitAsync

| commitSync | commitAsync |
|---|---|
| Reliable | Faster |
| Blocking | Non-blocking |
| Retries on failure | No automatic retry |

### Best Practice

```
Poll
  │
  ▼
Process Records
  │
  ▼
Commit Offset
  │
  ▼
Poll Again
```

### Tricky Questions

**Q1. Why is Manual Commit preferred in financial applications?**
Offsets are committed only after successful processing, reducing the chance of losing processed state.

**Q2. Can commitAsync() lose a commit?**
Yes. If the asynchronous commit fails and isn't handled, offsets may not be recorded.

---

## 10. Graceful Shutdown

### Definition

A graceful shutdown lets a consumer stop safely without losing progress or leaving the consumer group abruptly.

### Steps

1. Stop accepting new work.
2. Wake up the consumer.
3. Finish processing fetched records.
4. Commit final offsets.
5. Close the consumer.

### Example

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    consumer.wakeup();
}));

try {
    while (true) {
        consumer.poll(Duration.ofMillis(100));
    }
} catch (WakeupException e) {
    // Expected during shutdown
} finally {
    consumer.commitSync();
    consumer.close();
}
```

### Why Use wakeup()?

`poll()` is a blocking call. `wakeup()` interrupts it safely by throwing a `WakeupException`, allowing controlled shutdown.

### Tricky Questions

**Q1. Why not simply call consumer.close() from another thread?**
Because `poll()` may be blocked. `wakeup()` is the thread-safe mechanism designed to interrupt `poll()`.

**Q2. What happens if offsets are not committed before shutdown?**
On restart, the consumer resumes from the last committed offset, so previously processed records may be reprocessed.

---

## 11. Consumer Lifecycle

### Definition

The Consumer Lifecycle is the sequence of states a Kafka consumer goes through from startup until shutdown.

### Lifecycle

```
Create KafkaConsumer
     │
     ▼
Subscribe to Topic(s)
     │
     ▼
Join Consumer Group
     │
     ▼
Partition Assignment
     │
     ▼
Poll Loop Starts
     │
     ▼
Fetch Records
     │
     ▼
Process Records
     │
     ▼
Commit Offsets
     │
     ▼
Continue Polling
     │
     ▼
Graceful Shutdown
     │
     ▼
Leave Group
```

### Lifecycle Explained

1. **Create Consumer** — Initialize `KafkaConsumer` with configuration.
2. **Subscribe** — Subscribe to one or more topics.
3. **Join Group** — Coordinator registers the consumer in the consumer group.
4. **Partition Assignment** — Kafka assigns partitions to consumers.
5. **Poll** — Fetches records and performs coordination tasks.
6. **Process** — Business logic executes.
7. **Commit** — Offsets are stored after successful processing.
8. **Shutdown** — Consumer commits remaining offsets, closes connections, and leaves the group cleanly.

### Interview Notes

- A consumer does **not** receive partitions immediately after `subscribe()`. The assignment occurs during the **first `poll()`**.
- Rebalances can happen at any point if consumers join, leave, or partitions change.
- The lifecycle repeats until the application shuts down.

### Tricky Questions

**Q1. When are partitions actually assigned to a consumer?**
After `subscribe()`, during the **first successful `poll()`** when the consumer joins the group.

**Q2. Does consumer.close() trigger a rebalance?**
Yes. A graceful `close()` causes the consumer to leave the group, and Kafka reassigns its partitions to the remaining consumers.