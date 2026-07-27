# Kafka Error Handling (Beginner → Advanced Interview Notes)

---

## 1. Consumer Exceptions ⭐⭐⭐⭐

### What are Consumer Exceptions?

Consumer exceptions are errors that occur while a Kafka consumer is reading or processing messages.

The exception may occur:

- While polling records
- While deserializing records
- While processing business logic
- While committing offsets

### Common Consumer Exceptions

| Exception | Cause |
|---|---|
| `DeserializationException` | Invalid message format |
| `RecordTooLargeException` | Message exceeds fetch size |
| `CommitFailedException` | Offset commit failed |
| `WakeupException` | Consumer interrupted gracefully |
| `AuthorizationException` | No permission |
| `TimeoutException` | Broker timeout |
| `KafkaException` | Generic Kafka error |

### Example Flow

```
Producer
    │
    ▼
Kafka Topic
    │
    ▼
Consumer polls records
    │
    ▼
Business Logic
    │
 ┌──┴───────────┐
 │ Success      │
 │ Offset Commit│
 └──────────────┘

 OR

Business Logic Failed
        │
        ▼
Retry / DLQ / Skip
```

### Handling Consumer Exceptions

#### 1. Retry

Retry processing a few times.

```
Attempt 1 ❌
Attempt 2 ❌
Attempt 3 ✅
```

Useful for:

- Temporary DB issue
- API timeout

#### 2. Dead Letter Topic (DLT/DLQ)

If retries fail:

```
Topic_A
   │
Consumer
   │
Processing Failed
   │
   ▼
Topic_A.DLT
```

The bad message is stored separately.

#### 3. Skip the Record

- Log the error.
- Commit offset.
- Continue processing.

Useful when one bad record should not stop the application.

#### 4. Stop Consumer

For critical failures.

**Example:**

- Database schema corrupted
- Authentication failed

Application stops until fixed.

### Spring Kafka Support

**`DefaultErrorHandler`**

Supports:

- Retry
- Backoff
- Dead Letter Publishing
- Skip record

**Example:**

```java
@Bean
DefaultErrorHandler handler() {

    return new DefaultErrorHandler(
        new DeadLetterPublishingRecoverer(kafkaTemplate),
        new FixedBackOff(1000L, 3));
}
```

**Meaning:**

- Retry 3 times
- Wait 1 second
- Then send to DLT

### Interview Questions

**Q1.** What happens if consumer throws an exception before committing offset?

> **Answer:** Offset isn't committed. Kafka redelivers the same message.

**Q2.** What happens if exception occurs after offset commit?

> **Answer:** Kafka assumes message is processed. Message is lost.

### Tricky Questions

**1.** Why should offsets be committed only after successful processing?

> **Answer:** Otherwise message loss can occur.

**2.** Can one bad message stop an entire partition?

> **Answer:** Yes. Without proper error handling, consumer repeatedly reads the same bad message and blocks all later messages in that partition.

---

## 2. Producer Exceptions ⭐⭐⭐⭐

Producer exceptions occur while sending messages to Kafka.

### Common Exceptions

| Exception | Cause |
|---|---|
| `TimeoutException` | Broker unavailable |
| `SerializationException` | Serialization failed |
| `RecordTooLargeException` | Message exceeds max size |
| `AuthorizationException` | ACL issue |
| `UnknownTopicOrPartitionException` | Topic doesn't exist |
| `NetworkException` | Network issue |

### Producer Flow

```
Application
    │
Producer
    │
Serialize
    │
Broker
```

Exception can occur at any stage.

### How Producer Handles Errors

**Retries:**

```
Broker Busy → Retry 1 → Retry 2 → Retry 3
```

Configured by:

- `retries`
- `retry.backoff.ms`

**Idempotent Producer:**

```properties
enable.idempotence=true
```

Prevents duplicate messages during retries.

**Callback:**

```java
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        // handle failure
    }
});
```

Useful for logging failures.

**Future:**

```java
producer.send(record).get();
```

- Waits until Kafka confirms.
- Throws exception immediately.

### Interview Questions

**Q1.** Can producer know whether message reached Kafka?

> **Answer:** Yes. Using callback or `Future`.

**Q2.** Should producer retry forever?

> **Answer:** No. Infinite retries may block the application.

### Tricky Questions

**1.** Can producer receive an exception even if broker eventually stores the message?

> **Answer:** Yes. If the acknowledgement times out, the producer may assume failure even though the broker successfully wrote the record. Retrying without idempotence can create duplicates.

**2.** Why is `acks=all` alone not enough to prevent duplicates?

> **Answer:** Because retries after uncertain failures can resend the same record. `enable.idempotence=true` is also needed to guarantee no duplicates from producer retries.

---

## 3. Serialization Errors ⭐⭐⭐⭐

### What is Serialization?

Serialization converts a Java object into bytes before sending it to Kafka.

```
User Object → Serializer → Bytes → Kafka
```

**Example:**

```
User → JSON → byte[]
```

### Common Reasons

- Wrong serializer (`StringSerializer` sending `User` Object)
- Missing serializer
- Invalid schema
- Custom serializer bug
- Null values

**Example:**

```
ProducerRecord<String, User>
Configured as StringSerializer
Result → SerializationException
```

### How to Handle

- Use correct serializer.
- Validate data before sending.
- Test serialization separately.
- Use callbacks to detect failures.

### Interview Questions

**Q1.** Where does serialization happen?

> **Answer:** Producer side.

**Q2.** Can serialization error reach Kafka broker?

> **Answer:** No. Message never leaves producer.

### Tricky Questions

**1.** Can retries fix serialization errors?

> **Answer:** No. Serialization errors are client-side programming/configuration issues, so retrying the same record will fail again until the problem is fixed.

**2.** Why is Schema Registry useful?

> **Answer:** It validates producer and consumer compatibility, helping prevent serialization/deserialization failures caused by schema changes.

---

## 4. Deserialization Errors ⭐⭐⭐⭐⭐

### What is Deserialization?

Converting bytes back into Java objects.

```
Kafka Bytes → Deserializer → Java Object
```

**Example:**

```
Producer sends: JSON
Consumer expects: Avro
Result → DeserializationException
```

### Common Reasons

- Wrong deserializer
- Corrupted message
- Schema mismatch
- Version mismatch
- Missing fields
- Invalid JSON

### Problem

Consumer continuously polls the same record.

```
Poll → Deserialize → Fail → Poll Again → Fail Again → Infinite Loop
```

### Solution (Spring Kafka)

Use `ErrorHandlingDeserializer`.

```
ErrorHandlingDeserializer → DefaultErrorHandler → DLT
```

Consumer skips the bad record after configured retries.

### Why ErrorHandlingDeserializer?

**Without it:**

- Deserializer throws exception
- Consumer never receives record
- No retry possible

**With it:**

- Consumer receives failed record
- Error Handler manages retry
- DLQ / Skip / Continue

### Interview Questions

**Q1.** Why is `ErrorHandlingDeserializer` important?

> **Answer:** It converts deserialization failures into records that Spring Kafka's error handler can process instead of crashing the poll loop.

**Q2.** Can deserialization error happen on producer?

> **Answer:** No. Deserialization occurs only on the consumer side.

### Tricky Questions

**1.** Why can't `DefaultErrorHandler` alone handle deserialization failures?

> **Answer:** Because the failure occurs before the consumer receives the record. `ErrorHandlingDeserializer` is required to wrap the exception so the error handler can act on it.

**2.** What happens if one poison record causes deserialization failure and no error handler is configured?

> **Answer:** The consumer repeatedly fails on the same offset, preventing progress for that partition.

---

## 5. Poison Messages ⭐⭐⭐⭐⭐

### What is a Poison Message?

A poison message is a message that always fails processing, no matter how many times it is retried.

**Examples:**

- Invalid JSON
- Corrupted bytes
- Missing mandatory fields
- Business validation failure
- Unsupported schema version

**Example:**

```
Offset 25 → Invalid JSON → Retry → Fail → Retry → Fail → Retry → Fail → DLQ
```

### Why are Poison Messages Dangerous?

Without handling:

```
Offset 10 ❌
Offset 11
Offset 12
Offset 13
```

Consumer never reaches offsets 11, 12, and 13 because Kafka preserves ordering within a partition.

### Best Practices

- Limit retries.
- Send poison messages to a Dead Letter Topic.
- Monitor DLQ volume.
- Alert on repeated failures.
- Investigate and reprocess after fixing the root cause.

### Spring Kafka Flow

```
Consumer → Retry (3) → DefaultErrorHandler → DeadLetterPublishingRecoverer → Topic.DLT
```

### Reprocessing Poison Messages

```
Topic.DLT → Fix Application/Data → DLT Consumer → Original Topic
```

### Interview Questions

**Q1.** What is the difference between a temporary failure and a poison message?

> **Answer:** Temporary failure: Can succeed after retries (e.g., database temporarily unavailable). Poison message: Always fails until the message or application is corrected.

**Q2.** Should poison messages be retried forever?

> **Answer:** No. Infinite retries waste resources and block processing of subsequent records in the same partition.

### Tricky Questions

**1.** Can a poison message affect only one partition or the entire topic?

> **Answer:** It blocks only the partition containing that message. Consumers assigned to other partitions can continue processing normally.

**2.** If a poison message is sent to a DLQ, does its original Kafka offset change?

> **Answer:** No. The original record keeps its original topic and offset. The copy written to the DLQ receives a new offset in the DLQ topic because it is a newly produced message.

---

## Interview Summary (Must Remember)

| Topic | Key Point |
|---|---|
| Consumer Exceptions | Retry, skip, or send failed records to a DLT; commit offsets only after successful processing. |
| Producer Exceptions | Handle send failures with callbacks, retries, and idempotence to avoid duplicates. |
| Serialization Errors | Occur on the producer before the message reaches Kafka. |
| Deserialization Errors | Occur on the consumer; use `ErrorHandlingDeserializer` with `DefaultErrorHandler` in Spring Kafka. |
| Poison Messages | Permanently failing records should be isolated in a DLQ after limited retries to avoid blocking a partition. |

---

## Deep Dive: DLQ Offset Behavior

> If a poison message is sent to a DLQ, does its original Kafka offset change?

**Short Answer:** No. The original Kafka offset never changes.

When a message is sent to a Dead Letter Queue (DLQ), Kafka does **not** move the original message. Instead, it creates a **new copy** of that message in the DLQ topic.

### Example

Suppose we have this topic:

**Original Topic: `orders`**

| Offset | Message |
|---|---|
| 0 | Order-101 |
| 1 | Order-102 |
| 2 | Order-103 ❌ (Poison Message) |
| 3 | Order-104 |
| 4 | Order-105 |

Consumer reads:

- Offset 0 → Success ✅
- Offset 1 → Success ✅
- Offset 2 → Failed ❌

After 3 retries, the consumer decides: *"I can't process this message anymore. I'll send it to the DLQ."*

### What Actually Happens?

**Step 1: Original message remains unchanged**

The `orders` topic is still:

| Offset | Message |
|---|---|
| 0 | Order-101 |
| 1 | Order-102 |
| 2 | Order-103 ❌ |
| 3 | Order-104 |
| 4 | Order-105 |

Notice:

- Offset 2 is still Offset 2.
- Kafka never modifies offsets.

**Step 2: Producer writes to DLQ**

A new record is produced to another topic.

```
orders
   │
   │  Copy Message
   ▼
orders.DLT
```

**DLQ topic:**

| Offset | Message |
|---|---|
| 0 | Order-103 ❌ |

If another poison message comes later:

| Offset | Message |
|---|---|
| 0 | Order-103 |
| 1 | Order-220 |
| 2 | Order-555 |

These are new offsets belonging to the DLQ topic, not the original topic.

### Why Does the DLQ Get a New Offset?

Because Kafka treats the DLQ as a completely different topic.

Think of it like sending a new email.

- **Original Inbox:** Email #25
- You forward it to another mailbox.
- **Archive Mailbox:** Email #1

The email content is the same, but in the new mailbox it gets a new number. Kafka works exactly the same way.

### Visual Diagram

```
Original Topic (orders)

Offset 0  → Order-101
Offset 1  → Order-102
Offset 2  → Order-103 ❌
Offset 3  → Order-104

          │
          │ Copy (Produce)
          ▼

DLQ Topic (orders.DLT)

Offset 0  → Order-103 ❌
```

Notice:

- Original topic still has Offset 2
- DLQ has its own Offset 0
- They are two independent topics.

### What Happens to the Consumer Offset?

After writing the failed message to the DLQ, the consumer usually commits Offset 2.

So the next poll starts from:

```
Offset 3
```

Instead of reading Offset 2 again. This prevents the poison message from blocking the rest of the partition.

### Interview Question

**Q:** When a message is moved to a DLQ, is it removed from the original topic?

> **Answer:** No. Kafka messages are immutable. The original record remains in the original topic with the same offset until it expires based on the topic's retention policy. The DLQ contains a new copy of that record with a new offset in the DLQ topic.

### Easy Way to Remember

Kafka never moves messages — it only appends new ones.

So when a message goes to a DLQ:

- ✅ Original message stays in the original topic.
- ✅ Original offset stays the same.
- ✅ A copy is produced to the DLQ.
- ✅ The DLQ assigns a new offset because it is a different topic.

---

## Topic: Retry

### Subtopics

- Retry Strategies
- Fixed Delay
- Exponential Backoff
- Retry Topics
- Blocking Retry
- Non-blocking Retry

---

## Topic: Dead Letter Queue

### Subtopics

- DLQ
- Dead Letter Topic
- Parking Lot Queue
- Manual Replay
- Reprocessing
- Offset Handling
- Recoverers

---

# Kafka Retry (Beginner → Advanced Interview Notes)

## 1. Retry Strategies ⭐⭐⭐⭐⭐

### What is Retry?

Retry means trying to process the same message again after a failure.

Useful for **temporary (transient) failures**, such as:

- Database temporarily down
- External API timeout
- Network issue
- Service unavailable

**Not useful for:**

- Invalid JSON
- Wrong schema
- Missing required fields
- Business validation failures (Poison Messages)

### Retry Flow

```
Consumer
   │
Process Message
   │
 ┌─┴─────────────┐
 │ Success ✅     │
 └───────────────┘

 OR

Failure ❌
   │
Retry
   │
Retry
   │
Retry
   │
DLQ
```

### Best Practices

- Retry only for temporary failures.
- Limit retry attempts.
- Use exponential backoff.
- Send permanently failing messages to DLQ.

### Interview Questions

**Q1.** When should we retry?

> **Answer:** Only for temporary failures.

**Q2.** Should we retry deserialization errors?

> **Answer:** No. Because retrying won't fix invalid data.

### Tricky Questions

**1.** Can unlimited retries be dangerous?

> **Answer:** Yes. One bad message can block the partition forever.

**2.** Can retry create duplicate processing?

> **Answer:** Yes. If processing succeeds but offset isn't committed before a crash.

---

## 2. Fixed Delay ⭐⭐⭐

### What is Fixed Delay?

Every retry waits the same amount of time.

**Example:**

```
Attempt 1 → Wait 2 sec → Attempt 2 → Wait 2 sec → Attempt 3 → Wait 2 sec
```

### Spring Kafka

```java
new FixedBackOff(2000L, 3);
```

**Meaning:**

- Wait 2 seconds
- Retry 3 times

### Advantages

- Simple
- Predictable

### Disadvantages

- Can overload a failing service.

### Interview Questions

**Q1.** When is Fixed Delay useful?

> **Answer:** For short-lived temporary failures.

**Q2.** What is the drawback?

> **Answer:** Constant retry rate may overwhelm a slow service.

### Tricky Questions

**1.** If the service takes 30 seconds to recover, is a 2-second fixed delay ideal?

> **Answer:** No. You'll waste retries before the service is available.

**2.** Does Fixed Delay change after each retry?

> **Answer:** No. The delay remains constant.

---

## 3. Exponential Backoff ⭐⭐⭐⭐⭐

### What is Exponential Backoff?

Retry delay increases after every failure.

**Example:**

```
Retry 1 → 1 sec
Retry 2 → 2 sec
Retry 3 → 4 sec
Retry 4 → 8 sec
```

### Why Use It?

It gives downstream systems time to recover.

### Comparison

| Retry | Fixed Delay | Exponential |
|---|---|---|
| 1 | 2 sec | 1 sec |
| 2 | 2 sec | 2 sec |
| 3 | 2 sec | 4 sec |
| 4 | 2 sec | 8 sec |

### Advantages

- Prevents retry storms.
- Better for distributed systems.

### Interview Questions

**Q1.** Why is exponential backoff preferred?

> **Answer:** It reduces load on failing services.

**Q2.** Where is it commonly used?

> **Answer:** Kafka, REST APIs, cloud services, microservices.

### Tricky Questions

**1.** Can exponential backoff retry forever?

> **Answer:** Yes, if configured incorrectly. Always set a maximum number of retries or a maximum delay.

**2.** Can exponential backoff solve poison messages?

> **Answer:** No. It only delays retries. Invalid data will still fail.

---

## 4. Retry Topics ⭐⭐⭐⭐⭐

### What are Retry Topics?

Instead of retrying immediately, failed messages are sent to another Kafka topic dedicated to retries.

**Example:**

```
orders → Failure → orders.retry → Retry Later → orders → Success
```

### Spring Kafka Flow

```
orders → orders-retry-5s → orders-retry-30s → orders-retry-1m → orders.DLT
```

### Why Use Retry Topics?

- Prevent blocking the main consumer.
- Delay retries.
- Improve scalability.

### Interview Questions

**Q1.** Why use retry topics instead of immediate retries?

> **Answer:** They prevent the main consumer from waiting during retry delays.

**Q2.** Can retry topics have different delay intervals?

> **Answer:** Yes. Each retry topic can represent a different delay (5s, 30s, 1m, etc.).

### Tricky Questions

**1.** Does the retry topic preserve the original Kafka offset?

> **Answer:** No. The retry topic receives a new message with a new offset because it's a different topic.

**2.** Can retry topics preserve message order?

> **Answer:** Not necessarily. Delayed retries may allow later messages to be processed before the retried one.

---

## 5. Blocking Retry ⭐⭐⭐⭐

### What is Blocking Retry?

The consumer waits before retrying the same message.

**Example:**

```
Offset 10 ❌ → Wait → Retry → Wait → Retry → Success
```

During this time: Offsets 11, 12, 13 are **not** processed.

### Advantages

- Simple
- Easy configuration

### Disadvantages

- Blocks the partition.
- Lower throughput.

### Interview Questions

**Q1.** Why is it called blocking?

> **Answer:** The consumer thread is occupied retrying the failed message.

**Q2.** Does blocking retry affect only one partition?

> **Answer:** Yes. Other partitions assigned to other consumer threads may continue.

### Tricky Questions

**1.** Can blocking retry increase consumer lag?

> **Answer:** Yes. Because processing stops while retries are happening.

**2.** Is blocking retry suitable for long retry delays?

> **Answer:** No. Retry topics or non-blocking retries are usually better.

---

## 6. Non-blocking Retry ⭐⭐⭐⭐⭐

### What is Non-blocking Retry?

Instead of waiting, the consumer immediately forwards the failed message to a retry topic.

**Example:**

```
orders → Failure → orders.retry → Consumer continues → Next Message
```

### Flow

```
Offset 10 ❌ → Send to Retry Topic → Commit Offset 10 → Continue with Offset 11
```

No waiting.

### Advantages

- High throughput.
- Doesn't block partitions.
- Better scalability.

### Interview Questions

**Q1.** Why is non-blocking retry faster?

> **Answer:** The consumer immediately continues processing other messages.

**Q2.** Which Spring Kafka feature supports it?

> **Answer:** `@RetryableTopic`.

### Tricky Questions

**1.** Can non-blocking retry affect ordering?

> **Answer:** Yes. Later messages may complete before the retried message.

**2.** When should you avoid non-blocking retries?

> **Answer:** When strict ordering within a partition is a business requirement.

---

# Kafka Dead Letter Queue (DLQ)

## 1. DLQ (Dead Letter Queue) ⭐⭐⭐⭐⭐

### What is DLQ?

A DLQ is a place where permanently failing messages are stored after all retry attempts are exhausted.

```
Consumer → Retry → Retry → Retry → DLQ
```

### Why Use a DLQ?

**Without a DLQ:** One poison message can block an entire partition.

**With a DLQ:** The bad message is isolated and processing continues.

### Interview Questions

**Q1.** When should a message be sent to the DLQ?

> **Answer:** After all configured retries fail.

**Q2.** Should every failed message go directly to the DLQ?

> **Answer:** No. Temporary failures should usually be retried first.

### Tricky Questions

**1.** Does Kafka create a DLQ automatically?

> **Answer:** No. You must configure a DLQ topic and the logic to publish failed records.

**2.** Does moving a message to the DLQ delete it from the original topic?

> **Answer:** No. Kafka creates a copy in the DLQ. The original record remains until retention removes it.

---

## 2. Dead Letter Topic (DLT)

### What is a DLT?

A Dead Letter Topic is simply a Kafka topic that stores failed messages.

**Example:**

```
orders → orders.DLT
```

- **DLQ** is the concept.
- **DLT** is the Kafka topic implementing that concept.

### Spring Kafka

```
orders → orders.DLT
```

Created automatically when using Spring's retry topic support (if configured).

### Interview Questions

**Q1.** Difference between DLQ and DLT?

> **Answer:** DLQ → Concept. DLT → Actual Kafka topic.

**Q2.** Can multiple applications have separate DLTs?

> **Answer:** Yes. Each application or consumer group can have its own DLT strategy.

### Tricky Questions

**1.** Can one DLT store messages from multiple topics?

> **Answer:** Yes, but separate DLTs are generally easier to monitor and replay.

**2.** Does the DLT keep the original message headers?

> **Answer:** Usually yes. Spring Kafka adds useful metadata such as the original topic, partition, offset, exception type, and stack trace.

---

## 3. Parking Lot Queue ⭐⭐⭐⭐

### What is a Parking Lot Queue?

A Parking Lot Queue is a final holding area for messages that even fail after DLQ reprocessing.

### Flow

```
Main Topic → DLQ → Replay → Still Failed → Parking Lot Queue
```

### Why?

Prevents endlessly retrying hopeless messages while preserving them for manual investigation.

### Interview Questions

**Q1.** Why not keep retrying forever?

> **Answer:** Because it wastes resources and blocks operations.

**Q2.** Who usually handles Parking Lot Queue messages?

> **Answer:** Support or operations teams after investigation.

### Tricky Questions

**1.** Is a Parking Lot Queue mandatory?

> **Answer:** No. It's an architectural pattern, not a Kafka feature.

**2.** Can messages be replayed from a Parking Lot Queue?

> **Answer:** Yes, after fixing the underlying issue.

---

## 4. Manual Replay ⭐⭐⭐⭐

### What is Manual Replay?

Reading messages from the DLQ and publishing them back to the original topic after the issue is fixed.

```
orders.DLT → Replay Tool → orders
```

### Interview Questions

**Q1.** When should manual replay be performed?

> **Answer:** After the root cause has been resolved.

**Q2.** Can replay create duplicates?

> **Answer:** Yes. If the original message was actually processed before the failure was detected.

### Tricky Questions

**1.** Should replay preserve the original offset?

> **Answer:** No. Republishing creates a new Kafka record with a new offset.

**2.** Should all DLQ messages be replayed?

> **Answer:** No. Some messages may contain permanently invalid data and should not be replayed.

---

## 5. Reprocessing ⭐⭐⭐⭐

### What is Reprocessing?

Processing failed messages again after fixing the application or data.

### Flow

```
Fix Bug → Replay DLQ → Consumer Processes Again
```

### Best Practices

- Verify the fix first.
- Replay in batches.
- Monitor results.
- Ensure idempotent processing if duplicates are possible.

### Interview Questions

**Q1.** Why is idempotency important during reprocessing?

> **Answer:** It prevents duplicate side effects if a message is processed more than once.

**Q2.** Should reprocessing happen automatically?

> **Answer:** It depends on business requirements. Critical systems often prefer manual approval before replay.

### Tricky Questions

**1.** Can reprocessing change the Kafka offset?

> **Answer:** Yes. Republished messages receive new offsets.

**2.** Can reprocessing fail again?

> **Answer:** Yes. If the underlying issue wasn't completely fixed.

---

## 6. Offset Handling ⭐⭐⭐⭐⭐

### What Happens After Sending a Message to the DLQ?

Typical flow:

```
Offset 25 → Retry Failed → Publish to DLT → Commit Offset 25 → Continue with Offset 26
```

### Why Commit the Offset?

To avoid reading the same poison message repeatedly.

### Interview Questions

**Q1.** Why commit the offset after publishing to the DLT?

> **Answer:** Because the failed message has been handled, allowing the consumer to continue.

**Q2.** What happens if the offset isn't committed?

> **Answer:** The consumer reads the same failed message again after restart or rebalance.

### Tricky Questions

**1.** Should the offset be committed before publishing to the DLT?

> **Answer:** No. If publishing to the DLT fails after the offset is committed, the message may be lost from processing.

**2.** What is the safer order?

> **Answer:**
> 1. Publish to the DLT successfully.
> 2. Commit the original topic's offset.
> 3. Continue processing.

---

## 7. Recoverers ⭐⭐⭐⭐⭐

### What is a Recoverer?

A Recoverer decides what to do after retries are exhausted.

**Examples:**

- Send to DLT
- Log the error
- Ignore the record
- Execute custom recovery logic

### Spring Kafka Example

```java
@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(template);

    return new DefaultErrorHandler(
            recoverer,
            new FixedBackOff(1000L, 3)
    );
}
```

### Flow

```
Retry → Retry → Retry → Recoverer → DLT
```

### Common Recoverers

| Recoverer | Purpose |
|---|---|
| `DeadLetterPublishingRecoverer` | Publishes failed messages to a DLT |
| Logging Recoverer | Logs the error and skips the record |
| Custom Recoverer | Executes custom business recovery logic |

### Interview Questions

**Q1.** What is the role of `DeadLetterPublishingRecoverer`?

> **Answer:** It publishes failed records to a Dead Letter Topic after retries are exhausted.

**Q2.** Does a Recoverer retry the message?

> **Answer:** No. Retries are handled by the error handler. The Recoverer executes after retry attempts are finished.

### Tricky Questions

**1.** Can you implement your own Recoverer?

> **Answer:** Yes. You can provide custom logic, such as storing failed messages in a database, sending alerts, or invoking another service.

**2.** What is the relationship between `DefaultErrorHandler` and `DeadLetterPublishingRecoverer`?

> **Answer:** `DefaultErrorHandler` decides how many times to retry and when to stop retrying. `DeadLetterPublishingRecoverer` decides what to do next after retries are exhausted (typically publish the record to a DLT).

---

## Quick Interview Cheat Sheet

| Topic | One-line Summary |
|---|---|
| Retry Strategies | Retry only transient failures; send permanent failures to a DLT. |
| Fixed Delay | Same wait time between every retry. |
| Exponential Backoff | Wait time increases after each retry to reduce system load. |
| Retry Topics | Retry using separate Kafka topics instead of blocking the consumer. |
| Blocking Retry | Consumer waits and retries, blocking progress on that partition. |
| Non-blocking Retry | Failed records go to retry topics so the consumer can continue immediately. |
| DLQ | Logical concept for storing permanently failed messages. |
| Dead Letter Topic | The actual Kafka topic that implements the DLQ. |
| Parking Lot Queue | Final holding area for messages that fail even after replay. |
| Manual Replay | Republishing DLT messages after fixing the root cause. |
| Reprocessing | Processing failed messages again after a fix. |
| Offset Handling | Publish to the DLT first, then commit the original offset. |
| Recoverers | Decide the final action once all retries are exhausted. |