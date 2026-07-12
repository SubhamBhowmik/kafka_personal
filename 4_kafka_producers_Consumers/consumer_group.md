# Kafka Consumer Groups ⭐⭐⭐⭐⭐

**Interview Importance:** Extremely High (Core Kafka Concept)

Consumer Groups are one of Kafka's most important features because they provide scalability, fault tolerance, and parallel processing.

---

## 1. Consumer Groups — Definition

A **Consumer Group** is a collection of consumers that work together to consume messages from one or more Kafka topics.

Each consumer in the group receives a subset of partitions, ensuring every message is processed **only once** within that group.

Different consumer groups can independently read the same topic.

---

## 2. Why Consumer Groups?

### Without Consumer Groups

```
Topic
  P0
   |
Consumer
```

Only one consumer processes everything.

### With Consumer Groups

```
Topic:  P0  P1  P2  P3
         |   |   |   |
Group A: C1  C2  C3  C4
```

Each consumer processes different partitions.

### Benefits

- Parallel processing
- Scalability
- Fault tolerance
- Independent applications
- Higher throughput

---

## 3. Important Rule

Inside one consumer group, **one partition can be assigned to only one consumer**.

```
Topic:  P0  P1  P2
         |   |   |
Group:  C1  C2  C3
```

No two consumers in the same group can consume P0 simultaneously.

---

## 4. Multiple Consumer Groups

Different consumer groups consume the same messages **independently**.

```
Producer
    |
Orders Topic
    |
-------------------------------
|       |         |
GroupA  GroupB   GroupC
(Analytics) (Notification) (Fraud Detection)
```

Each group maintains its **own offsets**.

---

## 5. Number of Consumers vs Partitions

### Case 1: Partitions = Consumers

```
3 Partitions, 3 Consumers
P0 → C1
P1 → C2
P2 → C3
```

Perfect utilization.

### Case 2: Consumers < Partitions

```
6 Partitions, 3 Consumers
P0, P1 → C1
P2, P3 → C2
P4, P5 → C3
```

Consumers process multiple partitions.

### Case 3: Consumers > Partitions

```
2 Partitions, 5 Consumers
P0 → C1
P1 → C2
C3 Idle
C4 Idle
C5 Idle
```

Extra consumers remain idle.

---

## 6. Consumer Group Offset

Each consumer group stores offsets **separately**.

```
Topic: Offset  0  1  2  3  4  5  6

Group A: Current Offset = 6
Group B: Current Offset = 2
```

Completely independent.

---

## 7. Consumer Group Advantages

- Parallel processing
- Automatic failover
- Independent applications
- Easy scaling
- Better throughput
- Offset isolation

---

## 8. Group Coordinator

### Definition

A **Group Coordinator** is a Kafka broker responsible for managing a consumer group.

Every consumer group has exactly **one coordinator** at any given time.

The coordinator manages:
- Consumer registration
- Heartbeats
- Partition assignment
- Rebalancing
- Offset commits (through the group protocol)

### How Coordinator Is Selected

Kafka hashes the `group.id` to determine which broker coordinates the group.

```
Group ID: payment-group
     ↓
     Hash
     ↓
Broker 2 ← becomes coordinator
```

This spreads groups across brokers.

### Responsibilities

**1. Register Consumers**

When a consumer starts:

```
Consumer → JoinGroup Request → Coordinator
```

Coordinator adds it to the group.

**2. Detect Failures**

Consumers periodically send heartbeats.

```
Heartbeat → Heartbeat → Heartbeat → No heartbeat? → Consumer considered dead
```

**3. Trigger Rebalance**

If:
- Consumer joins
- Consumer leaves
- Consumer crashes
- Partitions change

Coordinator starts a rebalance.

**4. Store Group Metadata**

Coordinator stores:
- Members
- Generation ID
- Assignment
- State

---

## 9. Group Assignment

### Definition

**Group Assignment** determines which consumer gets which partitions.

```
Topic:  P0  P1  P2  P3
         |   |   |   |
Group:  C1      C2
         |       |
        P0 P1   P2 P3
```

### Assignment Process

1. Consumers join.
2. Coordinator collects members.
3. Leader consumer (or broker-side assignor in newer protocols) computes/receives assignment depending on the assignment strategy.
4. Coordinator distributes assignments.
5. Consumers begin polling.

### Assignment Strategies

- Range
- RoundRobin
- Sticky
- CooperativeSticky

---

## 10. Load Balancing

### Definition

Load balancing means distributing partitions among consumers so that work is shared efficiently.

### Bad vs Good

| Bad | Good |
|---|---|
| C1 → 6 partitions | C1 → 3 partitions |
| C2 → 1 partition | C2 → 4 partitions |
| Unbalanced workload | Balanced workload |

### Why Needed?

Without balancing:
- One consumer overloaded
- Others idle
- Lower throughput
- Higher latency

### Kafka Goal

Kafka tries to distribute partitions as evenly as possible while respecting assignment strategy constraints.

---

## 11. Rebalancing

### Definition

**Rebalancing** is the process of redistributing partitions among consumers whenever group membership or subscribed topics change.

### When Rebalancing Happens?

- New consumer joins
- Consumer leaves
- Consumer crashes
- Session timeout
- New partitions added
- Subscription changes

### Example

```
Initially:
P0 → C1
P1 → C2

New consumer C3 joins

After rebalance:
P0 → C1
P1 → C2
P2 → C3
```

### Why Rebalancing Is Expensive?

During a traditional eager rebalance:
1. Consumers stop fetching
2. Partitions revoked
3. Assignment recalculated
4. Consumers resume

Processing pauses temporarily.

### How to Reduce Rebalances?

- Static membership
- Cooperative rebalancing
- Longer session timeout (carefully tuned)
- Avoid unnecessary consumer restarts

---

## 12. Static Membership

### Problem

Normally, a consumer restart causes **two rebalances**:

```
C1 crashes → Leaves group → Rebalance → Restart → Another rebalance
```

### Solution

Use `group.instance.id`. Kafka recognizes the restarted consumer as the same logical member.

**Result:**
- Fewer rebalances
- Faster restart
- Stable assignments

### Best Use Cases

- Kubernetes
- Docker
- Stateful applications
- Long-running services

---

## 13. Sticky Assignment

### Definition

**Sticky Assignment** attempts to:
- Balance partitions
- Move as few partitions as possible

### Example

```
Before:
C1 → P0, P1
C2 → P2, P3

New consumer joins.

Without Sticky: Everything reshuffled.
With Sticky: Only one partition moves. Less disruption.
```

### Benefits

- Better cache locality
- Less network traffic
- Faster recovery
- Fewer partition movements

---

## 14. Cooperative Rebalancing

### Problem with Traditional Rebalance

Traditional (eager) rebalance:

```
Stop Everyone → Revoke All Partitions → Reassign → Restart
```

Large pause.

### Cooperative Rebalance

Consumers release **only** the partitions that need to move.

```
Instead of moving: P0, P1, P2, P3
Move only: P3
Everything else continues processing.
```

### Benefits

- Minimal pause
- Higher availability
- Faster rebalance
- Better throughput
- Reduced latency

### Cooperative vs Eager

| Feature | Eager | Cooperative |
|---|---|---|
| Stop all consumers | Yes | No |
| Revoke all partitions | Yes | No |
| Move only changed partitions | No | Yes |
| Downtime | Higher | Lower |
| Throughput | Lower | Higher |

---

## 15. Consumer Lag ⭐⭐⭐⭐⭐

### Definition

**Consumer Lag** is the difference between:

```
Latest Offset (Log End Offset) − Committed Offset = Consumer Lag
```

### Example

```
Topic: Latest Offset = 500
Consumer committed: 450
Lag: 500 - 450 = 50
```

The consumer is 50 messages behind.

### Why Lag Happens?

- Slow processing
- Heavy business logic
- Database delays
- Downstream service latency
- Consumer crash
- Too few consumers
- Too few partitions
- Network issues
- Large message batches

### Types of Lag

**Temporary Lag**

```
Producer: 1000 msg/sec
Consumer: 800 msg/sec
```

Lag increases briefly, then catches up.

**Permanent Lag**

```
Producer: 5000/sec
Consumer: 1000/sec
```

Lag keeps growing.

### Monitoring Lag

Useful metrics include:
- Current Lag
- Log End Offset (LEO)
- Committed Offset
- Records Lag Max
- Consumer Fetch Rate
- Consumer Throughput

Common tools:
- `kafka-consumer-groups.sh --describe`
- JMX metrics
- Prometheus + Grafana
- Confluent Control Center

### How to Reduce Lag?

- Increase partitions
- Add consumers (up to the number of partitions)
- Optimize processing logic
- Increase fetch sizes where appropriate
- Tune `max.poll.records`
- Reduce unnecessary database/network calls
- Use faster storage or downstream systems
- Scale consumer instances

---

## 16. Complete Flow

```
Producer
    ↓
  Topic
    ↓
Consumer Group
    ↓
Coordinator
    ↓
Join Group
    ↓
Partition Assignment
    ↓
Consumers Start Polling
    ↓
Heartbeats
    ↓
Offset Commit
    ↓
Consumer Crash
    ↓
Coordinator Detects Failure
    ↓
Rebalance
    ↓
Consumers Resume
```

---

## 17. Interview Tips ⭐⭐⭐⭐⭐

### Key Points to Remember

- A partition can be consumed by only one consumer within the same consumer group, but different consumer groups can consume the same partition independently.
- Each consumer group maintains its own committed offsets.
- More consumers than partitions result in idle consumers.
- The Group Coordinator manages membership, heartbeats, assignments, and rebalancing.
- Static Membership reduces unnecessary rebalances during restarts.
- Sticky Assignment minimizes partition movement while balancing load.
- Cooperative Rebalancing moves only required partitions instead of stopping the entire group.
- Consumer lag is LEO − Committed Offset, and sustained lag often indicates a capacity or processing bottleneck.

### Tricky Interview Questions

**Q1. Can two consumers in the same consumer group read the same partition simultaneously?**

**Answer:** No. Within a consumer group, a partition is assigned to exactly one consumer to preserve ordering and avoid duplicate processing. However, consumers in different consumer groups can independently read the same partition.

**Q2. If you have 10 consumers and only 6 partitions, how many consumers will actually consume data?**

**Answer:** Only 6 consumers will be assigned partitions. The remaining 4 consumers will stay idle because Kafka cannot assign the same partition to multiple consumers in the same group.

**Q3. What is the difference between Sticky Assignment and Cooperative Rebalancing?**

**Answer:**

- **Sticky Assignment** is an assignment strategy that tries to keep existing partition assignments and minimize movement while balancing load.
- **Cooperative Rebalancing** is a rebalance protocol that allows consumers to continue processing unaffected partitions while only transferring the partitions that actually need to move, reducing downtime.

**Q4. Why can increasing the number of consumers fail to reduce consumer lag?**

**Answer:** Because the maximum parallelism is limited by the number of partitions. If a topic has 4 partitions, adding more than 4 consumers to the same group will not improve throughput; additional consumers will remain idle. Lag may instead require more partitions, faster processing, or optimization of downstream dependencies.