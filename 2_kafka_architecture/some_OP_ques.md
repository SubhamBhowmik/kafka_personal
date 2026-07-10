# Kafka Architecture — Key Concepts & Interview Questions

---

## 1. High Watermark (HW)

### Definition

**High Watermark (HW)** is the highest offset that has been successfully replicated to all In-Sync Replicas (ISR). Consumers can only read messages up to the High Watermark.

### Why is High Watermark Needed?

**Scenario:**

```
Leader                    Follower
Offset                    Offset
 0 → A                     0 → A
 1 → B                     1 → B
 2 → C                     2 → C
 3 → D
 4 → E
```

The follower has **not** copied D and E yet. If consumers read D and E now and the leader crashes, the new leader (the follower) doesn't have D and E.

**Result:**
- Consumer already saw D and E.
- Those messages are lost.
- Inconsistent data.

### High Watermark Solution

Kafka exposes only the messages that are **safely replicated** to all ISR replicas.

```
Leader
 0 → A
 1 → B
 2 → C   ← High Watermark
 3 → D
 4 → E
```

Consumers can read only: **A, B, C**

After the follower copies D:

```
Leader
 0 → A
 1 → B
 2 → C
 3 → D   ← High Watermark
 4 → E
```

Now consumers can also read D.

### Flow

```
Producer writes message
        │
        ▼
Leader stores message
        │
        ▼
Followers replicate
        │
        ▼
High Watermark moves
        │
        ▼
Consumers can read
```

### Real-life Analogy

Imagine writing an important document:
1. Save the document.
2. Make a backup.
3. Share it.

Kafka follows the same approach:
- **Leader** = Original document
- **Followers** = Backup copies
- **High Watermark** = "Safe to share"

### Interview Definition

> **High Watermark** is the highest replicated offset among all ISR replicas. Consumers only read up to the High Watermark, ensuring they never see data that could be lost if the leader fails.

---

## 2. Why KRaft Replaced ZooKeeper

Earlier Kafka relied on **ZooKeeper** for cluster coordination.

### Old Architecture (ZooKeeper)

```
          ZooKeeper
              ▲
              │
   ┌─────────────────────┐
   │         │           │
Broker1   Broker2    Broker3
```

**Problems:**
- Two distributed systems
- Extra installation
- Extra maintenance
- More network communication
- Operational complexity

### KRaft Architecture

```
      KRaft Controller Quorum

      Controller1
      Controller2
      Controller3

           ▲
           │
   ┌─────────────────────┐
   │         │           │
Broker1   Broker2    Broker3
```

No external ZooKeeper is required.

### Advantages of KRaft

**1. Simpler Architecture**
- Before: Kafka + ZooKeeper
- Now: Kafka only

**2. Easier Deployment**
- ZooKeeper: Install → Configure → Maintain
- KRaft: Install Kafka → Start Kafka

**3. Faster Metadata Operations**
Metadata changes happen directly within Kafka without involving an external service.

**4. Better Scalability**
Designed to handle thousands of brokers and millions of partitions more efficiently than ZooKeeper.

**5. Uses Raft Consensus**
- ZooKeeper: Uses ZAB protocol
- KRaft: Uses the Raft consensus algorithm (simpler and integrated into Kafka)

**6. Easier Upgrades**
- Old: Upgrade ZooKeeper → Upgrade Kafka
- New: Upgrade Kafka

**7. Lower Resource Usage**
No separate ZooKeeper cluster means fewer machines, less CPU, less memory, and lower operational cost.

**8. Better Failure Handling**
Controllers replicate metadata. If the leader controller fails:

```
Controller1 (Leader)
    │
    ▼ (Fails)
    │
Controller2 becomes Leader
```

### ZooKeeper vs KRaft

| ZooKeeper | KRaft |
|-----------|-------|
| External service | Built into Kafka |
| Two systems | One system |
| More maintenance | Less maintenance |
| ZAB protocol | Raft protocol |
| Older architecture | Modern architecture |

### Interview Answer

> KRaft removes the dependency on ZooKeeper by integrating metadata management directly into Kafka. It uses a controller quorum with the Raft consensus algorithm, resulting in simpler deployment, lower operational overhead, better scalability, and improved metadata management.

---

## 3. How KRaft Manages Metadata Without ZooKeeper

### Earlier (ZooKeeper)

```
              ZooKeeper
       Stores all metadata
              ▲
              │
     ┌─────────────────────┐
     │         │           │
  Broker1   Broker2    Broker3
```

ZooKeeper stored:
- Broker information
- Topic information
- Partition information
- ISR
- Leader election
- Controller election

### Now (KRaft)

Kafka itself runs a **Controller Quorum**.

```
      Controller1
      Controller2
      Controller3

           ▲
           │
     ┌─────────────────────┐
     │         │           │
  Broker1   Broker2    Broker3
```

These controllers are **Kafka processes** — no external software is needed.

### Metadata Log

Kafka stores cluster metadata inside a special **metadata log**. Unlike normal topic logs, it stores cluster state.

**Example entries:**
- Create Topic Orders
- Create Partition 0
- Broker Joined
- Leader Changed
- Delete Topic

### Who Writes the Metadata Log?

Only the **Leader Controller** writes metadata.

```
Controller1 (Leader)
    │
    ▼ (Writes)
Create Topic Orders
    │
    ▼ (Replicates via Raft)
Controller2
Controller3
```

### Why Multiple Controllers?

Exactly like partition replication:

```
Controller1 (Leader)
Controller2 (Follower)
Controller3 (Follower)
```

If Controller1 fails → Controller2 becomes Leader → Metadata remains available.

### Topic Creation Flow

**Step 1:** Client sends `Create Topic Orders` → Leader Controller receives the request.

**Step 2:** Leader Controller writes the metadata (`Create Topic Orders`) into the metadata log.

**Step 3:** Metadata is replicated to follower controllers via Raft.

```
Controller1 → Controller2 → Controller3
```

**Step 4:** All brokers receive the updated metadata.

```
Broker1, Broker2, Broker3 → New Topic Available
```

### Leader Election in KRaft

Suppose Broker2 crashes.

**Earlier (ZooKeeper):**
```
Broker2 crashes → ZooKeeper detects failure → Elects new leader
```

**Now (KRaft):**
```
Broker2 crashes
    │
    ▼
Leader Controller detects failure
    │
    ▼
Elects new partition leader
    │
    ▼
Updates metadata log
    │
    ▼
Notifies all brokers
```

### Simple Analogy

**Old Architecture:**
- Kafka = Employees
- ZooKeeper = Manager
- Employees always ask the manager.

**New Architecture:**
- Kafka = Employees
- One employee becomes the manager (Leader Controller).
- If that manager leaves, another employee automatically becomes the manager.
- **No external manager is required.**

---

## Final Interview Summary

### High Watermark
- Highest replicated offset among ISR.
- Consumers read only up to HW.
- Prevents reading messages that could be lost after a leader failure.

### KRaft
- Replaces ZooKeeper.
- Uses an internal controller quorum.
- Metadata is stored in a replicated metadata log.
- Uses the Raft consensus algorithm.
- Simpler deployment, lower maintenance, and better scalability.

### KRaft Metadata Flow

```
Client
    │
    ▼
Leader Controller
    │
    ▼
Metadata Log
    │
    ▼
Replicate via Raft
    │
    ▼
Follower Controllers
    │
    ▼
All Brokers Updated