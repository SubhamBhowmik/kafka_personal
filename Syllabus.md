# Kafka Syllabus 📘

---

## Table of Contents

| # | Topic | Importance |
|---|-------|------------|
| [1](#1-kafka-fundamentals--architecture-) | Kafka Fundamentals & Architecture | ⭐⭐⭐⭐⭐ |
| [2](#2-topics-partitions--offsets-) | Topics, Partitions & Offsets | ⭐⭐⭐⭐⭐ |
| [3](#3-producers--consumers-) | Producers & Consumers | ⭐⭐⭐⭐⭐ |
| [4](#4-reliability-replication--delivery-guarantees-) | Reliability, Replication & Delivery Guarantees | ⭐⭐⭐⭐⭐ |
| [5](#5-spring-boot-kafka-) | Spring Boot Kafka | ⭐⭐⭐⭐⭐ |
| [6](#6-error-handling-retry--dlq-) | Error Handling, Retry & DLQ | ⭐⭐⭐⭐⭐ |
| [7](#7-performance-monitoring--security-) | Performance, Monitoring & Security | ⭐⭐⭐⭐☆ |
| [8](#8-kafka-ecosystem--advanced-features-) | Kafka Ecosystem & Advanced Features | ⭐⭐⭐⭐☆ |
| [9](#9-production--system-design-) | Production & System Design | ⭐⭐⭐⭐⭐ |
| [10](#10-interview--real-world-scenarios-) | Interview & Real-World Scenarios | ⭐⭐⭐⭐⭐ |

---

## [1](https://www.geeksforgeeks.org/apache-kafka/apache-kafka/). Kafka Fundamentals & Architecture ⭐⭐⭐⭐⭐

### 1.1 Introduction
- What is Apache Kafka?
- Why Kafka?
- Problems Kafka solves
- Event Streaming
- Messaging Queue vs Event Streaming
- Pub/Sub Model
- Kafka Use Cases

### 1.2 Messaging Concepts
- Synchronous vs Asynchronous Communication
- Producer
- Consumer
- Broker
- Topic
- Partition
- Offset
- Consumer Group
- Event
- Message
- Event-Driven Architecture

### 1.3 Kafka Architecture
- Broker
- Cluster
- Leader
- Follower
- ISR
- Controller
- Metadata
- High Watermark
- Log Storage
- Segment Files
- KRaft
- ZooKeeper
- Leader Election

---

## 2. Topics, Partitions & Offsets ⭐⭐⭐⭐⭐

### Topics
- Topic Creation
- Topic Configuration
- Internal Topics
- Topic Naming
- Topic Retention
- Topic Compaction
- Topic Deletion

### Partitions
- Why Partitions?
- Partition Strategy
- Parallelism
- Ordering
- Partition Key
- Round Robin
- Sticky Partitioner
- Custom Partitioner
- Increasing Partitions
- Partition Reassignment

### Offsets
- Offset
- Offset Storage
- Current Offset
- Committed Offset
- End Offset
- Offset Reset
- Auto Commit
- Manual Commit
- Offset Replay
- Offset Retention

---

## 3. Producers & Consumers ⭐⭐⭐⭐⭐

### Producer
- Producer Architecture
- KafkaProducer API
- Record Metadata
- Serialization
- Compression
- Batching
- Linger.ms
- Batch Size
- Buffer Memory
- Retries
- Acknowledgements (acks)
- Idempotent Producer
- Transactions
- Producer Interceptors

### Consumer
- Consumer Architecture
- KafkaConsumer API
- Poll Loop
- Fetch Requests
- Heartbeats
- Session Timeout
- Max Poll Interval
- Auto Commit
- Manual Commit
- Graceful Shutdown
- Consumer Lifecycle

### Consumer Groups
- Consumer Group
- Group Coordinator
- Group Assignment
- Load Balancing
- Rebalancing
- Static Membership
- Sticky Assignment
- Cooperative Rebalancing
- Consumer Lag

---

## 4. Reliability, Replication & Delivery Guarantees ⭐⭐⭐⭐⭐

### Replication
- Replication Factor
- Leader Replica
- Follower Replica
- ISR
- Replica Synchronization
- Preferred Leader Election
- Unclean Leader Election

### Delivery Semantics
- At Most Once
- At Least Once
- Exactly Once
- Duplicate Messages
- Message Loss

### Reliability
- Idempotence
- Transactions
- Read Committed
- Read Uncommitted
- Ordering Guarantees
- High Watermark
- Fault Tolerance

---

## 5. Spring Boot Kafka ⭐⭐⭐⭐⭐

### Spring Kafka
- Spring Kafka Architecture
- KafkaTemplate
- KafkaAdmin
- Producer Configuration
- Consumer Configuration
- Kafka Listener
- @KafkaListener
- Listener Container
- Concurrent Listeners
- Batch Listener
- Manual Acknowledgement
- Message Headers
- Message Conversion

### Spring Features
- JSON Serialization
- Custom Serializer
- Custom Deserializer
- Transactions
- Error Handling
- Retry
- Dead Letter Topic
- Record Filtering

---

## 6. Error Handling, Retry & DLQ ⭐⭐⭐⭐⭐

### Error Handling
- Consumer Exceptions
- Producer Exceptions
- Serialization Errors
- Deserialization Errors
- Poison Messages

### Retry
- Retry Strategies
- Fixed Delay
- Exponential Backoff
- Retry Topics
- Blocking Retry
- Non-blocking Retry

### Dead Letter Queue
- DLQ
- Dead Letter Topic
- Parking Lot Queue
- Manual Replay
- Reprocessing
- Offset Handling
- Recoverers

---

## 7. Performance, Monitoring & Security ⭐⭐⭐⭐☆

### Performance
- Batch Size
- Linger.ms
- Compression
- Fetch Size
- Producer Throughput
- Consumer Throughput
- JVM Tuning
- Disk Optimization
- Network Optimization

### Monitoring
- Consumer Lag
- JMX Metrics
- Broker Metrics
- Producer Metrics
- Prometheus
- Grafana
- Burrow
- Cruise Control

### Security
- SSL
- TLS
- SASL
- SCRAM
- Kerberos
- OAuth
- ACL
- Authentication
- Authorization
- Encryption

---

## 8. Kafka Ecosystem & Advanced Features ⭐⭐⭐⭐☆

### Serialization
- JSON
- Avro
- Protobuf
- Custom Serializer
- Schema Evolution

### Schema Registry
- Schema Registry
- Versioning
- Compatibility
- Subject Naming

### Kafka Connect
- Source Connectors
- Sink Connectors
- JDBC
- MongoDB
- Elasticsearch
- S3
- SMT

### Kafka Streams
- KStream
- KTable
- GlobalKTable
- Stateless Processing
- Stateful Processing
- Windowing
- Aggregations
- Joins
- State Store

### Admin & CLI
- Admin API
- Topic Management
- Broker Management
- Kafka CLI Commands

---

## 9. Production & System Design ⭐⭐⭐⭐⭐

### Production Deployment
- Single Broker vs Cluster
- Scaling Brokers
- Scaling Consumers
- Scaling Producers
- Partition Expansion
- Rack Awareness
- Capacity Planning
- High Availability
- Broker Failure
- Consumer Failure
- Producer Failure
- Controller Failure
- Network Failure
- Disaster Recovery

### Design Patterns
- Event Sourcing
- CQRS
- Saga Pattern
- Outbox Pattern
- Inbox Pattern
- CDC
- Request-Reply
- Event Versioning
- Idempotency

### Kafka in Microservices
- Event-Driven Microservices
- Async Communication
- Service Decoupling
- Distributed Transactions
- Event Choreography
- Event Orchestration

---

## 10. Interview & Real-World Scenarios ⭐⭐⭐⭐⭐

### Frequently Asked Concepts
- Kafka Internal Working
- End-to-End Message Flow
- Offset Commit Scenarios
- Consumer Lag Analysis
- Rebalancing Scenarios
- Ordering Scenarios
- Exactly Once Processing
- Transactions
- Replication
- Leader Election

### Troubleshooting
- High Consumer Lag
- Duplicate Messages
- Missing Messages
- Slow Producer
- Slow Consumer
- Broker Down
- ISR Shrinking
- Rebalance Storms
- Disk Full
- Network Latency
- Out of Memory
- Serialization Failures

### System Design
- Order Processing System
- Payment System
- Notification Service
- Inventory System
- Banking Transactions
- Fraud Detection
- Analytics Pipeline
- Log Aggregation
- Clickstream Processing
- IoT Streaming

### Hands-on
- Kafka Producer & Consumer
- Spring Boot Integration
- Retry & DLQ Implementation
- Manual Offset Commit
- Transactions
- Kafka Connect Demo
- Kafka Streams Demo
- Monitoring Setup

---

## Interview Priority

| # | Topic | Importance |
|---|-------|------------|
| 1 | Kafka Fundamentals & Architecture | ⭐⭐⭐⭐⭐ |
| 2 | Topics, Partitions & Offsets | ⭐⭐⭐⭐⭐ |
| 3 | Producers & Consumers | ⭐⭐⭐⭐⭐ |
| 4 | Reliability & Replication | ⭐⭐⭐⭐⭐ |
| 5 | Spring Boot Kafka | ⭐⭐⭐⭐⭐ |
| 6 | Error Handling, Retry & DLQ | ⭐⭐⭐⭐⭐ |
| 7 | Performance, Monitoring & Security | ⭐⭐⭐⭐☆ |
| 8 | Kafka Ecosystem & Advanced Features | ⭐⭐⭐⭐☆ |
| 9 | Production & System Design | ⭐⭐⭐⭐⭐ |
| 10 | Interview & Real-World Scenarios | ⭐⭐⭐⭐⭐ |