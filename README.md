# Kafka-like Distributed System Implementation

This project implements core functionalities of a Kafka-like distributed system, focusing on topic management and controller election mechanisms.

## Implementation Tasks

### 1. Replica Assignment Testing
Enhance the `ReplicaAssignerTest` to:
- Test dynamic partition count changes
- Verify replica distribution remains balanced when partition count changes
- Ensure replica assignment rules are maintained with different partition counts

Current test coverage includes:
- Correct number of partitions and replicas
- Replicas on different brokers
- Even distribution of replicas

### 2. Topic Change Detection
Complete the `detectNewTopicCreation` test in `TopicChangeListenerTest`:
- Implement test cases for new topic creation detection
- Verify callback mechanisms work correctly
- Ensure proper handling of topic metadata changes
- Test the listener's response to ZooKeeper events

### 3. Controller Election Implementation
Complete the `tryCreatingControllerPath` method to:
- Implement atomic controller election using ZooKeeper
- Handle race conditions in controller election
- Manage controller state transitions
- Ensure proper error handling


### 4. Log Implementation Testing
Complete the `LogTest` implementation:
- Implement log append and read functionality
- Test log segment creation and management
- Verify log entry persistence
- Test log compaction if implemented

Key test requirements:
- Test log entry writing and reading
- Verify log segment boundaries
- Test log file handling
- Ensure proper error handling for I/O operations

The current `ZkController.java` provides the framework with:
- Controller election structure
- ZooKeeper event handling
- Broker state management

### Partition and Replica Assignment
Implement logic for:
- Distributing partitions across available brokers evenly
- Assigning replicas to different brokers for fault tolerance
- Ensuring replicas are spread across different brokers
- Maintaining balanced load across brokers

Key considerations:
- Each partition should have a leader and followers
- Replicas should be distributed across different brokers for fault tolerance
- Implement a round-robin or similar algorithm for balanced distribution

### ZooKeeper Persistence
Stores topic and partition information in ZooKeeper:
- Create persistent nodes for topics
- Store partition assignments
- Store replica assignments
- Maintain broker-topic-partition mapping

ZooKeeper path structure: 
brokers/topics/[topic_name]
/brokers/topics/[topic_name]/partitions/[partition_id]
/brokers/topics/[topic_name]/partitions/[partition_id]/state

### Topic Partition Change Detection
Callback mechanisms to:
- Monitor changes in topic partitions
- React to partition reassignments
- Handle broker failures affecting partitions
- Update local state based on ZooKeeper changes

Example callback registration:
```java
zookeeperClient.subscribeDataChanges("/brokers/topics", topicChangeListener);
```

### Controller Election
Implement controller election using ZooKeeper:
- Use ZooKeeper's atomic operations for leader election
- Handle controller failover
- Maintain controller state
- Register watches for controller changes

Current implementation in `ZkController.java`:
- Uses ephemeral nodes for controller election
- Handles election failures and existing controllers
- Maintains live broker information
- Implements callback interfaces for ZooKeeper events