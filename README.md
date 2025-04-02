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

The current `ZkController.java` provides the framework with:
- Controller election structure
- ZooKeeper event handling
- Broker state management

### 4. Create Topic Command
Implement a command to create topics with specified configurations:
- Accept parameters for:
  - Topic name
  - Number of partitions
  - Replication factor
- Validate input parameters
- Ensure topic names are unique

Example API:
```java
public void createTopic(String topicName, int partitions, int replicationFactor)
```

### 5. Partition and Replica Assignment
Implement logic for:
- Distributing partitions across available brokers evenly
- Assigning replicas to different brokers for fault tolerance
- Ensuring replicas are spread across different brokers
- Maintaining balanced load across brokers

Key considerations:
- Each partition should have a leader and followers
- Replicas should be distributed across different brokers for fault tolerance
- Implement a round-robin or similar algorithm for balanced distribution

### 6. ZooKeeper Persistence
Store topic and partition information in ZooKeeper:
- Create persistent nodes for topics
- Store partition assignments
- Store replica assignments
- Maintain broker-topic-partition mapping

ZooKeeper path structure: 
brokers/topics/[topic_name]
/brokers/topics/[topic_name]/partitions/[partition_id]
/brokers/topics/[topic_name]/partitions/[partition_id]/state

### 7. Topic Partition Change Detection
Implement callback mechanisms to:
- Monitor changes in topic partitions
- React to partition reassignments
- Handle broker failures affecting partitions
- Update local state based on ZooKeeper changes

Example callback registration:
```java
zookeeperClient.subscribeDataChanges("/brokers/topics", topicChangeListener);
```

### 8. Controller Election
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

## Testing Requirements
- Test topic creation with various partition and replica configurations
- Verify partition distribution balance
- Test controller election and failover scenarios
- Ensure proper handling of broker failures
- Validate ZooKeeper persistence