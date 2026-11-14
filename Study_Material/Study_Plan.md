## 1. Concepts You Must Understand

| Concept | What You Should Understand |
| :--- | :--- |
| **Kafka Broker** | Server that stores and serves Kafka data |
| **Cluster** | Collection of Kafka brokers |
| **Topic** | Logical category where events are published |
| **Partition** | Unit of parallelism and ordered storage |
| **Producer** | Application that publishes events |
| **Consumer** | Application that reads events |
| **Offset** | Position of a record within a partition |
| **Consumer Group** | Consumers cooperating to process topic partitions |
| **Replication** | Copies of partitions across brokers |


### Detailed Kafka Concepts

#### 1. Kafka Broker
A **Kafka Broker** is a single Kafka server. Its primary job is to receive messages from producers, assign offsets to them, commit them to disk storage, and serve these messages to consumers when requested. Brokers operate statelessly regarding consumers and are designed to handle high volumes of read and write requests with very low latency.
```text
+-------------------------------------------------------------+
|                        Kafka Cluster                        |
|                                                             |
|   +--------------+    +--------------+    +--------------+  |
|   |   Broker 1   |    |   Broker 2   |    |   Broker 3   |  |
|   | (Server/Node)|    | (Server/Node)|    | (Server/Node)|  |
|   +--------------+    +--------------+    +--------------+  |
+-------------------------------------------------------------+
```

#### 2. Cluster
A **Kafka Cluster** is a group of one or more Kafka brokers working together as a single distributed system. Clustering allows Kafka to scale horizontally and provides fault tolerance. If one broker fails, other brokers in the cluster can take over its load. Clusters use KRaft (Kafka Raft) or Apache ZooKeeper (in older versions) to manage metadata, coordinate brokers, and elect leaders.

#### 3. Topic
A **Topic** is a logical channel or category name to which events/messages are published. Think of a topic like a folder in a filesystem or a table in a relational database. Topics in Kafka are multi-subscriber, meaning a single topic can have zero, one, or many consumers subscribing to the data written to it. Data in a topic is retained for a configurable amount of time, regardless of whether it has been consumed.

#### 4. Partition
To achieve massive scale, a topic is divided into smaller, independent chunks called **Partitions**. 
* **Parallelism:** Partitions allow a topic's data to be distributed across multiple brokers, enabling multiple producers and consumers to read and write data in parallel. 
* **Ordering:** A partition is an append-only, ordered log. Kafka guarantees the order of messages *only within a single partition*, not across the entire topic.  
(Note: Ordering is guaranteed only within a single partition, not across the entire topic.)
```text
Topic: "website-clicks"

Partition 0:  [offset:0] -> [offset:1] -> [offset:2] -> [offset:3 (latest)]
               (Oldest)                                  (Newest)

Partition 1:  [offset:0] -> [offset:1 (latest)]

Partition 2:  [offset:0] -> [offset:1] -> [offset:2 (latest)]
```

#### 5. Producer
A **Producer** is a client application that creates and publishes events to one or more Kafka topics. The producer is responsible for deciding which partition within a topic to send a message to. This is usually determined by a message key (e.g., using a hash of a "User ID" to ensure all events for that user go to the same partition) or distributed round-robin if no key is provided.

#### 6. Consumer
A **Consumer** is a client application that subscribes to topics and reads (pulls) events from Kafka. Unlike traditional message queues that push data, Kafka consumers actively poll the brokers for new data. This allows consumers to process messages at their own pace without being overwhelmed.  
``` text
+--------------+      Writes (Push)       +---------------+
|  Producer A  | =======================> |               |
+--------------+                          |               |
                                          |  Kafka Topic  |
+--------------+                          | (Partitions)  |
|  Producer B  | =======================> |               |
+--------------+                          +---------------+
                                                  ||
                                                  || Reads (Pull)
                                                  \/
                                          +---------------+
                                          |   Consumer    |
                                          +---------------+
```

#### 7. Offset
An **Offset** is a unique, sequentially increasing integer ID assigned to every message as it is written to a partition. It represents the exact position of a record within that specific partition. Consumers use offsets to track their progress; by committing their current offset back to Kafka, a consumer can disconnect, reconnect later, and resume reading exactly where it left off.

#### 8. Consumer Group
A **Consumer Group** is a collection of one or more consumers working together to process a topic in parallel. 
* Kafka distributes the topic's partitions evenly among the consumers in the group. 
* **The Golden Rule:** A single partition can only be read by *one* consumer within the same consumer group at any given time. If you have more consumers than partitions, the extra consumers will sit idle.  
```text
+---------------------------------------------+
|            Kafka Topic (4 Partitions)       |
|   [Partition 0]               [Partition 1] |
|   [Partition 2]               [Partition 3] |
+---------------------------------------------+
         |            |               |
         v            |               v
+------------------+  |  +------------------+
|   Consumer 1     |  |  |   Consumer 2     | 
| (Reads P0 & P1)  |  |  | (Reads P2 & P3)  |
+------------------+  |  +------------------+
                      v
      +---------------------------------+
      |        Consumer Group A         |
      +---------------------------------+
```

#### 9. Replication
**Replication** ensures your data is safe and highly available even if a server crashes. Every partition can be copied (replicated) across multiple brokers.
* **Leader:** For a given partition, one broker acts as the Leader. All read and write requests go to the leader.
* **Followers:** The other brokers act as Followers, passively copying data from the leader. If the leader broker goes down, a follower is automatically elected as the new leader to prevent data loss or downtime.  
```text
Broker 1                 Broker 2                 Broker 3
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
|  [Partition 0]   |     |  [Partition 0]   |     |  [Partition 0]   |
|     LEADER       |====>|    FOLLOWER      |====>|    FOLLOWER      |
| (Handles R/W)    |     |  (Copies data)   |     |  (Copies data)   |
|                  |     |                  |     |                  |
+------------------+     +------------------+     +------------------+
```