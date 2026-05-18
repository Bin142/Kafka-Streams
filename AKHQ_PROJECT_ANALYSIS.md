# AKHQ Internal Logic Analysis

> **Mục đích tài liệu**: Phân tích chi tiết logic nội bộ của AKHQ để làm tài liệu tham khảo khi xây dựng hệ thống quản lý Kafka riêng bằng ngôn ngữ/framework khác.

---

## 1. Tổng Quan Kiến Trúc

### 1.1 Luồng Xử Lý Request

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  REST API   │────▶│ Repository  │────▶│ KafkaModule │────▶│ Kafka APIs  │
│ Controllers │     │   Layer     │     │  (Wrapper)  │     │  (Native)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                          │                          │
                    ▼                          ▼                          ▼
           ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
           │  AdminClient  │          │ KafkaConsumer │          │ KafkaProducer │
           │  (Quản lý)    │          │ (Đọc data)    │          │ (Ghi data)    │
           └───────────────┘          └───────────────┘          └───────────────┘
```

### 1.2 Các Thành Phần Chính

| Thành phần | File | Chức năng |
|------------|------|-----------|
| **KafkaModule** | `modules/KafkaModule.java` | Factory tạo các Kafka clients |
| **AbstractKafkaWrapper** | `modules/AbstractKafkaWrapper.java` | Wrapper các Kafka Admin operations |
| **TopicRepository** | `repositories/TopicRepository.java` | Logic xử lý Topics |
| **RecordRepository** | `repositories/RecordRepository.java` | Logic produce/consume messages |
| **ConsumerGroupRepository** | `repositories/ConsumerGroupRepository.java` | Logic Consumer Groups |
| **SchemaRegistryRepository** | `repositories/SchemaRegistryRepository.java` | Logic Schema Registry |

---

## 2. Kết Nối Tới Kafka Cluster

### 2.1 Cấu Hình Connection (Connection.java)

```java
// Cấu trúc cấu hình trong YAML
akhq:
  connections:
    <cluster-name>:           // Tên cluster (unique identifier)
      properties:             // Kafka client properties
        bootstrap.servers: "kafka:9092"
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-256
        sasl.jaas.config: "..."
      schema-registry:        // Schema Registry config (optional)
        url: "http://schema-registry:8085"
        type: confluent|tibco|aws_glue
        basic-auth-username: ""
        basic-auth-password: ""
      connect:                // Kafka Connect config (optional)
        - name: "connect-1"
          url: "http://connect:8083"
      ksqldb:                 // KsqlDB config (optional)
        - name: "ksqldb-1"
          url: "http://ksqldb:8088"
```

### 2.2 Tạo AdminClient (KafkaModule.java)

```java
// Logic tạo AdminClient - dùng để quản lý cluster
public AdminClient getAdminClient(String clusterId) {
    // 1. Kiểm tra cluster tồn tại
    if (!this.clusterExists(clusterId)) {
        throw new InvalidClusterException("Invalid cluster '" + clusterId + "'");
    }
    
    // 2. Cache AdminClient theo clusterId (singleton per cluster)
    if (!this.adminClient.containsKey(clusterId)) {
        // 3. Lấy properties từ config
        Properties props = this.getAdminProperties(clusterId);
        // 4. Tạo AdminClient với Kafka native API
        this.adminClient.put(clusterId, AdminClient.create(props));
    }
    
    return this.adminClient.get(clusterId);
}

// Merge properties từ defaults và connection config
private Properties getAdminProperties(String clusterId) {
    Properties props = new Properties();
    props.putAll(this.getDefaultsProperties(this.defaults, "admin"));  // defaults
    props.putAll(this.getDefaultsProperties(this.connections, clusterId));  // connection specific
    return props;
}
```

### 2.3 Tạo KafkaConsumer (KafkaModule.java)

```java
// Logic tạo Consumer - dùng để đọc messages
public KafkaConsumer<byte[], byte[]> getConsumer(String clusterId, Properties properties) {
    Properties props = this.getConsumerProperties(clusterId);
    props.putAll(properties);  // Override với custom properties
    
    // Sử dụng ByteArrayDeserializer để đọc raw bytes
    // Việc deserialize sẽ được xử lý ở tầng application
    if (props.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) &&
        props.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
        return new KafkaConsumer<>(props);
    } else {
        return new KafkaConsumer<>(
            props,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        );
    }
}
```

### 2.4 Tạo KafkaProducer (KafkaModule.java)

```java
// Logic tạo Producer - dùng để ghi messages (cached per cluster)
public KafkaProducer<byte[], byte[]> getProducer(String clusterId) {
    if (!this.producers.containsKey(clusterId)) {
        Properties props = this.getProducerProperties(clusterId);
        
        // Sử dụng ByteArraySerializer để ghi raw bytes
        this.producers.put(clusterId, new KafkaProducer<>(
            props,
            new ByteArraySerializer(),
            new ByteArraySerializer()
        ));
    }
    return this.producers.get(clusterId);
}
```



---

## 3. Kafka Admin Operations (AbstractKafkaWrapper.java)

### 3.1 Describe Cluster

```java
// Lấy thông tin cluster: nodes, controller, cluster ID
public DescribeClusterResult describeCluster(String clusterId) {
    DescribeClusterResult cluster = kafkaModule.getAdminClient(clusterId).describeCluster();
    
    // Các thông tin trả về:
    // - cluster.clusterId()   -> String: Kafka cluster ID
    // - cluster.nodes()       -> Collection<Node>: Danh sách brokers
    // - cluster.controller()  -> Node: Controller node
    
    return cluster;
}
```

**Kafka API tương đương:**
```
AdminClient.describeCluster()
```

### 3.2 List Topics

```java
// Liệt kê tất cả topics (bao gồm internal topics)
public Collection<TopicListing> listTopics(String clusterId) {
    return kafkaModule.getAdminClient(clusterId)
        .listTopics(new ListTopicsOptions().listInternal(true))
        .listings()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.listTopics(ListTopicsOptions)
```

### 3.3 Describe Topics

```java
// Lấy thông tin chi tiết của topics
public Map<String, TopicDescription> describeTopics(String clusterId, List<String> topics) {
    // Cache kết quả để tránh gọi lại
    List<String> list = new ArrayList<>(topics);
    list.removeIf(value -> this.describeTopics.get(clusterId).containsKey(value));
    
    if (list.size() > 0) {
        Map<String, TopicDescription> description = kafkaModule.getAdminClient(clusterId)
            .describeTopics(list)
            .allTopicNames()
            .get();
        
        this.describeTopics.get(clusterId).putAll(description);
    }
    
    return this.describeTopics.get(clusterId);
}
```

**TopicDescription chứa:**
- `name()` - Tên topic
- `partitions()` - List<TopicPartitionInfo>: thông tin partitions
- `isInternal()` - Boolean: có phải internal topic không

### 3.4 Create Topic

```java
public void createTopics(String clusterId, String name, int partitions, 
    short replicationFactor, List<Config> configs) {
    
    // 1. Chuyển đổi configs sang Map
    Map<String, String> kafkaTopicConfigs = new HashMap<>();
    configs.forEach(c -> kafkaTopicConfigs.put(c.getName(), c.getValue()));
    
    // 2. Tạo NewTopic object
    NewTopic topic = new NewTopic(name, partitions, replicationFactor)
        .configs(kafkaTopicConfigs);
    
    // 3. Gọi Kafka Admin API
    kafkaModule.getAdminClient(clusterId)
        .createTopics(Collections.singleton(topic))
        .all()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.createTopics(Collection<NewTopic>)
```

### 3.5 Delete Topic

```java
public void deleteTopics(String clusterId, String name) {
    kafkaModule.getAdminClient(clusterId)
        .deleteTopics(Collections.singleton(name))
        .all()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.deleteTopics(Collection<String>)
```

### 3.6 Alter Topic Partitions

```java
// Tăng số partition (chỉ có thể tăng, không giảm được)
public void alterTopicPartition(String clusterId, String name, int partitions) {
    Map<String, NewPartitions> newPartitionMap = new HashMap<>();
    newPartitionMap.put(name, NewPartitions.increaseTo(partitions));
    
    kafkaModule.getAdminClient(clusterId)
        .createPartitions(newPartitionMap)
        .all()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.createPartitions(Map<String, NewPartitions>)
```

### 3.7 Describe Topic Offsets

```java
// Lấy first/last offset của mỗi partition
public Map<String, List<Partition.Offsets>> describeTopicsOffsets(String clusterId, List<String> topics) {
    // 1. Lấy danh sách TopicPartition từ describeTopics
    List<TopicPartition> collect = this.describeTopics(clusterId, topics).entrySet()
        .stream()
        .flatMap(topicDescription -> topicDescription.getValue().partitions().stream()
            .map(info -> new TopicPartition(topicDescription.getValue().name(), info.partition()))
        )
        .collect(Collectors.toList());
    
    // 2. Tạo consumer để lấy offsets
    KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId);
    
    // 3. Lấy beginning offsets (dùng offsetsForTimes với timestamp=0)
    Map<TopicPartition, Long> startOffsetsToSearch = collect.stream()
        .map(p -> new AbstractMap.SimpleEntry<>(p, 0L))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<TopicPartition, OffsetAndTimestamp> begins = consumer.offsetsForTimes(startOffsetsToSearch);
    
    // 4. Lấy end offsets
    Map<TopicPartition, Long> ends = consumer.endOffsets(collect);
    
    consumer.close();
    
    // 5. Kết hợp thành Partition.Offsets
    return begins.entrySet().stream()
        .collect(groupingBy(
            o -> o.getKey().topic(),
            mapping(
                begin -> new Partition.Offsets(
                    begin.getKey().partition(),
                    begin.getValue() != null ? begin.getValue().offset() : ends.get(begin.getKey()),
                    ends.get(begin.getKey())
                ),
                toList()
            )
        ));
}
```

**Kafka API tương đương:**
```
KafkaConsumer.offsetsForTimes(Map<TopicPartition, Long>)  // beginning offsets
KafkaConsumer.endOffsets(Collection<TopicPartition>)      // end offsets
```

### 3.8 List Consumer Groups

```java
public Collection<ConsumerGroupListing> listConsumerGroups(String clusterId) {
    return kafkaModule.getAdminClient(clusterId)
        .listConsumerGroups()
        .all()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.listConsumerGroups()
```

### 3.9 Describe Consumer Groups

```java
public Map<String, ConsumerGroupDescription> describeConsumerGroups(String clusterId, List<String> groups) {
    return kafkaModule.getAdminClient(clusterId)
        .describeConsumerGroups(groups)
        .all()
        .get();
}
```

**ConsumerGroupDescription chứa:**
- `groupId()` - Group ID
- `state()` - ConsumerGroupState (STABLE, PREPARING_REBALANCE, etc.)
- `coordinator()` - Node: coordinator broker
- `members()` - Collection<MemberDescription>: active members
- `partitionAssignor()` - String: partition assignor strategy

### 3.10 Get Consumer Group Offsets

```java
public Map<TopicPartition, OffsetAndMetadata> consumerGroupsOffsets(String clusterId, String groupId) {
    return kafkaModule.getAdminClient(clusterId)
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata()
        .get();
}
```

**Kafka API tương đương:**
```
AdminClient.listConsumerGroupOffsets(String groupId)
```

### 3.11 Delete Consumer Group

```java
public void deleteConsumerGroups(String clusterId, String name) {
    kafkaModule.getAdminClient(clusterId)
        .deleteConsumerGroups(Collections.singleton(name))
        .all()
        .get();
}
```

### 3.12 Delete Consumer Group Offsets

```java
public void deleteConsumerGroupOffsets(String clusterId, String groupName, String topicName) {
    // 1. Lấy thông tin topic để biết số partitions
    final Map<String, TopicDescription> topics = describeTopics(clusterId, List.of(topicName));
    
    if (topics.containsKey(topicName)) {
        final TopicDescription topic = topics.get(topicName);
        
        // 2. Tạo set TopicPartition cho tất cả partitions của topic
        final Set<TopicPartition> topicPartitions = topic.partitions().stream()
            .map(p -> new TopicPartition(topicName, p.partition()))
            .collect(toSet());
        
        // 3. Xóa offsets
        kafkaModule.getAdminClient(clusterId)
            .deleteConsumerGroupOffsets(groupName, topicPartitions)
            .all()
            .get();
    }
}
```

### 3.13 Describe Configs

```java
// Lấy configs của topic hoặc broker
public Map<ConfigResource, Config> describeConfigs(String clusterId, 
    ConfigResource.Type type, List<String> names) {
    
    return kafkaModule.getAdminClient(clusterId)
        .describeConfigs(names.stream()
            .map(s -> new ConfigResource(type, s))  // type: TOPIC, BROKER
            .collect(Collectors.toList())
        )
        .all()
        .get();
}
```

**ConfigResource.Type:**
- `TOPIC` - Topic configs
- `BROKER` - Broker configs

### 3.14 Alter Configs

```java
public void alterConfigs(String clusterId, Map<ConfigResource, Collection<AlterConfigOp>> configs) {
    kafkaModule.getAdminClient(clusterId)
        .incrementalAlterConfigs(configs)
        .all()
        .get();
}
```

**AlterConfigOp.OpType:**
- `SET` - Set config value
- `DELETE` - Delete config (reset to default)
- `APPEND` - Append to list config
- `SUBTRACT` - Remove from list config

### 3.15 Describe ACLs

```java
public Collection<AclBinding> describeAcls(String clusterId, AclBindingFilter filter) {
    return kafkaModule.getAdminClient(clusterId)
        .describeAcls(filter)
        .values()
        .get();
}
```

### 3.16 Describe Log Dirs

```java
public Map<Integer, Map<String, LogDirDescription>> describeLogDir(String clusterId) {
    // 1. Lấy danh sách node IDs
    List<Integer> nodeIds = this.describeCluster(clusterId).nodes().get()
        .stream()
        .map(Node::id)
        .collect(Collectors.toList());
    
    // 2. Describe log dirs cho tất cả nodes
    return kafkaModule.getAdminClient(clusterId)
        .describeLogDirs(nodeIds)
        .allDescriptions()
        .get();
}
```



---

## 4. Topic Operations (TopicRepository.java)

### 4.1 List Topics với Filter

```java
public List<String> all(String clusterId, TopicListView view, Optional<String> search, List<String> filters) {
    return kafkaWrapper.listTopics(clusterId)
        .stream()
        .map(TopicListing::name)
        // Filter theo search keyword
        .filter(name -> isSearchMatch(search, name) && isMatchRegex(filters, name))
        // Filter theo view type
        .filter(name -> isListViewMatch(view, name))
        // Sort alphabetically
        .sorted(Comparator.comparing(String::toLowerCase))
        .collect(Collectors.toList());
}

// TopicListView options:
// - ALL: Hiển thị tất cả
// - HIDE_INTERNAL: Ẩn internal topics (match với internalRegexps)
// - HIDE_STREAM: Ẩn stream topics (match với streamRegexps)
// - HIDE_INTERNAL_STREAM: Ẩn cả internal và stream topics

// Default internal regexps: ["^_.*$", "^.*_schemas$", "^.*connect-config$", ...]
// Default stream regexps: ["^.*-changelog$", "^.*-repartition$", ...]
```

### 4.2 Get Topic Details

```java
public List<Topic> findByName(String clusterId, List<String> topics) {
    ArrayList<Topic> list = new ArrayList<>();
    
    // 1. Describe topics để lấy partition info
    Set<Map.Entry<String, TopicDescription>> topicDescriptions = 
        kafkaWrapper.describeTopics(clusterId, topics).entrySet();
    
    // 2. Lấy offsets của topics
    Map<String, List<Partition.Offsets>> topicOffsets = 
        kafkaWrapper.describeTopicsOffsets(clusterId, topics);
    
    // 3. Build Topic model
    for (Map.Entry<String, TopicDescription> description : topicDescriptions) {
        list.add(new Topic(
            description.getValue(),                              // TopicDescription
            logDirRepository.findByTopic(clusterId, name),      // Log dir info
            topicOffsets.get(name),                             // Offsets
            isInternal(name),                                   // Is internal?
            isStream(name)                                      // Is stream?
        ));
    }
    
    return list;
}
```

### 4.3 Create Topic

```java
public void create(String clusterId, String name, int partitions, 
    short replicationFactor, List<Config> configs) {
    kafkaWrapper.createTopics(clusterId, name, partitions, replicationFactor, configs);
}
```

### 4.4 Delete Topic

```java
public void delete(String clusterId, String name) {
    kafkaWrapper.deleteTopics(clusterId, name);
}
```

### 4.5 Increase Partitions

```java
public void increasePartition(String clusterId, String name, int partitions) {
    kafkaWrapper.alterTopicPartition(clusterId, name, partitions);
}
```

---

## 5. Record Operations (RecordRepository.java)

### 5.1 Consume Messages (Sort OLDEST)

```java
private List<Record> consumeOldest(Topic topic, Options options) {
    List<Record> list = new ArrayList<>();
    
    // 1. Lấy starting offset cho mỗi partition
    Map<TopicPartition, Long> partitions = getTopicPartitionForSortOldest(topic, options);
    
    // 2. Consume song song từ mỗi partition
    partitions.entrySet().parallelStream().forEach(partition -> {
        Properties properties = new Properties() {{
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, options.size);
        }};
        
        try (KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId, properties)) {
            // Assign partition và seek tới offset
            consumer.assign(List.of(partition.getKey()));
            consumer.seek(partition.getKey(), partition.getValue());
            
            // Poll records
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeout));
            
            for (ConsumerRecord<byte[], byte[]> record : records) {
                Record current = newRecord(record, options, topic);
                // Apply filters (search by key, value, headers)
                if (matchFilters(options, current)) {
                    list.add(current);
                }
            }
        }
    });
    
    // 3. Sort theo timestamp và limit
    return list.stream()
        .sorted(Comparator.comparing(Record::getTimestamp))
        .limit(options.size)
        .toList();
}
```

### 5.2 Consume Messages (Sort NEWEST)

```java
private List<Record> consumeNewest(Topic topic, Options options) {
    return topic.getPartitions().parallelStream()
        .map(partition -> {
            // 1. Tính toán offset range (end - size -> end)
            KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId, properties);
            return getOffsetForSortNewest(consumer, partition, options);
        })
        .flatMap(topicPartitionOffset -> {
            // 2. Assign và seek
            consumer.assign(Collections.singleton(topicPartitionOffset.getTopicPartition()));
            consumer.seek(topicPartitionOffset.getTopicPartition(), topicPartitionOffset.getBegin());
            
            List<Record> list = new ArrayList<>();
            
            // 3. Poll cho đến khi đạt end offset
            do {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeout));
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.offset() > topicPartitionOffset.getEnd()) {
                        break;
                    }
                    Record current = newRecord(record, options, topic);
                    if (matchFilters(options, current)) {
                        list.add(current);
                    }
                }
            } while (emptyPoll < 1);
            
            Collections.reverse(list);
            consumer.close();
            
            return Stream.of(list);
        })
        .flatMap(List::stream)
        .sorted(Comparator.comparing(Record::getTimestamp).reversed())
        .limit(options.size)
        .collect(Collectors.toList());
}
```

### 5.3 Get Offsets By Timestamp

```java
public List<TimeOffset> getOffsetForTime(String clusterId, 
    List<TopicPartition> partitions, Long timestamp) {
    
    try (KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId)) {
        // Tạo map partition -> timestamp
        Map<TopicPartition, Long> map = new HashMap<>();
        partitions.forEach(partition -> map.put(
            new TopicPartition(partition.getTopic(), partition.getPartition()),
            timestamp
        ));
        
        // Gọi Kafka API
        return consumer.offsetsForTimes(map)
            .entrySet()
            .stream()
            .filter(r -> r.getValue() != null)
            .map(r -> new TimeOffset(
                r.getKey().topic(),
                r.getKey().partition(),
                r.getValue().offset()
            ))
            .collect(Collectors.toList());
    }
}
```

### 5.4 Produce Message

```java
public RecordMetadata produce(String clusterId, String topic, 
    Optional<String> value, List<KeyValue<String, String>> headers,
    Optional<String> key, Optional<Integer> partition, Optional<Long> timestamp,
    Optional<String> keySchema, Optional<String> valueSchema) {
    
    byte[] keyAsBytes = null;
    byte[] valueAsBytes;
    
    // 1. Serialize key
    if (key.isPresent()) {
        if (keySchema.isPresent() && StringUtils.isNotEmpty(keySchema.get())) {
            // Serialize với Schema Registry
            Schema schema = schemaRegistryRepository.getLatestVersion(clusterId, keySchema.get());
            SchemaSerializer keySerializer = serializerFactory.createSerializer(clusterId, schema.getId());
            keyAsBytes = keySerializer.serialize(key.get());
        } else {
            // Plain string
            keyAsBytes = key.get().getBytes();
        }
    }
    
    // 2. Serialize value
    if (value.isPresent() && valueSchema.isPresent()) {
        Schema schema = schemaRegistryRepository.getLatestVersion(clusterId, valueSchema.get());
        SchemaSerializer valueSerializer = serializerFactory.createSerializer(clusterId, schema.getId());
        valueAsBytes = valueSerializer.serialize(value.get());
    } else {
        valueAsBytes = value.map(String::getBytes).orElse(null);
    }
    
    // 3. Produce
    return kafkaModule.getProducer(clusterId)
        .send(new ProducerRecord<>(
            topic,
            partition.orElse(null),
            timestamp.orElse(null),
            keyAsBytes,
            valueAsBytes,
            headers.stream()
                .map(entry -> new RecordHeader(entry.getKey(), entry.getValue().getBytes()))
                .collect(Collectors.toList())
        ))
        .get();
}
```

### 5.5 Delete Record (Tombstone)

```java
// Xóa record bằng cách produce tombstone (value = null)
public RecordMetadata delete(String clusterId, String topic, Integer partition, byte[] key) {
    return kafkaModule.getProducer(clusterId)
        .send(new ProducerRecord<>(
            topic,
            partition,
            key,
            null  // Tombstone: value = null
        ))
        .get();
}
```

### 5.6 Empty Topic (Delete All Records)

```java
public void emptyTopic(String clusterId, String topicName) {
    Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
    
    var topic = topicRepository.findByName(clusterId, topicName);
    
    // Xóa tất cả records trước lastOffset của mỗi partition
    topic.getPartitions().forEach(partition -> {
        recordsToDelete.put(
            new TopicPartition(partition.getTopic(), partition.getId()),
            RecordsToDelete.beforeOffset(partition.getLastOffset())
        );
    });
    
    // Gọi Kafka Admin API
    kafkaModule.getAdminClient(clusterId)
        .deleteRecords(recordsToDelete)
        .lowWatermarks();
}
```

### 5.7 Get Last Record of Topics

```java
public Map<String, Record> getLastRecord(String clusterId, List<String> topicsName) {
    // 1. Lấy thông tin topics
    Map<String, Topic> topics = topicRepository.findByName(clusterId, topicsName).stream()
        .collect(Collectors.toMap(Topic::getName, Function.identity()));
    
    // 2. Tạo list TopicPartition
    List<TopicPartition> topicPartitions = topics.values().stream()
        .flatMap(topic -> topic.getPartitions().stream())
        .map(partition -> new TopicPartition(partition.getTopic(), partition.getId()))
        .collect(Collectors.toList());
    
    ConcurrentHashMap<String, Record> records = new ConcurrentHashMap<>();
    
    try (KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId)) {
        consumer.assign(topicPartitions);
        
        // 3. Seek tới gần cuối mỗi partition (endOffset - 2)
        Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(consumer.assignment());
        consumer.endOffsets(consumer.assignment())
            .forEach((topicPartition, offset) -> {
                long beginOffset = beginOffsets.getOrDefault(topicPartition, 0L);
                consumer.seek(topicPartition, Math.max(beginOffset, offset - 2));
            });
        
        // 4. Poll và lấy record mới nhất
        consumer.poll(Duration.ofMillis(pollTimeout))
            .forEach(record -> {
                if (!records.containsKey(record.topic())) {
                    records.put(record.topic(), newRecord(record, clusterId, topics.get(record.topic())));
                } else {
                    Record current = records.get(record.topic());
                    if (current.getTimestamp().toInstant().toEpochMilli() < record.timestamp()) {
                        records.put(record.topic(), newRecord(record, clusterId, topics.get(record.topic())));
                    }
                }
            });
    }
    
    return records;
}
```



---

## 6. Consumer Group Operations (ConsumerGroupRepository.java)

### 6.1 List Consumer Groups

```java
public List<String> all(String clusterId, Optional<String> search, List<String> filters) {
    return kafkaWrapper.listConsumerGroups(clusterId).stream()
        .map(ConsumerGroupListing::groupId)
        .filter(groupId -> isSearchMatch(search, groupId) && isMatchRegex(filters, groupId))
        .sorted(String::compareToIgnoreCase)
        .collect(Collectors.toList());
}
```

### 6.2 Get Consumer Group Details

```java
public List<ConsumerGroup> findByName(String clusterId, List<String> groups, List<String> filters) {
    // 1. Describe consumer groups
    Map<String, ConsumerGroupDescription> consumerDescriptions = 
        kafkaWrapper.describeConsumerGroups(clusterId, groups);
    
    // 2. Lấy offsets của mỗi group
    Map<String, Map<TopicPartition, OffsetAndMetadata>> groupGroupsOffsets = 
        consumerDescriptions.keySet().stream()
            .map(group -> new AbstractMap.SimpleEntry<>(
                group, 
                kafkaWrapper.consumerGroupsOffsets(clusterId, group)
            ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    
    // 3. Lấy danh sách topics từ offsets
    List<String> topics = groupGroupsOffsets.values().stream()
        .map(Map::keySet)
        .flatMap(Set::stream)
        .map(TopicPartition::topic)
        .distinct()
        .collect(Collectors.toList());
    
    // 4. Lấy offsets của topics để tính lag
    Map<String, List<Partition.Offsets>> topicTopicsOffsets = 
        kafkaWrapper.describeTopicsOffsets(clusterId, topics);
    
    // 5. Build ConsumerGroup model
    return consumerDescriptions.values().stream()
        .map(description -> new ConsumerGroup(
            description,                                          // ConsumerGroupDescription
            groupGroupsOffsets.get(description.groupId()),       // Group offsets
            groupGroupsOffsets.get(description.groupId()).keySet().stream()
                .map(TopicPartition::topic)
                .distinct()
                .collect(Collectors.toMap(
                    Function.identity(), 
                    topicTopicsOffsets::get
                ))                                                // Topic offsets (để tính lag)
        ))
        .sorted(Comparator.comparing(ConsumerGroup::getId))
        .collect(Collectors.toList());
}
```

### 6.3 Find Consumer Groups by Topic

```java
// Tìm tất cả consumer groups đang consume từ topic (bao gồm empty groups)
public List<ConsumerGroup> findByTopic(String clusterId, String topic, List<String> filters) {
    // 1. Lấy tất cả consumer groups
    List<String> groupName = this.all(clusterId, Optional.empty(), filters);
    List<ConsumerGroup> list = this.findByName(clusterId, groupName, filters);
    
    // 2. Filter theo topic
    return list.stream()
        .filter(consumerGroup -> consumerGroup.getTopics().stream()
            .anyMatch(s -> Objects.equals(s, topic)))
        .collect(Collectors.toList());
}
```

### 6.4 Find Active Consumer Groups by Topic

```java
// Tìm consumer groups đang active (có members) cho topic
public List<ConsumerGroup> findActiveByTopics(String clusterId, List<String> topics, List<String> filters) {
    List<String> groupsName = this.all(clusterId, Optional.empty(), filters);
    
    // Filter groups có members đang consume từ topics
    List<String> consumerGroups = kafkaWrapper.describeConsumerGroups(clusterId, groupsName)
        .values().stream()
        .filter(description ->
            description.members().stream()
                .flatMap(member -> member.assignment().topicPartitions().stream()
                    .map(TopicPartition::topic))
                .distinct()
                .anyMatch(topics::contains)
        )
        .map(ConsumerGroupDescription::groupId)
        .toList();
    
    return this.findByName(clusterId, consumerGroups, filters);
}
```

### 6.5 Update Consumer Group Offsets

```java
public void updateOffsets(String clusterId, String name, Map<TopicPartition, Long> offset) {
    // 1. Tạo consumer với group.id
    KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId, new Properties() {{
        put(ConsumerConfig.GROUP_ID_CONFIG, name);
    }});
    
    // 2. Chuyển đổi sang OffsetAndMetadata
    Map<TopicPartition, OffsetAndMetadata> offsets = offset.entrySet().stream()
        .map(r -> new AbstractMap.SimpleEntry<>(
            new TopicPartition(r.getKey().getTopic(), r.getKey().getPartition()),
            new OffsetAndMetadata(r.getValue())
        ))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    
    // 3. Commit offsets
    consumer.commitSync(offsets);
    consumer.close();
    
    // 4. Clear cache
    kafkaWrapper.clearConsumerGroupsOffsets();
}
```

**Lưu ý quan trọng**: Để update offsets, consumer group phải ở trạng thái EMPTY (không có active members).

---

## 7. Schema Registry Operations (SchemaRegistryRepository.java)

### 7.1 Kết Nối Schema Registry (KafkaModule.java)

```java
public RestService getRegistryRestClient(String clusterId) {
    Connection connection = this.getConnection(clusterId);
    
    if (connection.getSchemaRegistry() != null) {
        // 1. Tạo RestService với URL
        RestService restService = new RestService(connection.getSchemaRegistry().getUrl());
        
        // 2. Cấu hình SSL nếu có
        if (connection.getSchemaRegistry().getProperties() != null) {
            Map<String, Object> sslConfigs = connection.getSchemaRegistry().getProperties()
                .entrySet().stream()
                .filter(e -> e.getKey().startsWith("schema.registry."))
                .collect(Collectors.toMap(
                    e -> e.getKey().substring("schema.registry.".length()), 
                    Map.Entry::getValue
                ));
            
            SslFactory sslFactory = new SslFactory(sslConfigs);
            if (sslFactory.sslContext() != null) {
                restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
            }
        }
        
        // 3. Cấu hình Basic Auth nếu có
        if (connection.getSchemaRegistry().getBasicAuthUsername() != null) {
            BasicAuthCredentialProvider basicAuthCredentialProvider = 
                BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
                    new UserInfoCredentialProvider().alias(),
                    ImmutableMap.of(
                        "schema.registry.basic.auth.user.info",
                        connection.getSchemaRegistry().getBasicAuthUsername() + ":" +
                            connection.getSchemaRegistry().getBasicAuthPassword()
                    )
                );
            restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
        }
        
        return restService;
    }
    
    return null;
}

// Tạo SchemaRegistryClient với cache
public SchemaRegistryClient getRegistryClient(String clusterId) {
    List<SchemaProvider> providers = new ArrayList<>();
    providers.add(new AvroSchemaProvider());
    providers.add(new JsonSchemaProvider());
    providers.add(new ProtobufSchemaProvider());
    
    return new CachedSchemaRegistryClient(
        this.getRegistryRestClient(clusterId),
        1000,  // Cache size
        providers,
        connection.getSchemaRegistry().getProperties(),
        null
    );
}
```

### 7.2 List Schemas

```java
public List<String> all(String clusterId, Optional<String> search, List<String> filters) {
    return kafkaModule.getRegistryRestClient(clusterId)
        .getAllSubjects()
        .stream()
        .filter(s -> isSearchMatch(search, s) && isMatchRegex(filters, s))
        .sorted(Comparator.comparing(String::toLowerCase))
        .collect(Collectors.toList());
}
```

**Schema Registry API tương đương:**
```
GET /subjects
```

### 7.3 Get Latest Schema Version

```java
public Schema getLatestVersion(String clusterId, String subject) {
    // 1. Gọi Schema Registry API
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema latestVersion = 
        kafkaModule.getRegistryRestClient(clusterId).getLatestVersion(subject);
    
    // 2. Parse schema theo type
    ParsedSchema parsedSchema = getParsedSchema(latestVersion, clusterId);
    
    // 3. Lấy config (compatibility level)
    Schema.Config config = this.getConfig(clusterId, subject);
    
    return new Schema(latestVersion, parsedSchema, config);
}

private ParsedSchema getParsedSchema(Schema schema, String clusterId) {
    if (schema.getSchemaType().equals(JsonSchema.TYPE)) {
        return kafkaModule.getJsonSchemaProvider(clusterId)
            .parseSchema(schema.getSchema(), schema.getReferences())
            .orElse(null);
    } else if (schema.getSchemaType().equals(ProtobufSchema.TYPE)) {
        return kafkaModule.getProtobufSchemaProvider(clusterId)
            .parseSchema(schema.getSchema(), schema.getReferences())
            .orElse(null);
    } else {
        // Default: Avro
        return kafkaModule.getAvroSchemaProvider(clusterId)
            .parseSchema(schema.getSchema(), schema.getReferences())
            .orElse(null);
    }
}
```

**Schema Registry API tương đương:**
```
GET /subjects/{subject}/versions/latest
```

### 7.4 Get All Schema Versions

```java
public List<Schema> getAllVersions(String clusterId, String subject) {
    Schema.Config config = this.getConfig(clusterId, subject);
    
    return kafkaModule.getRegistryRestClient(clusterId)
        .getAllVersions(subject)  // Trả về List<Integer> version numbers
        .parallelStream()
        .map(id -> {
            // Lấy schema cho mỗi version
            return kafkaModule.getRegistryRestClient(clusterId).getVersion(subject, id);
        })
        .map(schema -> {
            ParsedSchema parsedSchema = getParsedSchema(schema, clusterId);
            return new Schema(schema, parsedSchema, config);
        })
        .collect(Collectors.toList());
}
```

**Schema Registry API tương đương:**
```
GET /subjects/{subject}/versions
GET /subjects/{subject}/versions/{version}
```

### 7.5 Register Schema

```java
public Schema register(String clusterId, String subject, String type, 
    String schema, List<SchemaReference> references) {
    
    // 1. Register schema
    RegisterSchemaResponse registerSchemaResponse = kafkaModule.getRegistryRestClient(clusterId)
        .registerSchema(
            schema,                    // Schema string
            type != null ? type : "AVRO",  // Schema type
            references,                // Schema references
            subject                    // Subject name
        );
    
    // 2. Verify registration
    Schema latestVersion = getLatestVersion(clusterId, subject);
    
    if (latestVersion.getId() != registerSchemaResponse.getId()) {
        throw new IllegalArgumentException("Invalid id from registry");
    }
    
    return latestVersion;
}
```

**Schema Registry API tương đương:**
```
POST /subjects/{subject}/versions
Content-Type: application/json
{
  "schema": "...",
  "schemaType": "AVRO|JSON|PROTOBUF",
  "references": []
}
```

### 7.6 Delete Schema

```java
public int delete(String clusterId, String subject) {
    List<Integer> list = kafkaModule.getRegistryRestClient(clusterId)
        .deleteSubject(new HashMap<>(), subject);
    
    return list.get(0);  // Trả về version đã xóa
}

public int deleteVersion(String clusterId, String subject, int version) {
    return kafkaModule.getRegistryRestClient(clusterId)
        .deleteSchemaVersion(new HashMap<>(), subject, String.valueOf(version));
}
```

**Schema Registry API tương đương:**
```
DELETE /subjects/{subject}
DELETE /subjects/{subject}/versions/{version}
```

### 7.7 Get/Update Compatibility Config

```java
public Schema.Config getConfig(String clusterId, String subject) {
    return new Schema.Config(
        kafkaModule.getRegistryRestClient(clusterId)
            .getConfig(Map.of(), subject, true)
    );
}

public void updateConfig(String clusterId, String subject, Schema.Config config) {
    kafkaModule.getRegistryRestClient(clusterId)
        .updateCompatibility(config.getCompatibilityLevel().name(), subject);
}
```

**Compatibility Levels:**
- `BACKWARD` - New schema có thể đọc data cũ
- `FORWARD` - Old schema có thể đọc data mới
- `FULL` - Cả backward và forward
- `NONE` - Không check compatibility

**Schema Registry API tương đương:**
```
GET /config/{subject}
PUT /config/{subject}
```



---

## 8. Kafka Connect Operations (KafkaModule.java)

### 8.1 Kết Nối Kafka Connect

```java
public Map<String, KafkaConnectClient> getConnectRestClient(String clusterId) {
    Connection connection = this.getConnection(clusterId);
    
    if (connection.getConnect() != null && !connection.getConnect().isEmpty()) {
        Map<String, KafkaConnectClient> mapConnects = new HashMap<>();
        
        connection.getConnect().forEach(connect -> {
            URIBuilder uri = URIBuilder.fromString(connect.getUrl().toString());
            Configuration configuration = new Configuration(uri.toNormalizedURI(false).toString());
            
            // Basic Auth
            if (connect.getBasicAuthUsername() != null) {
                configuration.useBasicAuth(
                    connect.getBasicAuthUsername(),
                    connect.getBasicAuthPassword()
                );
            }
            
            // SSL Trust Store
            if (connect.getSslTrustStore() != null) {
                configuration.useTrustStore(
                    new File(connect.getSslTrustStore()),
                    connect.getSslTrustStorePassword()
                );
            }
            
            // SSL Key Store
            if (connect.getSslKeyStore() != null) {
                configuration.useKeyStore(
                    new File(connect.getSslKeyStore()),
                    connect.getSslKeyStorePassword()
                );
            }
            
            mapConnects.put(connect.getName(), new KafkaConnectClient(configuration));
        });
        
        return mapConnects;
    }
    
    return null;
}
```

### 8.2 Cấu Hình Kafka Connect (Connect.java)

```java
// Các thuộc tính cấu hình
String name;                    // Tên connect cluster
URL url;                        // URL của Kafka Connect REST API
String basicAuthUsername;       // Basic Auth username
String basicAuthPassword;       // Basic Auth password
String sslTrustStore;          // Path to trust store
String sslTrustStorePassword;  // Trust store password
String sslKeyStore;            // Path to key store
String sslKeyStorePassword;    // Key store password
```

### 8.3 Kafka Connect REST API Operations

AKHQ sử dụng thư viện `kafka-connect-client` để gọi Kafka Connect REST API:

| Operation | Kafka Connect API |
|-----------|-------------------|
| List connectors | `GET /connectors` |
| Get connector | `GET /connectors/{name}` |
| Create connector | `POST /connectors` |
| Update connector config | `PUT /connectors/{name}/config` |
| Delete connector | `DELETE /connectors/{name}` |
| Get connector status | `GET /connectors/{name}/status` |
| Restart connector | `POST /connectors/{name}/restart` |
| Pause connector | `PUT /connectors/{name}/pause` |
| Resume connector | `PUT /connectors/{name}/resume` |
| List tasks | `GET /connectors/{name}/tasks` |
| Restart task | `POST /connectors/{name}/tasks/{taskId}/restart` |
| List plugins | `GET /connector-plugins` |
| Validate plugin config | `PUT /connector-plugins/{pluginName}/config/validate` |

---

## 9. KsqlDB Operations (KafkaModule.java)

### 9.1 Kết Nối KsqlDB

```java
public Map<String, Client> getKsqlDbClient(String clusterId) {
    Connection connection = this.getConnection(clusterId);
    
    if (connection.getKsqldb() != null && !connection.getKsqldb().isEmpty()) {
        Map<String, Client> mapKsqlDbs = new HashMap<>();
        
        connection.getKsqldb().forEach(ksqlDb -> {
            URIBuilder uri = URIBuilder.fromString(ksqlDb.getUrl().toString());
            
            ClientOptions options = ClientOptions.create()
                .setHost(uri.getHost().get())
                .setPort(uri.getPort().get())
                .setUseTls(ksqlDb.isUseTls())
                .setUseAlpn(ksqlDb.isUseAlpn())
                .setVerifyHost(ksqlDb.isVerifyHost());
            
            // Basic Auth
            if (ksqlDb.getBasicAuthUsername() != null && ksqlDb.getBasicAuthPassword() != null) {
                options.setBasicAuthCredentials(
                    ksqlDb.getBasicAuthUsername(), 
                    ksqlDb.getBasicAuthPassword()
                );
            }
            
            Client client = Client.create(options);
            mapKsqlDbs.put(ksqlDb.getName(), client);
        });
        
        return mapKsqlDbs;
    }
    
    return null;
}
```

### 9.2 Cấu Hình KsqlDB (KsqlDb.java)

```java
String name;                    // Tên ksqlDB instance
URL url;                        // URL của KsqlDB server
boolean useTls = false;         // Sử dụng TLS
boolean useAlpn = false;        // Sử dụng ALPN
boolean verifyHost = true;      // Verify hostname
String basicAuthUsername;       // Basic Auth username
String basicAuthPassword;       // Basic Auth password
```

### 9.3 KsqlDB Operations

AKHQ sử dụng Confluent KsqlDB Java Client:

| Operation | Method |
|-----------|--------|
| Get server info | `client.serverInfo()` |
| List streams | `SHOW STREAMS` |
| List tables | `SHOW TABLES` |
| List queries | `SHOW QUERIES` |
| Execute statement | `client.executeStatement(sql)` |
| Execute pull query | `client.executeQuery(sql)` |

---

## 10. Cấu Hình Xác Thực (Authentication)

### 10.1 Các Phương Thức Xác Thực Kafka

#### Plain Text (Không xác thực)
```yaml
properties:
  bootstrap.servers: "kafka:9092"
```

#### SASL/PLAIN
```yaml
properties:
  bootstrap.servers: "kafka:9094"
  security.protocol: SASL_PLAINTEXT
  sasl.mechanism: PLAIN
  sasl.jaas.config: >
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="password";
```

#### SASL/SCRAM-SHA-256
```yaml
properties:
  bootstrap.servers: "kafka:9094"
  security.protocol: SASL_SSL
  sasl.mechanism: SCRAM-SHA-256
  sasl.jaas.config: >
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="admin"
    password="password";
```

#### SSL/TLS
```yaml
properties:
  bootstrap.servers: "kafka:9093"
  security.protocol: SSL
  ssl.truststore.location: /path/to/truststore.jks
  ssl.truststore.password: password
  ssl.keystore.location: /path/to/keystore.jks
  ssl.keystore.password: password
  ssl.key.password: password
```

#### AWS MSK IAM
```yaml
properties:
  bootstrap.servers: "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"
  security.protocol: SASL_SSL
  sasl.mechanism: AWS_MSK_IAM
  sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;
  sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

### 10.2 Xác Thực Schema Registry

```yaml
schema-registry:
  url: "https://schema-registry:8085"
  basic-auth-username: sr-user
  basic-auth-password: sr-password
  properties:
    schema.registry.ssl.truststore.location: /path/to/truststore.jks
    schema.registry.ssl.truststore.password: password
```

### 10.3 Xác Thực Kafka Connect

```yaml
connect:
  - name: "connect-1"
    url: "https://connect:8083"
    basic-auth-username: connect-user
    basic-auth-password: connect-password
    ssl-trust-store: /path/to/truststore.jks
    ssl-trust-store-password: password
    ssl-key-store: /path/to/keystore.jks
    ssl-key-store-password: password
```

### 10.4 Xác Thực KsqlDB

```yaml
ksqldb:
  - name: "ksqldb-1"
    url: "https://ksqldb:8088"
    use-tls: true
    verify-host: true
    basic-auth-username: ksqldb-user
    basic-auth-password: ksqldb-password
```



---

## 11. API Endpoints Reference

### 11.1 Cluster APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/cluster` | Lấy danh sách clusters | Config |
| GET | `/api/{cluster}/ui-options` | Lấy UI options | Config |

### 11.2 Topic APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/topic` | List topics | `AdminClient.listTopics()` |
| GET | `/api/{cluster}/topic/name` | List topic names | `AdminClient.listTopics()` |
| POST | `/api/{cluster}/topic` | Create topic | `AdminClient.createTopics()` |
| GET | `/api/{cluster}/topic/{name}` | Get topic details | `AdminClient.describeTopics()` |
| DELETE | `/api/{cluster}/topic/{name}` | Delete topic | `AdminClient.deleteTopics()` |
| GET | `/api/{cluster}/topic/{name}/partitions` | Get partitions | `AdminClient.describeTopics()` |
| POST | `/api/{cluster}/topic/{name}/partitions` | Increase partitions | `AdminClient.createPartitions()` |
| GET | `/api/{cluster}/topic/{name}/groups` | Get consumer groups | `AdminClient.listConsumerGroupOffsets()` |
| GET | `/api/{cluster}/topic/{name}/configs` | Get topic configs | `AdminClient.describeConfigs()` |
| POST | `/api/{cluster}/topic/{name}/configs` | Update topic configs | `AdminClient.incrementalAlterConfigs()` |
| GET | `/api/{cluster}/topic/{name}/logs` | Get log dirs | `AdminClient.describeLogDirs()` |
| GET | `/api/{cluster}/topic/{name}/acls` | Get ACLs | `AdminClient.describeAcls()` |

### 11.3 Topic Data APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/topic/{name}/data` | Consume messages | `KafkaConsumer.poll()` |
| POST | `/api/{cluster}/topic/{name}/data` | Produce message | `KafkaProducer.send()` |
| DELETE | `/api/{cluster}/topic/{name}/data` | Delete record (tombstone) | `KafkaProducer.send(key, null)` |
| DELETE | `/api/{cluster}/topic/{name}/data/empty` | Empty topic | `AdminClient.deleteRecords()` |
| GET | `/api/{cluster}/topic/{name}/data/search` | Search messages (SSE) | `KafkaConsumer.poll()` |
| GET | `/api/{cluster}/topic/{name}/data/download` | Download data | `KafkaConsumer.poll()` |
| GET | `/api/{cluster}/topic/{name}/data/record/{partition}/{offset}` | Get single record | `KafkaConsumer.seek() + poll()` |
| GET | `/api/{cluster}/topic/{name}/offsets/start` | Get offsets by timestamp | `KafkaConsumer.offsetsForTimes()` |
| GET | `/api/{cluster}/topic/last-record` | Get last record of topics | `KafkaConsumer.endOffsets() + poll()` |
| POST | `/api/{cluster}/topic/{from}/copy/{toCluster}/topic/{to}` | Copy data | `Consumer.poll() + Producer.send()` |

### 11.4 Consumer Group APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/group` | List consumer groups | `AdminClient.listConsumerGroups()` |
| GET | `/api/{cluster}/group/{name}` | Get group details | `AdminClient.describeConsumerGroups()` |
| GET | `/api/{cluster}/group/{name}/offsets` | Get group offsets | `AdminClient.listConsumerGroupOffsets()` |
| GET | `/api/{cluster}/group/{name}/members` | Get group members | `AdminClient.describeConsumerGroups()` |
| GET | `/api/{cluster}/group/{name}/acls` | Get group ACLs | `AdminClient.describeAcls()` |
| POST | `/api/{cluster}/group/{name}/offsets` | Update offsets | `KafkaConsumer.commitSync()` |
| GET | `/api/{cluster}/group/{name}/offsets/start` | Get offsets by timestamp | `KafkaConsumer.offsetsForTimes()` |
| DELETE | `/api/{cluster}/group/{name}` | Delete consumer group | `AdminClient.deleteConsumerGroups()` |
| DELETE | `/api/{cluster}/group/{name}/topic/{topic}` | Delete group offsets | `AdminClient.deleteConsumerGroupOffsets()` |
| GET | `/api/{cluster}/group/topics` | Get groups by topics | `AdminClient.describeConsumerGroups()` |

### 11.5 Node/Broker APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/node` | List nodes | `AdminClient.describeCluster()` |
| GET | `/api/{cluster}/node/partitions` | Get partition stats | `AdminClient.describeTopics()` |
| GET | `/api/{cluster}/node/{id}` | Get node details | `AdminClient.describeCluster()` |
| GET | `/api/{cluster}/node/{id}/logs` | Get node log dirs | `AdminClient.describeLogDirs()` |
| GET | `/api/{cluster}/node/{id}/configs` | Get node configs | `AdminClient.describeConfigs(BROKER)` |
| POST | `/api/{cluster}/node/{id}/configs` | Update node configs | `AdminClient.incrementalAlterConfigs()` |

### 11.6 Schema Registry APIs

| Method | Endpoint | Mô tả | Schema Registry API |
|--------|----------|-------|---------------------|
| GET | `/api/{cluster}/schemas` | List subjects | `GET /subjects` |
| GET | `/api/{cluster}/schema` | List schemas (paginated) | `GET /subjects` |
| GET | `/api/{cluster}/schema/topic/{topic}` | Get schemas for topic | `GET /subjects` |
| POST | `/api/{cluster}/schema` | Create schema | `POST /subjects/{subject}/versions` |
| GET | `/api/{cluster}/schema/{subject}` | Get latest schema | `GET /subjects/{subject}/versions/latest` |
| POST | `/api/{cluster}/schema/{subject}` | Update schema | `POST /subjects/{subject}/versions` |
| GET | `/api/{cluster}/schema/id/{id}` | Find subject by ID | `GET /schemas/ids/{id}/subjects` |
| GET | `/api/{cluster}/schema/{subject}/version` | List versions | `GET /subjects/{subject}/versions` |
| DELETE | `/api/{cluster}/schema/{subject}` | Delete schema | `DELETE /subjects/{subject}` |
| DELETE | `/api/{cluster}/schema/{subject}/version/{version}` | Delete version | `DELETE /subjects/{subject}/versions/{version}` |

### 11.7 Kafka Connect APIs

| Method | Endpoint | Mô tả | Connect API |
|--------|----------|-------|-------------|
| GET | `/api/{cluster}/connect/{connectId}` | List connectors | `GET /connectors` |
| GET | `/api/{cluster}/connect/{connectId}/plugins` | List plugins | `GET /connector-plugins` |
| GET | `/api/{cluster}/connect/{connectId}/plugins/{type}` | Get plugin | `GET /connector-plugins/{type}` |
| PUT | `/api/{cluster}/connect/{connectId}/plugins/{type}/validate` | Validate config | `PUT /connector-plugins/{type}/config/validate` |
| POST | `/api/{cluster}/connect/{connectId}` | Create connector | `POST /connectors` |
| GET | `/api/{cluster}/connect/{connectId}/{name}` | Get connector | `GET /connectors/{name}` |
| DELETE | `/api/{cluster}/connect/{connectId}/{name}` | Delete connector | `DELETE /connectors/{name}` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/tasks` | Get tasks | `GET /connectors/{name}/tasks` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/configs` | Get configs | `GET /connectors/{name}/config` |
| POST | `/api/{cluster}/connect/{connectId}/{name}/configs` | Update configs | `PUT /connectors/{name}/config` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/restart` | Restart connector | `POST /connectors/{name}/restart` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/pause` | Pause connector | `PUT /connectors/{name}/pause` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/resume` | Resume connector | `PUT /connectors/{name}/resume` |
| GET | `/api/{cluster}/connect/{connectId}/{name}/tasks/{taskId}/restart` | Restart task | `POST /connectors/{name}/tasks/{taskId}/restart` |

### 11.8 KsqlDB APIs

| Method | Endpoint | Mô tả | KsqlDB Operation |
|--------|----------|-------|------------------|
| GET | `/api/{cluster}/ksqldb/{ksqlDbId}/info` | Get server info | `serverInfo()` |
| GET | `/api/{cluster}/ksqldb/{ksqlDbId}/streams` | List streams | `SHOW STREAMS` |
| GET | `/api/{cluster}/ksqldb/{ksqlDbId}/tables` | List tables | `SHOW TABLES` |
| GET | `/api/{cluster}/ksqldb/{ksqlDbId}/queries` | List queries | `SHOW QUERIES` |
| PUT | `/api/{cluster}/ksqldb/{ksqlDbId}/queries/pull` | Execute pull query | `executeQuery()` |
| PUT | `/api/{cluster}/ksqldb/{ksqlDbId}/execute` | Execute statement | `executeStatement()` |

### 11.9 ACL APIs

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/acls` | List ACLs | `AdminClient.describeAcls()` |
| GET | `/api/{cluster}/acls/{principal}` | Get ACLs by principal | `AdminClient.describeAcls()` |

### 11.10 Tail API (Real-time Streaming)

| Method | Endpoint | Mô tả | Kafka Operation |
|--------|----------|-------|-----------------|
| GET | `/api/{cluster}/tail/sse` | Tail topics (SSE) | `KafkaConsumer.poll()` (continuous) |



---

## 12. Tóm Tắt Kafka Client APIs Cần Thiết

### 12.1 AdminClient Operations

```java
// Tạo AdminClient
AdminClient adminClient = AdminClient.create(properties);

// Cluster operations
adminClient.describeCluster();

// Topic operations
adminClient.listTopics(new ListTopicsOptions().listInternal(true));
adminClient.describeTopics(topicNames);
adminClient.createTopics(Collection<NewTopic>);
adminClient.deleteTopics(topicNames);
adminClient.createPartitions(Map<String, NewPartitions>);

// Config operations
adminClient.describeConfigs(Collection<ConfigResource>);
adminClient.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>);

// Consumer Group operations
adminClient.listConsumerGroups();
adminClient.describeConsumerGroups(groupIds);
adminClient.listConsumerGroupOffsets(groupId);
adminClient.deleteConsumerGroups(groupIds);
adminClient.deleteConsumerGroupOffsets(groupId, topicPartitions);

// ACL operations
adminClient.describeAcls(AclBindingFilter);

// Log Dir operations
adminClient.describeLogDirs(nodeIds);

// Delete records
adminClient.deleteRecords(Map<TopicPartition, RecordsToDelete>);
```

### 12.2 KafkaConsumer Operations

```java
// Tạo Consumer
KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
    properties,
    new ByteArrayDeserializer(),
    new ByteArrayDeserializer()
);

// Assign partitions
consumer.assign(Collection<TopicPartition>);

// Seek to offset
consumer.seek(TopicPartition, offset);

// Get offsets
consumer.beginningOffsets(Collection<TopicPartition>);
consumer.endOffsets(Collection<TopicPartition>);
consumer.offsetsForTimes(Map<TopicPartition, Long>);

// Poll records
ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeout));

// Commit offsets (for consumer group offset management)
consumer.commitSync(Map<TopicPartition, OffsetAndMetadata>);

// Close
consumer.close();
```

### 12.3 KafkaProducer Operations

```java
// Tạo Producer
KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
    properties,
    new ByteArraySerializer(),
    new ByteArraySerializer()
);

// Send record
RecordMetadata metadata = producer.send(new ProducerRecord<>(
    topic,
    partition,      // Optional
    timestamp,      // Optional
    key,            // byte[]
    value,          // byte[]
    headers         // Iterable<Header>
)).get();

// Close
producer.close();
```

### 12.4 Schema Registry Client Operations

```java
// Tạo client
SchemaRegistryClient client = new CachedSchemaRegistryClient(
    restService,
    cacheSize,
    providers,
    properties,
    null
);

// RestService operations
restService.getAllSubjects();
restService.getLatestVersion(subject);
restService.getAllVersions(subject);
restService.getVersion(subject, version);
restService.registerSchema(schema, schemaType, references, subject);
restService.deleteSubject(headers, subject);
restService.deleteSchemaVersion(headers, subject, version);
restService.getConfig(subject);
restService.updateCompatibility(compatibilityLevel, subject);
restService.testCompatibility(schema, subject, version);
```

---

## 13. Lưu Ý Khi Xây Dựng Hệ Thống Riêng

### 13.1 Connection Management

1. **Cache clients**: AdminClient, Producer nên được cache per cluster
2. **Consumer không cache**: Tạo mới cho mỗi request vì cần assign/seek khác nhau
3. **Thread safety**: AdminClient và Producer là thread-safe, Consumer thì không

### 13.2 Error Handling

```java
// Các exception thường gặp
try {
    // Kafka operations
} catch (ExecutionException e) {
    if (e.getCause() instanceof ClusterAuthorizationException) {
        // Không có quyền truy cập cluster
    } else if (e.getCause() instanceof TopicAuthorizationException) {
        // Không có quyền truy cập topic
    } else if (e.getCause() instanceof SecurityDisabledException) {
        // Security không được enable
    } else if (e.getCause() instanceof TimeoutException) {
        // Request timeout
    } else if (e.getCause() instanceof UnsupportedVersionException) {
        // Kafka version không hỗ trợ operation
    }
}
```

### 13.3 Pagination

AKHQ sử dụng cursor-based pagination cho consume messages:
- `after` parameter chứa map `partition -> offset`
- Mỗi request trả về `after` cursor cho request tiếp theo

### 13.4 Search/Filter

- Search by key: So sánh string trong key
- Search by value: So sánh string trong value
- Search by header: So sánh header key/value
- Timestamp filter: Sử dụng `offsetsForTimes()` để tìm starting offset

### 13.5 Deserialization

AKHQ đọc raw bytes và deserialize ở application layer:
- Plain text: `new String(bytes)`
- Avro: Sử dụng `KafkaAvroDeserializer` với Schema Registry
- JSON Schema: Sử dụng `KafkaJsonSchemaDeserializer`
- Protobuf: Sử dụng `KafkaProtobufDeserializer`

### 13.6 Serialization (Produce)

- Plain text: `string.getBytes()`
- With Schema: Sử dụng `SchemaSerializer` để serialize theo schema từ Schema Registry

---

## 14. Dependencies Chính

```gradle
// Kafka Client
implementation 'org.apache.kafka:kafka-clients:4.0.1'

// Schema Registry
implementation 'io.confluent:kafka-schema-registry-client:8.1.1'
implementation 'io.confluent:kafka-avro-serializer:8.1.1'
implementation 'io.confluent:kafka-json-schema-serializer:8.1.1'
implementation 'io.confluent:kafka-protobuf-serializer:8.1.1'

// Kafka Connect Client
implementation 'org.sourcelab:kafka-connect-client:4.0.3'

// KsqlDB Client
implementation 'io.confluent.ksql:ksqldb-api-client:8.1.1'

// AWS Glue Schema Registry (optional)
implementation 'software.amazon.glue:schema-registry-serde:1.1.21'
```

---

*Tài liệu này được tạo từ source code của AKHQ v0.27.1*
