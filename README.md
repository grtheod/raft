raft 
====

A toy implementation of raft for consensus in Java using gRPC and RocksDB based on these [blogposts](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/).

## Building
Compile the project:
```
mvn clean compile -DskipTests
```

## Persistence
To enable persistence with RocksDB set the `USE_DISK` flag to true in [RaftConfiguration](src/main/java/org/neo4j/raft/utils/RaftConfiguration.java). However, this makes some tests fail if not run one by one.
