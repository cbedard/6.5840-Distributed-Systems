# 6.5840 - Distributed-Systems

Collection of labs from MIT's upper-undergrad/graduate distributed systems course, previously coded as 6.824. All labs are written in Go and have a set of tests and tooling for simulating distributed environments and verifying program correctness. I'm using the edition of the course published in Spring 2024 ([archive link](http://nil.csail.mit.edu/6.5840/2024/)), with lectures from 2020 ([link](https://www.youtube.com/@6.824/videos)). Big thank you to MIT for open-sourcing incredibly high quality learning resources like this.

## Labs
### Lab 1: MapReduce
Single-machine / multi-process MapReduce, simulating network communication with unix domain sockets.

### Lab 2: Key/Value Server & Client
A key-value server & client with retries and a handshake style verification. Tested using a simulation tool for unreliable connections.

### Lab 3: Implementing Raft
[Raft](https://raft.github.io/raft.pdf) is a consensus algorithm for replicated state machines. 

### Lab 4: Fault-tolerant Key/Value Server
TBD, Leader/followers server cluster using Raft.

### Lab 5: Sharded K/V
TBD, like lab 4 but we shard our K/V Service for *big data*.