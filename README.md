# Task 4 - Distributed Transactions (TXs)

In this task, you will extend the distrubuted key-value store by implementing a transaction mechanism on top of it. This will allow to operate on multiple keys in an atomic manner.

## The 2 Phase Commit Protocol (2PC)

To implement transactions on top of our distributed KVS, we use the **2 Phase Commit (2PC)** protocol. For concurrency control we use **2 Phase Locking (2PL)**. Before you start implementing your solution, you should familiarize yourselves with these concepts by watching the lecture or consulting the extensive resources online.

- Task 4 slides [link](https://github.com/TUM-DSE/cloud-lab/blob/main/docs/WS-22/task-4-Background_Distributed_Transactions.pdf)
- Task 4 video [link](https://www.youtube.com/watch?v=i1JniCt2Akg)
- Martin Kleppmann's lecture [link](https://www.youtube.com/watch?v=-_rdWB9hN1c&t=781s)


## 2PC Roles

### Client

The Client submits `join`, `tx_begin`, `tx_commit`, `tx_abort`, `tx_get`, `tx_put`, `tx_del`, requests to the Coordinator.

The code for the Client is provided in the `src` directory.
- `join`, example `ctl-test -a 127.0.0.1:40000 join 127.0.0.1:43000`. Submits a JOIN_CLUSTER request in order for node 127.0.0.1:43000 to join the cluster.
- `tx_begin`, example `ctl-test -a 127.0.0.1:40000 tx_begin tx_id 1 2 3`. Submits a transaction begin request with tx_id as transaction id and 1, 2, 3 as associated transaction keys. This command begins the tx_id transaction and associates the keys 1, 2, 3 with it.
- `tx_commit`, example `ctl-test -a 127.0.0.1:40000 tx_commit tx_id`. Submits a transaction commit request with tx_id as transaction id. This command commits the tx_id transaction and releases any locks on it's associated keys.
- `tx_abort`, example `ctl-test -a 127.0.0.1:40000 tx_abort tx_id`. Submits a transaction abort request with tx_id as transaction id. This command aborts the tx_id transaction, by rolling back all of it's non-commited operations, and releases any locks on it's associated keys.
- `tx_get`, example `ctl-test -a 127.0.0.1:40000 tx_get tx_id 1`. Submits a transaction get request with tx_id as transaction id and 1 as the key from which to receive it's matched value. The key 1 must be one of the associated transaction keys of tx_id transaction.
- `tx_put`, example `ctl-test -a 127.0.0.1:40000 tx_put tx_id 1 10`. Submits a transaction put request with tx_id as transaction id and 1 as the key with which to match the value 10. The key 1 must be one of the associated transaction keys of tx_id transaction.
- `tx_del`, example `ctl-test -a 127.0.0.1:40000 tx_del tx_id 1`. Submits a transaction delete request with tx_id as transaction id and 1 as the key to be deleted from the key-value store. The key 1 must be one of the associated transaction keys of tx_id transaction.

Client example:
```
ctl-test -a 127.0.0.1:40000 join 127.0.0.1:43000
ctl-test -a 127.0.0.1:40000 tx_begin example_tx 10 20
ctl-test -a 127.0.0.1:40000 tx_put example_tx 10 100
ctl-test -a 127.0.0.1:40000 tx_put example_tx 20 200
ctl-test -a 127.0.0.1:40000 tx_get example_tx 10
ctl-test -a 127.0.0.1:40000 tx_del example_tx 20
ctl-test -a 127.0.0.1:40000 tx_commit example_tx
```
### Participant Node

The Participant handles `tx_begin`, `tx_commit`, `tx_abort`, `tx_get`, `tx_put`, `tx_del`, requests sent by the Coordinator and responds back to him. The Participant issues transaction operations on the keys of the distributed (sharded) KVS that he is responsible for.

To run a Participant:
```
kvs-test -a 127.0.0.1:42000 -p 127.0.0.1:43000 -c 127.0.0.1:41000
```
### Coordinator Node

The Coordinator handles `tx_begin`, `tx_commit`, `tx_abort`, `tx_get`, `tx_put`, `tx_del`, requests sent by the Client and forwards these requests to the correct (sharded) Participants based on the associated transaction keys. The Coordinator also receives responses from Participants and then replies back to the Client.

To run the Coordinator:
```
kvs-test -a 127.0.0.1:40000 -r 127.0.0.1:41000 -l
```

## Tasks

Your task is to implement the functions that contain the following annotation:

```TODO (you)```

Additionally, you must include the following messages for the test cases to pass:

- **OK** for a successful join request
- Key and value for a get request, the value should be **ERROR** in case the key doesn't exist

### Task 4.1 - Implement single-node Txs

Explore RocksDB's transactions and recovery mechanisms.
Implement `tx_get`, `tx_put`, `tx_del` operations using RocksDB's transaction mechanisms.

### Task 4.2 - Implement distributed Txs w/o replication

Implement the 2 Phase Commit protocol with the prepare, commit, abort phases.
Add Coordinator node logic to forward the Client requests to the correct (sharded) Participant nodes.
You do not need to worry about replication on task 4.

### Task 4.3 - Implement 2 Phase Locking

Operations should take the locks of their associated keys.
Make sure these locks are released after a transaction commit or abort.

## Tests

### Test 4.1: Correct operation of 2PC

We initialise KVS keys with values.
We perform a transaction on those keys.
We check that the keys hold the expected results.

To execute this test independently, run:
```
python3 tests/test_two_phase_commit.py
```

### Test 4.2: Multiple transaction access on KVS

We initialise KVS keys with values.
We perform 2 transactions at the same time on those keys.
We check that the keys hold the expected results.

To execute this test independently, run:
```
python3 tests/test_concurrent_transactions.py
```

### Test 4.3: Dropping a Participant node

We initialise KVS keys with values.
We change those values during a transaction but we do not commit.
Instead, we kill the participant node and the coordinator.
We bring them back up.
Transaction rollback mechanism should kick in.
We check that the keys hold the initialization values.

To execute this test independently, run:
```
python3 tests/test_transaction_rollback.py
```

### Test 4.4: Client aborts a transaction

We initialise KVS keys with values.
We change those values during a transaction but we do not commit.
Instead, we abort the transaction.
Transaction rollback mechanism should kick in.
We check that the keys hold the initialization values.

To execute this test independently, run:
```
python3 tests/test_transaction_abort.py
```

## Build Instructions

1. Install all required packages:

   ```
   sudo apt-get install build-essential cmake libgtest-dev librocksdb-dev libprotobuf-dev protobuf-compiler libpthread-stubs0-dev zlib1g-dev libevent-dev
   ```

2. Build the source code:

   ```
   mkdir build && cd build/ && cmake .. && make
   ```

## References

* [Concurrency Control and Recovery in Database Systems, Chapter 7](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/05/chapter7.pdf)
* [Martin Kleppmann's lecture](https://www.youtube.com/watch?v=-_rdWB9hN1c&t=781s)
* [Protobufs](https://developers.google.com/protocol-buffers/docs/cpptutorial)
* [RocksDB](http://rocksdb.org/docs/getting-started.html)
