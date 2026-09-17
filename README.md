# kafka-nv

Apache Kafka is a distributed log: programs append records to **topics**
and other programs read them back in order. This package is a Kafka
client for novo-lang — it opens the sockets, sends the requests and
tracks the state a client has to keep. It is built on
[kafka-codec-nv](https://novo-lang.org/packages/kafka-codec-nv), which
is the wire protocol on its own and performs no input or output, and on
`std.net`. The protocol is described in the
[Kafka protocol guide](https://kafka.apache.org/protocol.html).

**Status: NOT IMPLEMENTED — interface only.** Every function is
declared with its full signature, but every body is a `todo()` that
panics when called. The package is published so its design can be
reviewed and depended on before it is implemented. Version 0.1.0 will
be the first working release.

## What a Kafka client has to do

A topic is divided into **partitions**, and each partition is an ordered
sequence of records. Order is guaranteed within a partition and nowhere
else, so which partition a record lands in is the whole of the ordering
a program gets. A record's place in its partition is its **offset**, a
number that only increases.

Each partition is **led** by exactly one broker at a time, and a write
or a read sent to any other broker is refused. So the first thing a
client does is fetch the **metadata**: the list of brokers, and for each
partition which broker leads it. Leadership moves — a broker restarts,
an administrator rebalances — so the metadata goes stale and has to be
refetched.

A **producer** appends records. It rarely sends one at a time: records
for the same partition are collected into a **batch**, and the batch is
sent as one request. How long a batch waits for company is the
**linger**, and it is the setting that trades latency for throughput
most directly. How many replicas must confirm a write before the broker
answers is **acks**, and it is the setting that trades throughput for
durability.

A **consumer** reads records, and it has to remember where it is. It can
remember on its own, or it can join a **consumer group**: a set of
consumers that share a topic's partitions between them, with the cluster
keeping each group's position. A group is managed by one broker, the
**coordinator** — a different lookup from the partition leaders, and one
that goes stale independently.

Joining a group is a handshake. Every member sends a **JoinGroup**; the
coordinator picks one of them as the **leader** and gives it every
member's subscription; the leader computes who reads which partitions
and sends the whole table back; each member is told its share. That is a
**rebalance**, and it happens again whenever a member joins or leaves.
A member that stops sending **heartbeats** for longer than its session
timeout is presumed dead and its partitions are given away.

A group's position is stored by **committing** an offset. The committed
offset is the *next* offset to read, not the last one read.

| Term | Meaning |
| --- | --- |
| Partition | One ordered sequence of records inside a topic |
| Offset | A record's place in its partition |
| Leader | The one broker that serves a partition |
| Metadata | Which broker leads which partition |
| Coordinator | The one broker that manages a group |
| Generation | A number that increases at every rebalance |
| High watermark | The offset one past the last record the leader holds |
| Lag | The high watermark minus a consumer's next offset |

## Install

```
novo pkg add kafka-nv
```

## Example

```novo
use std.bytes
use kafkaclienterror
use kafkaconn
use kafkaconsumer
use kafkaproducer

fn main() [io, net, time]
    // Where to start. Several addresses, because any one of them may
    // be down.
    let options = kafkaconn.with_bootstrap(
                      kafkaconn.default_options("orders-indexer"),
                      [kafkaconn.address("127.0.0.1", 9092)])

    match kafkaconn.bootstrap(options)
        Err(e) => println("no broker answered: ${kafkaclienterror.message(e)}")
        Ok(conn) =>
            // Which broker leads which partition. Everything else
            // needs this.
            match kafkaconn.fetch_metadata(conn, ["orders"])
                Err(e)   => println("no metadata: ${kafkaclienterror.message(e)}")
                Ok(meta) =>
                    let p = kafkaproducer.producer(meta.conn, meta.cluster,
                                                   kafkaproducer.default_config())

                    // This buffers. Nothing has been sent yet.
                    match kafkaproducer.send(p, kafkaproducer.message("orders",
                                                 bytes.from_str("order-7"),
                                                 bytes.from_str("{}")))
                        Err(e) => println("refused: ${kafkaclienterror.message(e)}")
                        Ok(a)  =>
                            // This is the call that writes to the socket.
                            match kafkaproducer.flush(a.producer)
                                Err(e) => println("not written: ${kafkaclienterror.message(e)}")
                                Ok(f)  => println("${list.len(f.results)} partitions written")
```

Build and test with `novo pkg build` and `novo test`. Today `novo test`
fails on purpose: every test reaches a
`not implemented: kafka-nv.<module>.<fn>` panic. The tests are the
specification the implementation will have to satisfy.

## What the package contains

| Module | Contents |
| --- | --- |
| `kafkaclienterror` | Every way a program can fail to talk to a cluster, and what each one means it should do next. |
| `kafkaconn` | The socket, the version handshake, the metadata cache and the coordinator cache. |
| `kafkaproducer` | Batching, partitioning, acknowledgement policy and the flush. |
| `kafkaconsumer` | The fetch loop, the positions, the reset policy and the commits. |
| `kafkagroup` | Group membership as a state machine, the subscription and assignment encodings, and the leader's assignment computation. |
| `kafkaadmin` | What a broker will tell you about itself: brokers, topics, partition ends and a group's lag. |

## How to choose an entry point

**`kafkaconn.bootstrap` is how every program starts.** It tries each
address in turn, connects, and completes the version handshake.

**`kafkaproducer` is for writing.** `send` buffers a record and `flush`
writes; a program that wants each record on the wire immediately sets
`linger_ms` to `0` and flushes after every send.

**`kafkaconsumer` is for reading as part of a group.**
`kafkaconsumer.standalone_config` is the same consumer with no group: it
keeps its own positions, commits nothing and is never rebalanced. Take
that one for a repair tool or a one-off read.

**`kafkagroup` is for a program that manages its own membership.** A
stream processor that assigns partitions by its own rules uses `join`,
`assign`, `sync` and `heartbeat` directly rather than through
`kafkaconsumer`.

**`kafkaadmin` is for a program that reports rather than reads.** A lag
dashboard built on a consumer would join the group and change the thing
it is measuring; `kafkaadmin.group_lag` does not.

## The rules a user needs

1. **`kafkaproducer.send` does not send.** It chooses the partition,
   checks the size and buffers. `kafkaproducer.flush` is the only
   function in that module that writes to a socket.
2. **Order exists within a partition and nowhere else.** A record with a
   key always goes to the same partition; a record with no key is
   spread. `kafkaproducer.message_without_key` is spelled separately
   from `kafkaproducer.message` for that reason.
3. **`acks` of `KproducerAckNone` gets no response from the broker.**
   Not an empty one — none. A flush under that setting cannot report
   which records were written, because nothing comes back.
4. **An idempotent producer requires `KproducerAckAll` and at most five
   requests in flight.** `kafkaproducer.with_idempotence` refuses a
   configuration that breaks either, naming the field. Kafka protocol
   guide, "Idempotent Producer".
5. **There is no safe default for a consumer with no position.**
   `KconsumerRefuse` is what `default_config` uses: starting at the
   earliest offset replays the whole retention window and starting at
   the latest skips whatever arrived while the consumer was away. Choose
   with `kafkaconsumer.with_auto_offset_reset`.
6. **Committing before processing is at-most-once; committing after is
   at-least-once.** There is no third option without transactions, and
   this release has none. `auto_commit` is off in `default_config`
   because leaving it on chooses the first.
7. **A committed offset is the next offset to read.**
   `kafkaconsumer.positions_after` computes it from the records that
   were processed. Committing the last offset processed reprocesses one
   record per partition after every restart.
8. **An uncommitted partition reports `-1`, not `0`.** A group that has
   never committed for a partition is sent to its reset policy.
   `kafkagroup.is_uncommitted` is the check.
9. **The connection's `request_timeout_ms` must exceed the consumer's
   `max_wait_ms`.** A fetch is a long poll that holds the connection
   open; a shorter request deadline tears the connection down and
   reconnects, having fetched nothing.
   `kafkaconsumer.check_config` compares the two.
10. **Heartbeats are the caller's.** This package starts no background
    task and declares no `[async]`. `kafkaconsumer.poll` heartbeats on
    the way past, and `kafkaconsumer.heartbeat_due` says when one is
    owed. A program whose processing takes longer than
    `session_timeout_ms` between polls must call
    `kafkagroup.heartbeat` itself, or the coordinator will evict it.
11. **`KafkaErrRebalanceInProgress`, `KafkaErrMemberIdRequired`,
    `KafkaErrIllegalGeneration` and `KafkaErrUnknownMemberId` are
    routine.** They are how group membership works, not failures.
    `kafkagroup.is_membership_event` recognises them and
    `kafkagroup.on_error` applies them.
12. **A rebalance invalidates the previous assignment's work.**
    `KconsumerPolled.rebalanced` is true when one happened during the
    poll; the records from before it must not be committed, because
    those partitions belong to another member now.
13. **Metadata and the group coordinator are two caches.**
    `kafkaclienterror.needs_metadata_refresh` and
    `needs_coordinator_refresh` answer separately, and refreshing the
    wrong one leaves the failure in place.
14. **Metadata goes stale without anything failing.**
    `metadata_max_age_ms` defaults to five minutes. A partition can move
    while every request still succeeds.
15. **A codec fault that loses the framing needs a new socket.** The
    framing is a four-byte length with no magic number, so a stream
    whose ordering has been lost cannot be resynchronised.
    `kafkaclienterror.needs_reconnect` is the check.
16. **Compression codecs are supplied by the caller.** Pass flate-nv's,
    snappy-nv's, lz4-nv's or zstd-nv's functions to
    `kafkaconn.with_codecs`. A program that reads only uncompressed
    topics links no compression library.
17. **Retry jitter is a parameter, not a random number.**
    `kafkaproducer.with_retries` takes it, so that a test can replay a
    retry schedule exactly.
18. **Adding partitions to a topic moves every keyed record.** The
    partition is `hash(key) mod count`, so a topic whose ordering
    matters cannot be widened without a plan for the records already in
    it.

## What this release cannot do

Three administrative writes are declared and answer
`KclientNotSupported` naming the API key they need, because
kafka-codec-nv 0.0.1 does not encode it. They are declared rather than
omitted so that the shape does not change when the codec grows the key.

| Function | Needs |
| --- | --- |
| `kafkaadmin.create_topic` | `CreateTopics` |
| `kafkaadmin.delete_topic` | `DeleteTopics` |
| `kafkaadmin.create_partitions` | `CreatePartitions` |

`kafkaadmin.supports` and `kafkaadmin.unsupported_operations` answer the
same table, so a program that builds a menu of what it can offer does
not hard-code this list a second time.

## What is not included

- **TLS.** `KconnTransport` has one arm, `KconnPlaintext`. Kafka has no
  in-band TLS negotiation — a port is either TLS or it is not — so a TLS
  arm carries its configuration and is used from the first byte. It is a
  later release.
- **SASL.** `SaslHandshake` and `SaslAuthenticate` are not encoded by
  kafka-codec-nv 0.0.1, so a broker that requires authentication cannot
  be connected to. Kafka's SASL exchange is a pair of ordinary requests,
  so it is a later release of that package and then of this one.
- **Transactions.** The transactional request set is not encoded, so
  there is no exactly-once producer and no transactional offset commit.
  A `read_committed` consumer still works: it skips the records of open
  and aborted transactions, which needs no transactional requests of its
  own.
- **The `cooperative-sticky` assignor.** `kafkagroup` implements `range`
  and `roundrobin`. Both revoke every partition at every rebalance.
  `cooperative-sticky` does not, which is what a large group wants, and
  it needs a second rebalance round and a revocation callback — a shape
  this release does not have.
- **A background thread.** No module here declares `[async]` or starts a
  task. Heartbeats, metadata refreshes and retries all happen on the
  calling thread, during a call the program made. See rule 10.
- **A connection pool.** One `Kconn` is one socket to one broker. A
  client talking to several brokers holds several, and how they are kept
  is the program's.
- **Randomness.** No module declares `[rand]`. Retry jitter is a
  parameter; see rule 17.
- **Printing.** No module declares `[io]`. Every function returns what
  happened rather than reporting it.
- **The wire protocol.** It is kafka-codec-nv, which this package does
  not duplicate: every byte out came from `kafkaframe.send` and every
  byte in goes to `kafkaframe.feed`.

## Related packages

- [kafka-codec-nv](https://novo-lang.org/packages/kafka-codec-nv) is the
  protocol without the sockets: request and response encodings, record
  batches, and the state machine this package pumps. Take it on its own
  to read a captured frame, write a broker test double, or read log
  segments off disk.
- [flate-nv](https://novo-lang.org/packages/flate-nv),
  [snappy-nv](https://novo-lang.org/packages/snappy-nv),
  [lz4-nv](https://novo-lang.org/packages/lz4-nv) and
  [zstd-nv](https://novo-lang.org/packages/zstd-nv) are the four
  compression codecs. See rule 16.
- [redis-nv](https://novo-lang.org/packages/redis-nv) and
  [postgres-nv](https://novo-lang.org/packages/postgres-nv) are the
  other two binary-protocol clients on the registry. Each keeps its
  codec as a module rather than as a separate package.

## Tests

The suites are written against the signatures and are red until the
bodies land. `kafkaconn_tests` asserts the defaults, the refusals in
`check_options` and the cluster snapshot's lookups.
`kafkaproducer_tests` asserts that `send` buffers, that the partitioner
and the ceilings answer without a broker, and that idempotence refuses
the configurations it cannot support. `kafkaconsumer_tests` asserts that
neither the reset policy nor the commit policy has been chosen for the
program. `kafkagroup_tests` drives the membership state machine and the
assignment computation with no coordinator.
`kafkaclientcover_tests` asserts the failure taxonomy and that the three
administrative writes refuse by name.

Everything that needs a broker is exercised against a port nothing is
listening on, which reaches each function's refusal path. The
implementation's own gate has to be a real Kafka: a single-broker
container for the produce and fetch paths, and a three-broker one for
leader movement and rebalances, which are the two behaviours a single
broker cannot show.

## Implementation status

| Module | Status |
| --- | --- |
| `kafkaclienterror` | Declared. Every body is a `todo()`. |
| `kafkaconn` | Declared. Every body is a `todo()`. |
| `kafkaproducer` | Declared. Every body is a `todo()`. |
| `kafkaconsumer` | Declared. Every body is a `todo()`. |
| `kafkagroup` | Declared. Every body is a `todo()`. |
| `kafkaadmin` | Declared. Every body is a `todo()`. Three functions will answer a refusal even once implemented — see "What this release cannot do". |

## Licence

Apache-2.0. See [LICENSE](LICENSE).
