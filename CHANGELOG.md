# Changelog

All notable changes to kafka-nv are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 rule that a breaking change bumps the MINOR number.

## [0.0.1] — 2026-09-17

**The interface, published before anyone implements it.** Every public
type and function carries its full signature, its effect row and its
doc comment; every body is `todo()`; the release is recorded
`implemented = false`.

### Added

- `kafkaproducer` — the load-bearing decision, and it is a row rather
  than a type: `send` is `[]`. It chooses the partition, checks the
  record against the ceilings, appends it to a batch and answers whether
  the batch must now go out; `flush` is the only function in the module
  that writes to a socket, and `should_flush` declares `[time]` alone
  because a linger deadline is a clock read and nothing else. The
  consequence is that the whole of a producer's batching — the
  partitioner, the size ceilings, the compression choice, the record
  that is too large — is a pure function, testable with a thousand
  records of known sizes and no cluster. A producer whose batching is
  entangled with its socket is one whose batching is tested by watching
  a broker's metrics. `KproducerAck` is an enum rather than the
  protocol's `0`, `1` and `-1`, and `with_idempotence` refuses a
  configuration whose `acks` or in-flight limit would make idempotence a
  lie.
- `kafkagroup` — membership as a state machine, and twenty of its
  twenty-four functions are `[]`. The member id, the generation, the
  leadership, the transitions an error code causes, the two
  `ConsumerProtocol` encodings and the leader's assignment computation
  are all pure, so the part of a Kafka client that is hardest to test
  needs no coordinator to test. `on_error` treats
  `MEMBER_ID_REQUIRED`, `REBALANCE_IN_PROGRESS`, `ILLEGAL_GENERATION`
  and `UNKNOWN_MEMBER_ID` as transitions rather than failures, because
  they are how group membership works; a client that surfaced them as
  errors from Kafka would turn the ordinary operation of a consumer
  group into an application-visible fault. `assign` is public and pure
  because the assignor runs in the group's leader and not in the broker.
- `kafkaconn` — the socket, the one-round-trip `ApiVersions` handshake
  and TWO caches. The metadata cache says which broker leads which
  partition; the coordinator cache says which broker manages which
  group. They come from different requests, they move independently and
  they go stale independently, so there are two refresh calls, two
  staleness predicates and two `kafkaclienterror` questions rather than
  one of each. `is_stale` takes the instant instead of reading a clock,
  so a cache policy is a pure function. `KconnTransport` has one arm
  today and is where TLS goes.
- `kafkaconsumer` — the fetch loop, and two decisions this package
  refuses to make for a program. `KconsumerReset` has a third arm,
  `KconsumerRefuse`, and `default_config` uses it: starting at the
  earliest offset replays the retention window and starting at the
  latest loses whatever arrived while the consumer was away, and both
  are catastrophic in some deployment. `auto_commit` is off for the same
  reason — leaving it on chooses at-most-once delivery on the program's
  behalf. `positions_after` keeps the next-offset arithmetic in one
  place, which is the most common off-by-one in the field.
- `kafkaadmin` — the reads a broker answers over `Metadata`,
  `ListOffsets` and `OffsetFetch`, including `group_lag`, which takes
  two requests to two different brokers and is the one number an
  operator asks for. It is here rather than in `kafkaconsumer` because
  the program that wants it is a dashboard, and building a consumer to
  answer a monitoring question would join the group and change what is
  being measured.
- `kafkaclienterror` — three sources of failure kept apart, because a
  codec refusal, a broker error code and a socket failure need three
  different responses. The codec's faults are wrapped whole rather than
  re-declared. Four predicates rather than one: retriable, stale
  metadata, stale coordinator, and whether the socket itself must be
  replaced.

### Known

- **The publish dry-run cannot pass yet, and that is expected.** The
  dependency on kafka-codec-nv is a PATH dependency while the two are
  staged side by side, and `novo pkg publish` refuses a path dependency
  — a path in a published tarball sends a consumer to a directory that
  does not exist. The line becomes `kafka-codec-nv = "^0.0.1"` once
  kafka-codec-nv 0.0.1 is on the registry, and the publish dry-run
  passes then. Every other gate is green today. This is the one gate
  that cannot pass until kafka-codec-nv is published.
- `novo test` is red, and that is this release's expected state: every
  assertion in the five suites reaches `not implemented:
  kafka-nv.<module>.<fn>`.
- **Three administrative writes cannot be implemented over
  kafka-codec-nv 0.0.1.** `kafkaadmin.create_topic`,
  `delete_topic` and `create_partitions` need `CreateTopics`,
  `DeleteTopics` and `CreatePartitions`, which that release does not
  encode. Their signatures are published and they answer
  `KclientNotSupported` naming the key, so the gap is visible in the
  interface rather than hidden behind a `todo()` that will one day do
  something else. `kafkaadmin.supports` answers the same table.
- **No SASL, and therefore no authenticated broker.**
  `SaslHandshake` and `SaslAuthenticate` are not encoded by
  kafka-codec-nv 0.0.1 either. Kafka's SASL exchange is a pair of
  ordinary requests, so this is a later release of that package and then
  of this one, not a redesign of either.
- **No TLS.** `KconnTransport` has one arm. Kafka has no in-band TLS
  negotiation, so the arm a TLS release adds carries its configuration
  and is used from the first byte.
- **No transactions and no `cooperative-sticky` assignor.** Both are
  named in the README's "What is not included", with what each would
  need.
- **The test suites exercise the connecting surface against a closed
  port.** That reaches each function's refusal path and nothing else.
  The implementation's own gate has to be a real Kafka, and a
  three-broker one for leader movement and rebalances, which a single
  broker cannot show.
