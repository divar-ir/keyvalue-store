# Key Value Store

A distributed cache using Redis backend.

This is an open-source version of the propriatory system we have in use for our
production systems.

## Motivation

According to the [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem) out of 3 following
properties, a distributed system can exhibit two out of three at the same time:

1. Consistency
2. Availability
3. Partition Tolerance

We at [Divar](https://divar.ir) have been using Redis extensively for micro-service caches in
a distributed fashion. But we have always had problems with uptime of our caches mainly because
our infrastructure is based on Kubernetes. We've had problems with updating endpoints, leader election,
etc.

Knowing that an AP (Available, Partition Tolerant) system will do well for our cache systems, we have
developed a wrapper around Redis that does the following:

1. All writes are proxied to all redis instances. KeyValueStore will wait for a number of minimum acknowledges
   dictated by `Consistency` level (a.k.a. One, Majority, All) before responding to client. Remaining writes are
   performed in background.
2. If a write failes to meet minimum consistency required level, a rollback operation undo'es operaiton on successful
   nodes
3. All reads are queried from all nodes. A comparer checks values against each other and returns the first value
   wich matches `Consistency` level (a.k.a. One, Majority, All).
4. After reading data, nodes that had conflicting values or did not have value at all will be repaired in background
   using a **read repair** operation. A read repair operation is like a write operation run in background that has
   rollback too in case a read-repair operation fails.

We have following consistency levels:
* **One:** This mode is preferred for caches. This consistency level means ensuring that writes are done to at least 1
  node and reading from the fastest node possible. However, given that while reading a lot of nodes might not contain data,
  we have implemented **policies** that resolve this issue and we will discuss it.
* **Majority:**: Writes and reads should be consistent with the quorom of the nodes.
* **All:** All reads/writes should be consistent with all nodes. This mode is not recommended.

We have following policies for handling **One** consistency level:
* **readone-firstavailable**: This policy is preferred. It does not take into account nodes that don't
  have the data and will keep waiting for data. This resolves the issue with nodes that don't have any data.
* **readone-localorrandomnode**: This policy will return result from fastest node possible.

### Cluster Discovery

Currently, a static discovery has been implemented only. Redis instances has to be stated manually.
KeyValueStore accepts a comma-seperated list of redis instances to connect to.

## Building

Simply run `go build ./cmd/keyvaluestored`

## Testing

Simply run `go test ./...`

## Running

Run `./keyvaluestored -c config.example.json serve`. This will start a redis-proxy in port `6380`.
Connect to it using:

```bash
redis-cli -h 127.0.0.1 -p 6380
127.0.0.1:6380> set a b
"OK"
127.0.0.1:6380> get a
"b"
```

## Supported redis-commands

The following redis-commands are supported by the proxy.

* SET
* DEL
* GET
* MGET
* MSET
* PING
* ECHO
* SETNX
* SETEX
* EXISTS
* TTL
* PTTL
* EXPIRE
* PEXPIRE
* EXPIREAT
* PEXPIREAT
* SELECT
* FLUSHDB

## License

This product is protected by MIT License. See [license](LICENSE).
