nimNats
=======

A [NATS.io](https://nats.io/) client in pure Nim.

Quality: wip

installation
============

nimble install nats

Features
========

NatsCore
---------

- [x] PUB
- [x] HPUB
- [x] SUB
  - [x] Callback Style
  - [ ] Async Style
- [x] Request / Reply
  - [x] Async Style
- [ ] Cluster
  - [x] Learning of cluster servers
  - [x] Try to reconnect to every cluster server
- [ ] Reconnect (still Buggy)
- [x] SSL
  - [x] Self signed, not configurable
  - [ ] Robust, configurable
- [x] Password authentication
- [ ] JWT

JetStream
---------

- [ ] JetStream
- [x] Key Value (KV)



Changelog
=========

To Implement:

- ??? subject to distinct string
- ??? JetStream ObjectStore
- ??? JetStream MsgQueue
- ??? JetStream Beginning
- ??? Core Nats example applications
- ??? Document Core Nats api usage
- ??? Option To Enforce TLS

Implemented:

- 0.2.4 SSL
- 0.2.3 JetStream KV
- 0.2.2 Added user callbacks
- 0.2.1 Fix HMSG parser / crash
- 0.2.0 Awaitable request/reply
- 0.1.1 Fix payload bugs
- 0.1.0 basic core NATS features using callbacks.