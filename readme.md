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
- [x] SUB
- [x] HPUB
- [ ] Cluster
  - [x] Learning of cluster servers
  - [X] Try to reconnect to every cluster server
- [ ] Reconnect (still Buggy)
- [ ] SSL
- [ ] Password authentication
- [ ] JWT

JetStream
---------

- [ ] JetStream


Authentication
==============




Changelog
=========

To Implement:

- 0.4.0 JetStream
- 0.3.0 SSL
- ??? subject to distinct string
- 0.2.1 Fix HMSG parser / crash

Implemented:

- 0.2.0 Awaitable request/reply
- 0.1.1 Fix payload bugs
- 0.1.0 basic core NATS features using callbacks.