nimNats
=======

A basic [NATS.io](https://nats.io/) client in pure Nim.

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
- [/] Reconnect
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

- 3.0.0 JetStream
- 2.0.0 SSL
- 1.0.0 Awaitable request/reply
- ??? subject to distinct string
- 0.1.1 Fix payload bugs

Implemented:

- 0.1.0 basic core NATS features using callbacks.