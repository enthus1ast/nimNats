import nats, unittest
include nats/kv


suite "kv":
  test "kv subjects":
    check splitKvSubject("$KV.tbuck.aa") == ("$KV", "tbuck", "aa")
    check splitKvSubject("$KV.tbuck.aa.aa.aa") == ("$KV", "tbuck", "aa.aa.aa")
