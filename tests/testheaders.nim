import unittest
import nats/headers

suite "headers":
  test "1":
    var hh: NatsHeaders
    hh["foo"] = "baa"
    check hh["foo"] == @["baa"]
  test "2":
    var hh: NatsHeaders
    hh["foo"] = @["baa", "baz"]
    check hh["foo"] == @["baa", "baz"]
  test "parseHeaders; error messages":
    let (headers, errorNumber, errorMessage) = parseHeaders("NATS/1.0 404 Message Not Found")
    check headers.len == 0
    check errorNumber == 404
    check errorMessage == "Message Not Found"
  test "parseHeaders; error messages; 2":
    let (headers, errorNumber, errorMessage) = parseHeaders("NATS/1.0 404")
    check headers.len == 0
    check errorNumber == 404
    check errorMessage == ""
  test "parseHeaders; error messages; 3":
    let (headers, errorNumber, errorMessage) = parseHeaders("NATS/1.0 404 ")
    check headers.len == 0
    check errorNumber == 404
    check errorMessage == ""

  test "header1":
    let t1 = "NATS/1.0\r\nfoo: baa"
    check t1.parseHeaders() == (@[("foo", "baa")], 0, "")
  test "header2":
    let t1 = "NATS/1.0\r\nfoo: baa\nfoo: baa"
    check t1.parseHeaders() == (@[("foo", "baa"), ("foo", "baa")], 0, "")
  test "header3":
    let t1 = "NATS/1.0\r\nfoo: baa\nfoo: zaa"
    check t1.parseHeaders() == (@[("foo", "baa"), ("foo", "zaa")], 0, "")
  test "header4 (no space)":
    let t1 = "NATS/1.0\r\nfoo:baa\nfoo:zaa"
    check t1.parseHeaders() == (@[("foo", "baa"), ("foo", "zaa")], 0, "")
  test "header invalid1 (wrong magic)":
    let t1 = "NATS/1.1\r\nfoo: baa\nfoo: zaa"
    check t1.parseHeaders().headers.len == 0
  test "header invalid2 (magic missing)":
    let t1 = "foo: baa\nfoo: zaa"
    check t1.parseHeaders().headers.len == 0
  test "header invalid3 (disallowed space)":
    let t1 = "NATS/1.0\r\nfoo : baa\nfoo : zaa"
    check t1.parseHeaders().headers.len == 0
  test "header invalid4 (disallowed space, ignore wrong header line)":
    let t1 = "NATS/1.0\r\nfoo : baa\nfoo: zaa"
    # echo t1.parseHeaders()
    check t1.parseHeaders().headers == @[("foo", "zaa")]
  test "header to string":
    let t1 = "NATS/1.0\r\nfoo:baa\r\nfoo: zaa"
    let h1 = t1.parseHeaders().headers
    # echo t1.parseHeaders()
    check "NATS/1.0\r\nfoo: baa\r\nfoo: zaa\r\n" == $h1
