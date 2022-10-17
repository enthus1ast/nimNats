# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest
import nimNats

suite "header parsing":
  test "header1":
    let t1 = "NATS/1.0\r\nfoo: baa"
    check t1.parseHeaders() == @[("foo", "baa")]
  test "header2":
    let t1 = "NATS/1.0\r\nfoo: baa\nfoo: baa"
    check t1.parseHeaders() == @[("foo", "baa"), ("foo", "baa")]
  test "header3":
    let t1 = "NATS/1.0\r\nfoo: baa\nfoo: zaa"
    check t1.parseHeaders() == @[("foo", "baa"), ("foo", "zaa")]
  test "header4 (no space)":
    let t1 = "NATS/1.0\r\nfoo:baa\nfoo:zaa"
    check t1.parseHeaders() == @[("foo", "baa"), ("foo", "zaa")]
  test "header invalid1 (wrong magic)":
    let t1 = "NATS/1.1\r\nfoo: baa\nfoo: zaa"
    check t1.parseHeaders().len == 0
  test "header invalid2 (magic missing)":
    let t1 = "foo: baa\nfoo: zaa"
    check t1.parseHeaders().len == 0
  test "header invalid3 (disallowed space)":
    let t1 = "NATS/1.0\r\nfoo : baa\nfoo : zaa"
    check t1.parseHeaders().len == 0
  test "header invalid4 (disallowed space, ignore wrong header line)":
    let t1 = "NATS/1.0\r\nfoo : baa\nfoo: zaa"
    # echo t1.parseHeaders()
    check t1.parseHeaders() == @[("foo", "zaa")]
