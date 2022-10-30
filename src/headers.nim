import std/enumerate
import strformat
import strutils, parseutils
import types
type
  NatsHeaders* = seq[tuple[key, val: string]]


proc `$`*(headers: NatsHeaders): string =
  result = ""
  result &= natsHeaderMagic & crlf
  for (key, val) in headers:
    result &= fmt"{key}: {val}{crlf}"

# proc `%*`*(headers: NatsHeaders): JsonNode =
#   var elems: seq[seq[string]] = @[]
#   for (key, val) in headers:
#     elems.add @[key, val]
#   return %* elems

proc `[]`*(headers: NatsHeaders, key: string): seq[string] =
  # return all values with the key `key`
  for (k, val) in headers:
    if k == key:
      result.add val

proc `[]=`*(headers: var NatsHeaders, key, value: string | seq[string]) =
  # TODO clean this up, it's is ugly af.
  if headers.len == 0:
    when value is string:
      headers.add (key, value)
    else:
      for val in value:
        headers.add (key, val)
  else:
    # Update a key
    for idx in 0..headers.len:
      if headers[idx].key == key:
        when value is string:
          headers[idx].val = value
        else:
          for val in value:
            headers.add (key, val)
        return
    # if not there, set it
    when value is string:
      headers.add (key, value)
    else:
      for val in value:
        headers.add (key, val)

proc parseHeaders*(str: string): tuple[headers: NatsHeaders, errorNumber: int, errorMessage: string] =
  ## Parses nats header
  #NATS/1.0\r\nfoo: baa
  if str.len == 0: return
  var lines = str.splitLines()
  if lines.len == 0: return
  if not lines[0].startsWith(natsHeaderMagic):
    # if nats.debug:
    # echo "[client]: invalid header magic: " & lines[0]
    return
  # Extract error number and errorMessage, if any.
  if lines[0].len > natsHeaderMagic.len:
    # We have additional data
    var pos = natsHeaderMagic.len + 1 # " "
    pos += parseInt(lines[0], result.errorNumber, pos)
    pos += skipWhile(lines[0], {' '}, pos) # skip " "
    result.errorMessage = lines[0][pos .. ^1]

  for line in lines[1 .. ^1]:
    var pos = 0
    var key = ""
    var val = ""
    pos += line.parseWhile(key, validHeaderKeyChars, pos)
    let colonFound = line.skip(":", pos)
    if colonFound == 0:
      # echo "ERROR No colon found... ", line
      continue
    pos += colonFound
    pos += line.skip(" ", pos) # optional whitespace
    pos += line.parseWhile(val, validHeaderValueChars, pos) # TODO maybe just use the rest?
    result.headers.add (key, val)

when isMainModule:
  import unittest

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