## This is the core nats, PUB, SUB etc

import asyncnet, asyncdispatch, uri, strutils, json, strformat,
  tables, print, parseutils, random, sets, sequtils, net

import std/enumerate
import std/importutils

import constants
import headers
import types

when defined ssl:
  import openssl
  import os

export uri

privateAccess(Nats)

# Forward declarations
proc connect*(nats: Nats, urls: seq[Uri]): Future[void]

proc jetstreamAvailable*(nats: Nats): bool =
  ## returns true if the server supports jetstream
  if not nats.info.hasKey("jetstream"): return false
  return nats.info["jetstream"].getBool()

proc getConnectUrls(js: JsonNode): seq[Uri] =
  if not js.isNil and js.hasKey("connect_urls"):
    for elem in js["connect_urls"]:
      result.add parseUri(elem.getStr())

proc getCurrentServer*(nats: Nats): Uri =
  ## returns the server we're currently connected to.
  return nats.currentServer

proc getAllKnownServers*(nats: Nats): seq[Uri] =
  ## return all the servers we where started with, and
  ## all servers we have learned that are in the current cluster.
  raise newException(NotImplementedError, "getAllKnownServers")

# Default handlers
proc defaultHandlerError*(nats: Nats, msg: string) {.async.} =
  raise newException(ValueError, msg)

proc defaultHandlerInfo*(nats: Nats, info: JsonNode) {.async.} =
  when not defined(release):
    echo info.pretty()

proc defaultHandlerDisconnected*(nats: Nats) {.async.} =
  when not defined(release):
    echo "[defaultHandlerDisconnected] Disconnected"

proc defaultHandlerServersGained*(nats: Nats, servers: seq[Uri]) {.async.} =
  when not defined(release):
    echo "Servers Discovered"
    for server in servers:
      echo server

proc defaultHandlerServersLost*(nats: Nats, servers: seq[Uri]) {.async.} =
  when not defined(release):
    echo "Servers Lost"
    for server in servers:
      echo server

proc genSid(nats: Nats): Sid =
  nats.lastSidInt.inc
  return $nats.lastSidInt

proc genInbox*(nats: Nats): Inbox =
  ## generates a new unique inbox
  # TODO store inboxes, to avoid collisions?
  return inboxPrefix & align($rand(int.high), inboxRandomLen, '0')

proc `$`*(msg: MsgPub): string =
  fmt"{PUB} {msg.subject} {msg.replyTo} {msg.payload.len}{crlf}{msg.payload}{crlf}"

proc `$`*(msg: MsgHpub): string =
  # HPUB <subject> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊
  let headerstr = $msg.headers
  fmt"{HPUB} {msg.subject} {msg.replyTo} {headerstr.len + crlf.len} {(headerstr.len + crlf.len) + msg.payload.len}{crlf}{headerstr}{crlf}{msg.payload}{crlf}"

proc `$`*(msg: MsgConnect): string =
  fmt"{CONNECT} {$ %* msg}{crlf}"

proc `$`*(msg: MsgSub): string =
  fmt"{SUB} {msg.subject} {msg.queueGroup} {msg.sid}{crlf}"

proc `$`*(msg: MsgUnsub): string =
  fmt"{UNSUB} {msg.sid} {msg.max_msgs}{crlf}"

proc newNats*(reconnect = true, debug = false): Nats =
  ## Creates a new `Nats` object
  randomize()
  result = Nats()
  result.info = %* {} # to avoid SIGSEGV: Illegal storage access
  result.debug = debug
  result.lastSidInt = 0
  result.reconnect = reconnect
  result.connectionStatus = disconnected
  result.handlerError = defaultHandlerError
  result.handlerInfo = defaultHandlerInfo
  result.handlerDisconnected = defaultHandlerDisconnected
  result.handlerServersGained = defaultHandlerServersGained
  result.handlerServersLost = defaultHandlerServersLost

proc splitMessage(str: string): tuple[verb: Verb, rest: string] =
  let parts = str.split(" ", 1)
  if parts.len == 0:
    raise newException(ParsingError, "malformed response: " & str)
  try:
    result.verb = parseEnum[Verb](parts[0])
  except:
    raise newException(ParsingError, "unknown verb: " & parts[0] & " Details: " & str)
  if parts.len >= 2:
    result.rest = parts[1]
  else:
    result.rest = ""

proc send*(nats: Nats, str: string) {.async.} =
  ## low level sending proc. Use the `$` proc's from the different `Msg*`.
  if nats.debug:
    echo "[send]: ", str.strip() # not accurate but less clutter in the console
  await nats.sock.send(str)

proc handleMsgERR(nats: Nats, rest: string) {.async.} =
  if not nats.handlerError.isNil:
    await nats.handlerError(nats, rest)

proc handleMsgPING(nats: Nats, rest: string) {.async.} =
  if nats.debug:
    echo "[client] send pong"
  await nats.send "PONG" & crlf

proc handleMsgPONG(nats: Nats, rest: string) {.async.} =
  discard #TODO reset the timeout counter

proc computeServersGainedAndLost(nats: Nats, info: JsonNode): tuple[gained, lost: seq[Uri]] =
  let newServerSet = info.getConnectUrls().toHashSet()
  let oldServerSet = nats.info.getConnectUrls().toHashSet()
  if newServerSet == oldServerSet:
    # everything is identically, nothing to do.
    discard
  else:
    let serversGained = newServerSet - oldServerSet
    let serversLost = oldServerSet - newServerSet
    return (serversGained.toSeq(), serversLost.toSeq())


proc handleMsgINFO(nats: Nats, rest: string) {.async.} =
  # Get informations from the server, this way we also learn about
  # new cluster servers.
  var info: JsonNode = %* {} # provide an empty default
  try:
    info = parseJson(rest)
  except:
    raise newException(ParsingError, "could not parse the server info: " & rest)

  # Inform the user about servers gained and lost
  let (serversGained, serversLost) = nats.computeServersGainedAndLost(info)
  if not nats.handlerServersGained.isNil and serversGained.len > 0:
    await nats.handlerServersGained(nats, serversGained)
  if not nats.handlerServersLost.isNil and serversLost.len > 0:
    await nats.handlerServersLost(nats, serversLost)

  # Call the user defined handler
  if not nats.handlerInfo.isNil:
    if nats.debug: echo "Call user handlerInfo"
    await nats.handlerInfo(nats, info)

  if nats.debug: echo "Set info to obj"
  nats.info = info # we use the info that was passed to the handler, so a user can override some values.
  echo nats.info

  # Learn of new cluster nodes.
  # if nats.info.hasKey("connect_urls"):
  #   for elem in nats.info["connect_urls"]:
  #     echo elem
  for server in nats.info.getConnectUrls():
    echo "Cluster Server: " & $server



proc handleMsgMSG(nats: Nats, rest: string) {.async.} =
  # MSG is more complex since we must parse it but then also read
  # the payload
  # MSG <subject> <sid> [reply-to] <#bytes>␍␊[payload]␍␊
  let parts = rest.split(" ")
  var subject = ""
  var sid: Sid = ""
  var bytes = 0
  var replyTo = ""
  # no reply to
  subject = parts[0]
  sid = parts[1]
  if parts.len == 4:
    replyTo = parts[2]
  try:
    bytes = parseInt(parts[^1])
  except:
    raise newException(ParsingError, "could not parse bytes from msg: " & rest)

  # parse the payload
  var payload = await nats.sock.recv(bytes)
  payload.setLen(payload.len) # remove crlf

  #print subject, sid, bytes, replyTo, payload
  # recv the crlf
  let cc = (await nats.sock.recv(crlf.len))
  assert cc == crlf

  # call the callback
  if nats.subscriptions.hasKey(sid):
    let msg = MsgHmsg(sid: sid, subject: subject, payload: payload, headers: @[], replyTo: replyTo)
    await nats.subscriptions[sid].callback(nats, msg)
  else:
    if nats.debug: echo "[err] no registered subscription!"


proc handleMsgHMSG(nats: Nats, rest: string) {.async.} =
  # HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>␍␊[payload]␍␊
  let parts = rest.split(" ") # TODO avoid split etc.
  let subject = parts[0]
  let sid = parts[1]
  let replyTo =
    if parts.len == 5:
      parts[2]
    else:
      ""
  let headerBytes = parts[^2].parseInt()
  let totalBytes = parts[^1].parseInt()
  let wholeBody = await nats.sock.recv(totalBytes + crlf.len)
  let headerBody = wholeBody[0 .. headerBytes]
  let payload = wholeBody[headerBytes .. ^3] # remove the crlf
  let (headers, errorNumber, errorMessage) = parseHeaders(headerBody)
  # print subject, sid, replyTo, headerBytes,totalBytes, wholeBody, headerBody, payload, headers
  # call the callback
  if nats.subscriptions.hasKey(sid):
    let msg = MsgHmsg(sid: sid, subject: subject, payload: payload, headers: headers,
      replyTo: replyTo, errorNumber: errorNumber, errorMessage: errorMessage)
    await nats.subscriptions[sid].callback(nats, msg)
  else:
    if nats.debug: echo "[err] no registered subscription!"


proc handleMessages*(nats: Nats) {.async.} =
  ## This ist the main message handler.
  ## Start this yourself
  nats.isHandelingMessages = true
  if nats.debug: echo "[client] start handleMessages"
  # while nats.connected:
  while nats.connectionStatus != disconnected:
    # Wait for message
    let line = await nats.sock.recvLine()
    if nats.debug:
      echo "[recv]: ", line
    if line.len == 0:
      if nats.debug: echo "[server] disconnect"
      if not nats.handlerDisconnected.isNil:
        await nats.handlerDisconnected(nats) # Call disconnection handler
      if nats.reconnect:
        nats.connectionStatus = reconnecting
        # Automatic reconnect
        echo "[client] reconnect"
        # TODO make this better
        var urls: seq[Uri] = @[nats.currentServer] # in every case try the current server as well.
        if nats.info.hasKey("connect_urls"):
          for elem in nats.info["connect_urls"]:
            urls.add parseUri("nats://" & elem.getStr())
        await nats.connect(urls)
        # Tell the new server what we have subscribed
        for subscription in nats.subscriptions.values:
          echo "[client]: resubscribe"
          # resend
          let sub = MsgSub(
            subject: subscription.subject,
            queueGroup: subscription.queueGroup,
            sid: subscription.sid
          )
          await nats.send($sub)
        continue
      else:
        nats.connectionStatus = disconnected
        return

    let (verb, rest) = splitMessage(line)
    case verb
    of `+OK`: discard # what else to do with them?
    of `-ERR`: await nats.handleMsgERR(rest)
    of PING: await nats.handleMsgPing(rest)
    of PONG: await nats.handleMsgPONG(rest)
    of INFO: await nats.handleMsgInfo(rest)
    of MSG: await nats.handleMsgMsg(rest)
    of HMSG: await nats.handleMsgHMSG(rest)
    else:
      if nats.debug:
        echo "[client] no handler for verb: ", verb

proc getPort(url: Uri): Port =
  ## either return the port from the uri, or the default port.
  if url.port.len == 0:
    defaulPort.Port
  else:
    parseInt(url.port).Port

proc connect*(nats: Nats, urls: seq[Uri]) {.async.} =
  ## connect to the first responding server from `urls`.
  ## After this is done, start `handleMessages`
  for url in urls:
    # assert url.scheme == "nats"
    try:
      let port = getPort(url)
      nats.sock = newAsyncSocket()
      # nats.sock = await asyncnet.dial(url.hostname, port)
      # TODO NATS effectively does "START_TLS", the first message from the server is plaintext.


      await nats.sock.connect(url.hostname, port)
      let line = await nats.sock.recvLine()

      let (verb, rest) = splitMessage(line)
      await nats.handleMsgInfo(rest)

      if nats.info.hasKey("tls_required"):
        if nats.info["tls_required"].getBool():
          echo "Server requires TLS!"
          when defined ssl:
            if nats.tlsEnabled:
              # var ctx = newContext(verifyMode = CVerifyNone) #CVerifyPeer)
              var ctx = newContext(verifyMode = CVerifyPeer)
              # if not fileExists(nats.tlsServerPublicKeyPath):
                # echo "[ERROR] could not find ssl file: ", nats.tlsServerPublicKeyPath
                # quit()
              # if 0 == SSL_CTX_load_verify_locations(ctx.context, nats.tlsServerPublicKeyPath, ""): # we gonna trust our self signed certificat
              #   echo "[ERROR] SSL_CTX_load_verify_locations returned: 0"
              #   quit()
              echo "SSL_CTX_load_verify_locations: ",  SSL_CTX_load_verify_locations(ctx.context, nats.tlsServerPublicKeyPath, "") #: # we gonna trust our self signed certificat
              wrapConnectedSocket(ctx, nats.sock, handshakeAsClient) # enables SSL for this socket.
            else:
              echo "Enable TLS!"
              quit()
          else:
            echo "Compile with -d:ssl"
            quit()
      nats.currentServer = url
      nats.connectionStatus = connected
      break
    except:
      if nats.debug: echo "[client] could not connect to: ", url, " trying next one!"
      continue
  if nats.sock.isNil:
    raise newException(ConnectionError, "could not connect to any server!")
  var mc = MsgConnect()
  mc.user = nats.currentServer.username
  mc.pass = nats.currentServer.password
  mc.verbose = false
  mc.pedantic = false
  mc.tls_required = nats.tlsEnabled
  mc.name = ""
  mc.headers = true
  mc.lang = "nim"
  mc.version = "0.1.0"
  mc.protocol = 1
  await nats.send($mc)
  # TODO maybe wait for INFO?

proc pingInterval*(nats: Nats, sleepTime: int = 1_000) {.async.} =
  ## starts to ping the server in the given interval
  ## this is optional since the server pings the client as well.
  # while nats.connected:
  while nats.connectionStatus != disconnected:
    if nats.connectionStatus == connected:
      if nats.debug: echo "[send]: PING"
      await nats.send("PING" & crlf)
    await sleepAsync sleepTime

proc subscribe*(nats: Nats, subject: string, cb: SubscriptionCallback, queueGroup = ""): Future[Sid] {.async.} =
  ## subscribes to the given subject, returns a Sid that is used to unsubscribe later.
  let msg = MsgSub(subject: subject, queueGroup: queueGroup, sid: nats.genSid())
  var subscription = Subscription(subject: subject, queueGroup: queueGroup, sid: msg.sid, callback: cb)
  nats.subscriptions[msg.sid] = subscription
  await nats.send $msg
  return msg.sid

proc reply*(nats: Nats, subject: string, cb: SubscriptionCallback, queueGroup = defaultQueueGroup): Future[Sid] {.async.} =
  ## same as subscribe but has a default queueGroup of "NATS-RPLY-22"
  return await nats.subscribe(subject, cb, queueGroup)

proc unsubscribe*(nats: Nats, sid: Sid, maxMsgs = 0) {.async.} =
  ## Unsubscribes from the given Sid (returned by subscribe)
  let msg = MsgUnsub(sid: sid, max_msgs: maxMsgs)
  await nats.send $msg
  if nats.subscriptions.hasKey(sid):
    nats.subscriptions.del(sid)

proc unsubscribeSubject*(nats: Nats, subject: string, maxMsgs = 0) {.async.} =
  ## Unsubscribes all subscriptions that maches a given subject.
  for subscription in nats.subscriptions.values:
    if subscription.subject == subject:
      await nats.unsubscribe(subscription.sid)

proc publish*(nats: Nats, subject, payload: string, replyTo = "") {.async.} =
  ## publishes a messages to the given subject, optionally a replyTo address can be specified
  let msg = MsgPub(subject: subject, payload: payload, replyTo: replyTo)
  await nats.send $msg

proc publish*(nats: Nats, subject, payload: string, headers: NatsHeaders, replyTo = "") {.async.} =
  ## publishes a messages to the given subject, optionally a replyTo address can be specified
  ## specify headers here, the message is sent as a HPUB.
  let msg = MsgHpub(subject: subject, payload: payload, headers: headers, replyTo: replyTo)
  await nats.send $msg

proc request*(nats: Nats, subject: string, payload: string = "", queueGroup = defaultQueueGroup, timeout = 5_000, headers: NatsHeaders = @[]): Future[MsgHmsg] {.async.} =
  ## awaitable
  # TODO either unsubscribe after or timeout or both
  let inbox = nats.genInbox()
  var fut = newFuture[MsgHmsg](fromProc = "request")
  var sid: Sid
  proc cb(nats: Nats, msg: MsgHmsg) {.async.} =
    fut.complete(msg)
  sid = await nats.subscribe(inbox, cb, queueGroup)
  await nats.publish(subject, payload, replyTo = inbox, headers = headers)
  var inTime = await withTimeout(fut, timeout)
  if (inTime):
    result = await fut
  else:
    fut.fail(newException(TimeoutError, "[request]: timeout"))
    result = await fut
  await nats.unsubscribe(sid)

# Jetstream ############
proc jsApiInfo*(nats: Nats): Future[JsonNode] {.async.} =
  # TODO json -> types?
  return parseJson((await nats.request("$JS.API.INFO")).payload)
