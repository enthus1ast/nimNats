import asyncnet, asyncdispatch, uri, strutils, json, strformat,
  tables, print, parseutils, random

export uri

const crlf = "\c\l"
const natsHeaderMagic = "NATS/1.0"
const validHeaderKeyChars: set[char] = {33.char .. 126.char} - {':'}
const validHeaderValueChars: set[char] = AllChars - {'\c', '\l'}
const inboxPrefix = "_nimNatsIn"

type

  ConnectionError* = object of IOError
  TimeoutError* = object of IOError
  ParsingError* = object of ValueError
  NotImplementedError = object of CatchableError

  ConnectionStatus* = enum
    connected
    reconnecting
    disconnected
  Verb* = enum
    # Sent by server
    INFO
    MSG
    HMSG
    `+OK`
    `-ERR`

    # Sent by client
    CONNECT
    PUB
    SUB
    UNSUB
    HPUB

    # Send by both
    PING
    PONG
  # Message = object
  #   messageKind: Verb

  MsgConnect* = object
    verbose: bool # : Turns on +OK protocol acknowledgements.
    pedantic: bool # : Turns on additional strict format checking, e.g. for properly formed subjects
    tls_required: bool # : Indicates whether the client requires an SSL connection.
    auth_token: string # : Client authorization token (if auth_required is set)
    user: string # : Connection username (if auth_required is set)
    pass: string # : Connection password (if auth_required is set)
    name: string # : Optional client name
    lang: string # : The implementation language of the client.
    version: string # : The version of the client.
    protocol: int # : optional int. Sending 0 (or absent) indicates client supports original protocol. Sending 1 indicates that the client supports dynamic reconfiguration of cluster topology changes by asynchronously receiving INFO messages with known servers it can reconnect to.
    `echo`: bool # : Optional boolean. If set to true, the server (version 1.2.0+) will not send originating messages from this connection to its own subscriptions. Clients should set this to true only for server supporting this feature, which is when proto in the INFO protocol is set to at least 1.
    sig: string # : In case the server has responded with a nonce on INFO, then a NATS client must use this field to reply with the signed nonce.
    jwt: string # : The JWT that identifies a user permissions and account.
    headers: bool # : Enabling Receiving of Message Headers https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-4.md

  MsgPub* = object
    subject*: string
    replyTo*: string
    payload*: string

  MsgHpub* = object
    subject*: string
    replyTo*: string
    payload*: string
    headers*: NatsHeaders

  MsgSub* = object
    subject*: string
    queueGroup*: string
    sid*: Sid

  MsgUnsub* = object
    sid*: Sid
    max_msgs*: int

  MsgHmsg* = object
    sid*: Sid
    subject*: string
    payload*: string
    headers*: NatsHeaders
    replyTo*: string


  Sid* = string # A unique alphanumeric subscription ID, generated by the client
  Inbox* = string # A returnTo address
  NatsHeaders* = seq[tuple[key, val: string]]

  SubscriptionCallback* = proc(nats: Nats, msg: MsgHmsg) {.async.}
  Subscription = object
    subject: string
    queueGroup: string # optional
    sid: Sid
    callback: SubscriptionCallback

  # User defined callbacks.
  HandlerError = proc(nats: Nats, msg: string) {.async.}
  HandlerInfo = proc(nats: Nats, info: JsonNode) {.async.}

  Nats* = ref object
    sock: AsyncSocket
    # connected: bool
    debug*: bool
    info: JsonNode
    subscriptions: Table[Sid, Subscription]
    requests: Table[Inbox, Future[MsgHmsg]]
    # TODO add subscription to sid lookup table
    lastSidInt: int
    servers: seq[Uri]
    reconnect: bool # auto reconnect enabled?
    connectionStatus: ConnectionStatus
    isHandelingMessages: bool
    currentServer: Uri # the server we're currently connected to
    # User defined handler/callbacks
    handlerError*: HandlerError
    handlerInfo*: HandlerInfo # called when the server sends an info eg when a new cluster server is available.

# Forward declarations
proc connect*(nats: Nats, urls: seq[Uri]): Future[void]

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


proc genSid(nats: Nats): Sid =
  nats.lastSidInt.inc
  return $nats.lastSidInt

proc genInbox*(nats: Nats): Inbox =
  ## generates a new unique inbox
  # TODO store inboxes, to avoid collisions?
  return inboxPrefix & $rand(int.high)

proc `$`*(headers: NatsHeaders): string =
  result = ""
  result &= natsHeaderMagic & crlf
  for (key, val) in headers:
    result &= fmt"{key}: {val}{crlf}"

proc `[]`*(headers: NatsHeaders, key: string): seq[string] =
  # return all values with the key `key`
  for (k, val) in headers:
    if k == key:
      result.add val

proc `$`*(msg: MsgPub): string =
  fmt"{PUB} {msg.subject} {msg.replyTo} {msg.payload.len}{crlf}{msg.payload}{crlf}"

proc `$`*(msg: MsgHpub): string =
  # HPUB <subject> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊
  let headerstr = $msg.headers
  fmt"{HPUB} {msg.subject} {msg.replyTo} {headerstr.len + crlf.len} {(headerstr.len + crlf.len) + msg.payload.len + (crlf.len)}{crlf}{headerstr}{crlf}{msg.payload}{crlf}{crlf}"

proc `$`*(msg: MsgConnect): string =
  fmt"{CONNECT} {$ %* msg}{crlf}"

proc `$`*(msg: MsgSub): string =
  fmt"{SUB} {msg.subject} {msg.queueGroup} {msg.sid}{crlf}"

proc `$`*(msg: MsgUnsub): string =
  fmt"{UNSUB} {msg.sid} {msg.max_msgs}{crlf}"



proc parseHeaders*(str: string): NatsHeaders =
  ## Parses nats header
  #NATS/1.0\r\nfoo: baa
  if str.len == 0: return
  var lines = str.splitLines()
  if lines.len == 0: return
  if not lines[0].startsWith(natsHeaderMagic):
    # if nats.debug:
    # echo "[client]: invalid header magic: " & lines[0]
    return
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
    result.add (key, val)

proc newNats*(reconnect = true, debug = false): Nats =
  randomize()
  result = Nats()
  result.debug = debug
  result.lastSidInt = 0
  result.reconnect = reconnect
  result.connectionStatus = disconnected
  result.handlerError = defaultHandlerError
  result.handlerInfo = defaultHandlerInfo

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
  ## low level sending proc. use the `$` proc from the `Msg*`.
  if nats.debug:
    echo "[send]: ", str.strip() # not accurate but less clutter in the console
  await nats.sock.send(str)
# proc parseMessage(str: string): Message =
#   return

proc handleMsgERR(nats: Nats, rest: string) {.async.} =
  if not nats.handlerError.isNil:
    await nats.handlerError(nats, rest)

proc handleMsgPING(nats: Nats, rest: string) {.async.} =
  if nats.debug:
    echo "[client] send pong"
  await nats.send "PONG" & crlf

proc handleMsgPONG(nats: Nats, rest: string) {.async.} =
  discard #TODO reset the timeout counter

proc handleMsgINFO(nats: Nats, rest: string) {.async.} =
  # Get informations from the server, this way we also learn about
  # new cluster servers.

  var info: JsonNode = %* {} # provide an empty default
  try:
    info = parseJson(rest)
  except:
    raise newException(ParsingError, "could not parse the server info: " & rest)

  # Call the user defined handler
  if not nats.handlerInfo.isNil:
    if nats.debug: echo "Call user handlerInfo"
    await nats.handlerInfo(nats, info)

  if nats.debug: echo "Set info to obj"
  nats.info = info # we use the info that was passed to the handler, so a user can override some values.
  echo nats.info

  # Learn of new cluster nodes.
  if nats.info.hasKey("connect_urls"):
    for elem in nats.info["connect_urls"]:
      echo elem

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
  echo "CC:", cc
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
  let headers = parseHeaders(headerBody)
  # print subject, sid, replyTo, headerBytes,totalBytes, wholeBody, headerBody, payload, headers
  # call the callback
  if nats.subscriptions.hasKey(sid):
    let msg = MsgHmsg(sid: sid, subject: subject, payload: payload, headers: headers, replyTo: replyTo)
    await nats.subscriptions[sid].callback(nats, msg)
  else:
    if nats.debug: echo "[err] no registered subscription!"


proc handleMessages*(nats: Nats) {.async.} =
  ## This ist the main
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
        # Tell the new server what we have supscribed
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
        # nats.connected = false
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

proc connect*(nats: Nats, urls: seq[Uri]) {.async.} =
  for url in urls:
    # assert url.scheme == "nats"
    try:
      nats.sock = await asyncnet.dial(url.hostname, parseInt(url.port).Port)
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
  mc.tls_required = false
  mc.name = ""
  mc.headers = true
  mc.lang = "nim"
  mc.version = "0.1.0"
  mc.protocol = 1
  await nats.send($mc)
  if nats.isHandelingMessages == false:
    asyncCheck nats.handleMessages()

proc pingInterval*(nats: Nats, sleepTime: int = 1_000) {.async.} =
  # while nats.connected:
  while nats.connectionStatus != disconnected:
    if nats.connectionStatus == connected:
      if nats.debug: echo "[send]: PING"
      await nats.send("PING" & crlf)
    await sleepAsync sleepTime

proc subscribe*(nats: Nats, subject: string, cb: SubscriptionCallback, queueGroup = ""): Future[Sid] {.async.} =
  let msg = MsgSub(subject: subject, queueGroup: queueGroup, sid: nats.genSid())
  var subscription = Subscription(subject: subject, queueGroup: queueGroup, sid: msg.sid, callback: cb)
  nats.subscriptions[msg.sid] = subscription
  await nats.send $msg
  return msg.sid

proc reply*(nats: Nats, subject: string, cb: SubscriptionCallback, queueGroup = "NATS-RPLY-22"): Future[Sid] {.async.} =
  ## same as subscribe but has a default queueGroup of "NATS-RPLY-22"
  return await nats.subscribe(subject, cb, queueGroup)

proc unsubscribe*(nats: Nats, sid: Sid, maxMsgs = 0) {.async.} =
  ## Unsubscribes from the given Sid
  let msg = MsgUnsub(sid: sid, max_msgs: maxMsgs)
  await nats.send $msg
  if nats.subscriptions.hasKey(sid):
    nats.subscriptions.del(sid)

proc unsubscribeSubject*(nats: Nats, subject: string, maxMsgs = 0) {.async.} =
  ## Unsubscribes all that maches a given subject
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

proc request*(nats: Nats, subject: string, payload: string, queueGroup = "NATS-RPLY-22", timeout = 5_000): Future[MsgHmsg] {.async.} =
  ## awaitable
  # TODO either unsubscribe after or timeout or both
  let inbox = nats.genInbox()
  var fut = newFuture[MsgHmsg](fromProc = "request")
  var sid: Sid
  proc cb(nats: Nats, msg: MsgHmsg) {.async.} =
    fut.complete(msg)
  sid = await nats.subscribe(inbox, cb, queueGroup)
  await nats.publish(subject, payload, replyTo = inbox)
  var inTime = await withTimeout(fut, timeout)
  if (inTime):
    result = await fut
  else:
    fut.fail(newException(TimeoutError, "[request]: timeout"))
    result = await fut
  await nats.unsubscribe(sid)


when isMainModule and true:

  proc handleDestinationSubject(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Callback!", msg.sid, msg.subject, msg.payload, msg.replyTo

  proc handleDestinationSubject2(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Callback22222!", msg.sid, msg.subject, msg.payload, msg.replyTo

  proc handleNeedHelp(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Need help callback", msg.sid, msg.subject, msg.payload, msg.replyTo
    await nats.publish(msg.replyTo, "i can help!!")

  proc main(nats: Nats) {.async.} =
    await nats.connect(@[
      parseUri("nats://127.0.0.1:2134"),
      parseUri("nats://a:b@127.0.0.1:5222"),
      parseUri("nats://a:b@127.0.0.1:4222"),
      ])
    asyncCheck nats.pingInterval(10_000)

    let sid1 = await nats.subscribe("destination.subject", handleDestinationSubject)
    let sid2 = await nats.subscribe("destination.subject", handleDestinationSubject2)
    let sid3 = await nats.reply("help", handleNeedHelp)
    var pub = MsgPub()
    pub.subject = "destination.subject"
    # pub.replyTo = "replyAddress"
    echo $pub
    # while nats.connected:
    while nats.connectionStatus != disconnected:
      for idx in 1..10:
        pub.payload = "SOME PAYLOAD" & crlf & "HAHAHA " & $idx
        await nats.publish("destination.subject", "FOO 1", @[("aaa", "bbb"), ("foo", "baa")])
        # await nats.publish("destination.subject", "", @[("no", "content"), ("foo", "baa")])
        try:
          echo "===================================="
          echo "HELP MESSAGE:",  await nats.request("AAA", "A".repeat(rand(1024)))
          echo "===================================="

        except TimeoutError:
          echo "Noone wants to help me :( ", getCurrentExceptionMsg()
        # await sleepAsync(1_000)
        # await sleepAsync(200)
        await sleepAsync(50)

      # await nats.unsubscribe(sid1)
      await nats.unsubscribe(sid2)
      # await sleepAsync(60_000)
    echo "end."

  var nats = newNats(debug = true)
  waitFor nats.main()