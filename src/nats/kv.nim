## Api for the key value store.
##
## A key value bucket has two modes of operandi.
## 1)
##   The first one is that every value is aquired from the NATS server on a `bucket.get("val")`
## 2)
##   NOT IMPLEMENTED YET!
##   The second one is, that the value(s) are aquired once on the creation and is cached in a nim table
##   then we subscribe on the buckets
##   Subjects and the server informs us about key/value changes.
##   This way the access is much faster.
import types
import core

type
  PlJsApiStreamCreateKv = object
    name: string
    subjects: seq[string]

type
  JsKvBucket = object
    name*: string
    nats: Nats # TODO is this good design? It allows `await bucket.get("foo")` syntax
    cache*: Table[string, string]
    # Config etc # TODO

proc addBucket*(nats: Nats, bucket: string): Future[JsKvBucket] {.async.} =
  ## adds a bucket
  let subject = fmt"$JS.API.STREAM.CREATE.KV_{bucket}"
  var pl: PlJsApiStreamCreateKv
  pl.name = fmt"KV_{bucket}"
  pl.subjects = @[fmt"$KV.{bucket}.>"]
  let res = parseJson((await nats.request(subject, $ %* pl)).payload) # TODO what to do with resp?
  return JsKvBucket(name: bucket, nats: nats)

proc remBucket*(nats: Nats, bucket: JsKvBucket): Future[void] {.async.} =
  ## removes a bucket
  # SUB _INBOX.9WlnkTv8gzdEAEBOUpFOsA.*  1
  # PUB $JS.API.STREAM.DELETE.KV_4buk _INBOX.9WlnkTv8gzdEAEBOUpFOsA.8AadBPts 0
  # ---
  # MSG _INBOX.9WlnkTv8gzdEAEBOUpFOsA.8AadBPts 1 73
  # {"type":"io.nats.jetstream.api.v1.stream_delete_response","success":true}
  let subject = fmt"$JS.API.STREAM.DELETE.{bucket.name}"
  let res = await bucket.nats.request(subject)
  # TODO parse response etcpp

proc put*(bucket: JsKvBucket, key, value: string): Future[void] {.async.} =
  # PUB $KV.3buk.foo _INBOX.SdKKL4Oxk9a4LqIje3Zx6e.L7dSHjnc 11
  # http://HAHA
  let subject = fmt"$KV.{bucket.name}.{key}"
  let res = await bucket.nats.request(subject, payload = value)
  return

proc get*(bucket: JsKvBucket, key: string, fetch = true): Future[string] {.async.} =
  # if fetch:
  #   let subject = fmt"$JS.API.DIRECT.GET.KV_{bucket.name}.$KV.{bucket.name}.{key}"
  #   result = (await bucket.nats.request(subject)).payload
  #   bucket.cache
  # else:
  #   if bucket.cache.hasKey(key):
  #     return bucket.cache[key]
  let subject = fmt"$JS.API.DIRECT.GET.KV_{bucket.name}.$KV.{bucket.name}.{key}"
  let hmsg = await bucket.nats.request(subject)
  if hmsg.errorNumber != 0:
    var ex = new(BucketKeyDoesNotExistError)
    ex.msg = hmsg.errorMessage
    ex.errorNumber = hmsg.errorNumber
    ex.key = key
    raise ex
  return hmsg.payload

proc del*(bucket: JsKvBucket, key: string): Future[void] {.async.} =
  ## deletes a key from the given bucket.
  # HPUB $KV.3buk.foo _INBOX.qPI2quiLioSXaDSqesMymj.EeUbg0W5 31 31
  # NATS/1.0
  # KV-Operation: DEL
  let subject = fmt"$KV.{bucket.name}.{key}"
  var headers: NatsHeaders
  headers["KV-Operation"] = "DEL"
  let res = await bucket.nats.request(subject, payload = "", headers = headers)


proc splitKvSubject(str: string): tuple[prefix, bucket, key: string] =
  ## splits a KV subject like: `$KV.tbuck.aa`
  const kvPref = "$KV"
  if str.len == 0: return
  if not str.startsWith(kvPref): return
  let parts = str[kvPref.len + 1 .. ^1].split(".", 1)
  assert parts.len == 2
  return (kvPref, parts[0], parts[1])


proc hmsgToKv*(hmsg: MsgHmsg): tuple[key, value: string] =
  ## extracts key & value from a MsgHmsg, use this in a KV bucket subscription.
  let parts = splitKvSubject(hmsg.subject)
  return (parts.key, hmsg.payload)

proc subscribe*(bucket: JsKvBucket, cb: SubscriptionCallback): Future[Sid] {.async.} =
  ## Subscribes to the bucket topics, to get informed of all key/value updates
  result = await bucket.nats.subscribe(fmt"$KV.{bucket.name}.>", cb)

