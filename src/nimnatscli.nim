import cligen, asyncdispatch, nats, times, json, std/jsonutils
import print

proc doSub(subject: string, queueGroup = "", url: string, asJson = false) {.async.} =
  var nats = newNats(debug = false)
  # nats.isHandelingMessages = true # TODO remove this from connect
  await nats.connect(@[parseUri(url)])
  proc cb(nats: Nats, msg: MsgHmsg) {.async.} =
    if asJson:
      echo $( toJson(msg)) #
      discard
    else:
      print $now(), msg
  let sid = (await nats.subscribe(subject, cb, queueGroup))
  await nats.handleMessages()

proc sub(subject: string, queueGroup = "", url = "nats://127.0.0.1:4222", asJson = false): int =
  waitFor doSub(subject, queueGroup, url, asJson)

# proc reply()


dispatchMulti([sub])
