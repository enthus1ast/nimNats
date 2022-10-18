import cligen, asyncdispatch, nats, times
import print

proc doSub(subject: string, queueGroup = "", url = "nats://127.0.0.1:4222") {.async.} =
  var nats = newNats(debug = true)
  await nats.connect(@[parseUri(url)])
  proc cb(nats: Nats, msg: MsgHmsg) {.async.} =
    print $now(), msg
  let sid = (await nats.subscribe(subject, cb, queueGroup))
  await nats.handleMessages()

proc sub(subject: string, queueGroup = "", url = "nats://127.0.0.1:4222"): int =
  waitFor doSub(subject, queueGroup, url)



dispatchMulti([sub])
