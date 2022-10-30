## NATS.io client in pure nim.
##
## look in the examples dir for usage examples.

import nats/[constants, types, core, kv]
export constants, types, core, kv



when isMainModule and true:
  import os
  import print
  import random

  proc handleDestinationSubject(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Callback!", msg.sid, msg.subject, msg.payload, msg.replyTo

  proc handleDestinationSubject2(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Callback22222!", msg.sid, msg.subject, msg.payload, msg.replyTo

  proc handleNeedHelp(nats: Nats, msg: MsgHmsg) {.async.} =
    print "Need help callback", msg.sid, msg.subject, msg.payload, msg.replyTo
    await nats.publish(msg.replyTo, "i can help!!")

  proc printMsg(nats: Nats, msg: MsgHmsg) {.async.} =
    echo msg
    # await nats.publish(msg.replyTo, "i can help!!")


  proc main(nats: Nats) {.async.} =
    await nats.connect(@[
      # parseUri("nats://127.0.0.1:2134"),
      parseUri("nats://127.0.0.1:4222"),
      # parseUri("nats://a:b@127.0.0.1:5222"),
      # parseUri("nats://a:b@127.0.0.1:5222"),
      # parseUri("nats://a:b@127.0.0.1:4222"),
      ])
    asyncCheck nats.handleMessages()
    asyncCheck nats.pingInterval(10_000)

    print nats.jetstreamAvailable # TODO this can only be known after the info message was handled.
    if nats.jetstreamAvailable:
      echo await nats.jsApiInfo()
    var buckfoo = await nats.addBucket("3buk")
    proc buckfooCb(nats: Nats, msg: MsgHmsg) {.async.} =
      let kv = hmsgToKv(msg)
      print "Bucket updated:#############################################", kv
    let bucksid = await buckfoo.subscribe(buckfooCb)

    let sid1 = await nats.subscribe("destination.subject", handleDestinationSubject)
    let sid2 = await nats.subscribe("destination.subject", handleDestinationSubject2)
    let sid3 = await nats.reply("help", handleNeedHelp)
    let sid4 = await nats.subscribe(">", printMsg)
    var pub = MsgPub()
    pub.subject = "destination.subject"
    # pub.replyTo = "replyAddress"
    echo $pub
    # while nats.connected:

    await buckfoo.put("deleteme","DELETEME")
    while nats.connectionStatus != disconnected:
      await sleepAsync(500)
      echo "###################"
      # echo "KV:", (await nats.getKv("3buk", "foo")).payload
      await buckfoo.put("foo", $rand(1024))
      await buckfoo.put("baa", $rand(1024))
      await buckfoo.put("baz", $rand(1024))
      await buckfoo.del("deleteme")

      # echo "KV:", await buckfoo.get("foo")

      # try:
      #   echo "GET UNKNOWN:", await buckfoo.get("AAAAA")
      # except BucketKeyDoesNotExistError as ex:
      #   echo "Key Not found:"
      #   echo ex.errorNumber
      #   echo ex.msg
      #   echo "Key: ", ex.key
      #   echo "^^^^"
      # echo "###################"
      # quit()

      when false:
        for idx in 1..10:
          pub.payload = "SOME PAYLOAD" & crlf & "HAHAHA " & $idx
          await nats.publish("destination.subject", "FOO 1" & "END" , @[("aaa", "bbb"), ("foo", "baa")])
          await nats.publish("destination.subject", "", @[("no", "content"), ("foo", "baa")])
          await nats.publish("destination.subject", "", @[])
          try:
            echo "===================================="
            echo "HELP MESSAGE:",  await nats.request("AAA", "A".repeat(rand(10)) & "END")
            echo "===================================="

          except TimeoutError:
            echo "Noone wants to help me :( ", getCurrentExceptionMsg()
          # await sleepAsync(1_000)
          await sleepAsync(200)
          # await sleepAsync(50)

      # await nats.unsubscribe(sid1)
      await nats.unsubscribe(sid2)
      # await sleepAsync(60_000)
    echo "end."

  var nats = newNats(debug = true)
  nats.tlsEnabled = true
  # nats.tlsServerPublicKeyPath = getAppDir() / "examples" / "tlskeys" / "public.pem"
  # nats.tlsServerPublicKeyPath = getAppDir() /  "examples" / "tlskeys" / "public.pem"
  nats.tlsServerPublicKeyPath = getAppDir() /  "examples/tlskeys/public.pem"
  echo nats.tlsServerPublicKeyPath
  waitFor nats.main()