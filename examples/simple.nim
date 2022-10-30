import asyncdispatch
import nats

proc main() {.async.} =
    var nats = newNats()

    # Connect to NATS!
    await nats.connect(@[parseUri "localhost"])

    # Start the main message handler,
    # this distributes messages,
    # answers to pings etc.
    asyncCheck nats.handleMessages()

    # The handler that is called for every
    # message you receive for subject `foo`
    proc messageHandler(nats: Nats, msg: MsgHmsg) {.async.} =
      echo("Received:", msg)

    # Subscribe to messages on subject `foo`
    # Note: NATS does not deliver messages send by us back to us.
    # So to receive these messages, start another client
    let sub = await nats.subscribe("foo", messageHandler)

    var cnt = 3
    while true:
      # Publish messages to subject `foo`
      await nats.publish("foo", "Hello from Nim!")
      await sleepAsync(3_000)
      cnt.dec
      if cnt == 0:
        echo "Unsubscribe"
        await nats.unsubscribe(sub)


waitFor main()