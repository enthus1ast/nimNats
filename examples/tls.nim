## this is a tls example
## 1. first configure your NATS server to use tls.
##  - for details: https://docs.nats.io/running-a-nats-service/configuration/securing_nats/tls
##  - create a tls certificate
##    For example:
##    openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout secret.key -out public.key
##  - Start the server with tls enable
##    For example:
##    .\nats-server.exe  -js --tls --tlscert=tlskeys\public.key --tlskey=C:\Users\david\projects\nimNats\src\examples\tlskeys\secret.key

import asyncdispatch
import nats
import os

proc main() {.async.} =
    var nats = newNats()
    nats.tlsEnabled = true
    nats.tlsServerPublicKeyPath = getAppDir() / "tlskeys/public.pem"



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

    # var cnt = 3
    while true:
      # Publish messages to subject `foo`
      await nats.publish("foo", "Hello from Nim!")
      await sleepAsync(3_000)
      # cnt.dec
      # if cnt == 0:
        # echo "Unsubscribe"
        # await nats.unsubscribe(sub)


waitFor main()