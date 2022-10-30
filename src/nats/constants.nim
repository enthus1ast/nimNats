from strutils import AllChars

const crlf* = "\c\l"
const natsHeaderMagic* = "NATS/1.0"
const validHeaderKeyChars*: set[char] = {33.char .. 126.char} - {':'}
const validHeaderValueChars*: set[char] = AllChars - {'\c', '\l'}
const inboxPrefix* = "_nimNatsIn"
const inboxRandomLen* = ($int.high).len
const defaultQueueGroup* = "NATS-RPLY-22" ## queue group that is used by the official nats tooling
const defaulPort* = 4222