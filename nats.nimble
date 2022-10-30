# Package

version       = "0.2.4"
author        = "David Krause"
description   = "A pure Nim NATS.io client"
license       = "MIT"
srcDir        = "src"

bin = @["natscli"]

# Dependencies

requires "nim >= 1.6.6"
requires "print" # for debugging