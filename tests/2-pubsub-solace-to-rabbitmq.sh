#!/usr/bin/env bash
# 2. Pub/Sub, Solace to RabbitMQ: send on Solace topic t/vnm/vatm/dev/atfm/v1/fpl, check it on RabbitMQ. Run on the Solace machine, after `source tests/env.sh`. Options: --recipients LIST, --payload FILE, --topic TOPIC.
root="$(cd "$(dirname "$0")/.." && pwd)"
exec java -cp "$root/lib/*:$root/tests/lib/*" "$root/tests/BridgeTest.java" solace-pubsub "$@"
