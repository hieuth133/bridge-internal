#!/usr/bin/env bash
# 3. Async Request/Reply, Solace to RabbitMQ: send a request with reply-to on Solace, check it on RabbitMQ. Run on the Solace machine, after `source tests/env.sh`. Options: --recipients LIST, --payload FILE, --topic TOPIC.
root="$(cd "$(dirname "$0")/.." && pwd)"
exec java -cp "$root/lib/*:$root/lib-rabbitmq/*:$root/tests/lib/*" "$root/tests/BridgeTest.java" solace-rr "$@"
