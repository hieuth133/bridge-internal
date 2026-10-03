#!/usr/bin/env bash
# 4. Pub/Sub, RabbitMQ to Solace: put a message on x/vnm/vatm/dev/swim, check it on Solace. Run on the RabbitMQ machine, after `source tests/env.sh`. Options: --recipients LIST, --payload FILE, --key KEY.
root="$(cd "$(dirname "$0")/.." && pwd)"
exec java -cp "$root/lib/*:$root/tests/lib/*" "$root/tests/BridgeTest.java" rabbitmq-pubsub "$@"
