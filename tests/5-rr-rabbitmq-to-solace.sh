#!/usr/bin/env bash
# 5. Async Request/Reply, RabbitMQ to Solace: put a request with reply-to on x/vnm/vatm/dev/route as the Router would, check it on Solace. Run on the RabbitMQ machine, after `source tests/env.sh`. Options: --recipients LIST, --payload FILE, --key KEY.
root="$(cd "$(dirname "$0")/.." && pwd)"
exec java -cp "$root/lib/*:$root/tests/lib/*" "$root/tests/BridgeTest.java" rabbitmq-rr "$@"
