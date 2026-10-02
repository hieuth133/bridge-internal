# The Routing key on the External EMS is the SWIM topic, unchanged

Superseded by ADR 0005: the Routing key now uses `.` between levels.

When the Bridge moves a message to RabbitMQ, the Routing key is the same string as the SWIM topic, `/` included (for example `t/vnm/vatm/dev/atfm/v1/fpl`). We do not turn `/` into `.`.

Partners that already use this RabbitMQ (`vhost swim_sg`) publish with SWIM topics as Routing keys, so we follow them. It also keeps the rule "the topic on Solace is the key on RabbitMQ" easy to understand.

What this costs us: RabbitMQ splits keys on `.`, so a whole SWIM topic counts as one word there. A RabbitMQ subscriber cannot use wildcards such as `t.vnm.#`. They must bind the exact topic, or bind `#` to get everything. Wildcards still work on the Solace side. Changing this later means every RabbitMQ subscriber has to re-bind.
