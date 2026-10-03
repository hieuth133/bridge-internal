# The Bridge talks AMQP 1.0 to RabbitMQ

Changes part of ADR 0004 and ADR 0006: B-03 reads from and B-04 publishes to RabbitMQ through RabbitMQ's AMQP 1.0 Java client in `ExecuteGroovyScript`, not through NiFi's `ConsumeAMQP` / `PublishAMQP`.

The SWIM APAC v4 agreement uses AMQP 1.0 between EMSes, and there is no AMQP 0-9-1 left in the network. The Bridge still reached RabbitMQ over 0-9-1. The NiFi setting cannot simply be changed:

- `ConsumeAMQP` / `PublishAMQP` (every NiFi version up to `main`, so 2.12 too) allow only `AMQP Version = 0.9.1`. They are built on RabbitMQ's 0-9-1 Java client.
- A stock JMS client for AMQP 1.0 (Qpid JMS) with `ConsumeJMS` / `PublishJMS` would break ADR 0006. It cannot set content-type. It sends a TextMessage as an `amqp-value` body, and a BytesMessage with content-type `application/octet-stream`. `PublishJMS` also drops SWIM reply-to names (ADR 0006).

Decision:

- **Client.** RabbitMQ's own AMQP 1.0 Java client, `com.rabbitmq.client:amqp-client` 1.5.0, with netty 4.2.17 and slf4j-api. The jars come from Maven Central (`nifi/rabbitmq-jars.txt`). They go into `lib-rabbitmq/`, mounted read-only at `/opt/nifi/rabbitmq-lib`, and are loaded through each script's `Additional Classpath`. They are kept apart from `lib/` so that netty never enters the classloader that loads Solace JMS. NiFi 2.12 already ships the same netty version, so the two copies do not clash.
- **B-04 publisher** (also used by the demo sender). It reads the same attributes `PublishAMQP` read, so the step before it does not change:
  - the address is the exchange plus the `routingKey` attribute;
  - `amqp$messageId`, `amqp$correlationId`, `amqp$contentType` and `amqp$replyTo` become the message's properties, and `amqp$deliveryMode` = 2 becomes `durable`;
  - the attributes that match the old Headers Pattern become application-properties, as strings;
  - the payload is one `data` section, unchanged.

  A flowfile goes to `success` only when RabbitMQ answers `accepted`. Otherwise it goes to `failure`, which loops back. This includes a message that no queue is bound to receive: `PublishAMQP` dropped it without a word, while this publisher keeps it and tries again until a binding exists.
- **B-03 consumer.** It writes the same attributes `ConsumeAMQP` wrote, so the proven Solace publisher does not change:
  - `consume.amqp.<name>` for each application-property;
  - `amqp$routingKey` from the `x-routing-key` annotation;
  - `amqp$messageId`, `amqp$correlationId`, `amqp$contentType` and `amqp$replyTo`.

  It accepts a message only after NiFi has committed its flowfile, and requeues it if the commit fails. If the connection drops first, RabbitMQ delivers the message again, so the worst case is a duplicate. A `data` body and a string `amqp-value` body are both taken as bytes. Any other body (a map or a list) cannot go to Solace as text, so B-03 keeps it in NiFi as text (Java's `toString` of the map or list) on a `failure` connection to a funnel, and logs an error.
- **Headers are application-properties only.** Every sender speaks 1.0, so a user header is always an application-property. Message annotations (`x-...`) are not carried.
- **Connections are named** `vatm-bridge B-03` and `vatm-bridge B-04`. They follow the pattern of B-03's Solace publisher: one connection per processor, closed when the processor stops, and closed on an error and opened again on the next run. The client's own recovery is turned off, so that only this one mechanism reconnects.
- **The tests are the check.** `tests/BridgeTest.java` uses the same client for every RabbitMQ step. `ping` (run by `source tests/env.sh`) fails unless both Bridge connections show in RabbitMQ with protocol `AMQP 1-0`. `nifi/check-headers.py` is retired. It went through RabbitMQ's management HTTP API, which publishes over 0-9-1 inside the broker, and `tests/2..5` already cover the same four paths.

What this costs us:

- About 200 lines of our own Groovy (a publisher and a consumer) in place of two stock processors, and one more mount and jar list.
- To upgrade an existing install, download the jars and re-create the container with the new mount (the volumes keep the flow), then load the new flow.
- Not done: the v4 documents run AMQP 1.0 over TLS/mTLS on port 5671. The Bridge and the tests still use plain port 5672. The tests' read of the management API (`/api/connections` on 15672) is plain HTTP with Basic auth too. Adding TLS later means setting TLS options on the same connection builders, plus the certificates.
