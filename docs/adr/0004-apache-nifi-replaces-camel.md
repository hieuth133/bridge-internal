# The Bridge is an Apache NiFi flow, not a Camel program

Supersedes ADR 0003.

The user wants the Bridge to need as little code as possible, use defaults as much as possible, and run from an official container image. We replaced the Java 21 + Camel Main program with one Apache NiFi flow:

- **Runtime:** the official image `docker.io/apache/nifi:2.12.0`, run with one `podman run` (or `docker run`) command, `--network=host`, `--restart=always`, and named volumes for NiFi's `conf`, `state` and repositories. We build no image of our own.
- **Internal EMS (Solace):** NiFi's `ConsumeJMS` / `PublishJMS` with a `JndiJmsConnectionFactoryProvider` that looks up the broker's default connection factory `/jms/cf/default`. The Solace JMS jars (`sol-jms-jakarta` and its runtime dependencies, listed in `nifi/solace-jars.txt`) are downloaded from Maven Central and mounted read-only. We use JNDI because NiFi's generic `JMSConnectionFactoryProvider` cannot set Solace's `Boolean` properties such as `dynamicDurables`.
- **External EMS (RabbitMQ):** NiFi's `ConsumeAMQP` / `PublishAMQP`. They never declare exchanges or queues, so the Bridge's RabbitMQ objects are created once by hand. `PublishAMQP` must use `Delivery Guarantee = AT_LEAST_ONCE`. Its default, `AT_MOST_ONCE`, can lose messages.
- **The flow** is kept as a NiFi flow definition, `nifi/bridge-flow.json`, imported once in the NiFi UI. The two broker passwords are sensitive parameters, typed in the UI and never stored in the repository.

What this costs us:

- The flow is JSON edited in a UI, not code. There are no automated tests. We check the Bridge with the demo in the README.
- A message leaves the Internal EMS (or the External EMS) once it is safely stored in NiFi's repositories, not once the other broker accepted it. NiFi then retries until the other broker accepts it. The NiFi volumes must therefore be kept.
- There is no loop guard. A loop needs an Inbound target that an Outbound source also matches. Keep the two sides' topics apart when adding Bridge Rules.
- RabbitMQ headers are not copied to Solace, because NiFi always gives them a name prefix with `.`, which a JMS property name cannot contain. Content type, message id and correlation id are copied.
