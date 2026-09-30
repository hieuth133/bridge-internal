# The Bridge is Java 21 with Apache Camel Main 4.22 LTS, built with the Maven Wrapper

The project had no code yet. The user wants Apache Camel in Java, with as little code as possible. Java 21 is installed on the machine. Maven is not.

We use:

- **Java 21** and **Apache Camel Main 4.22.1** (the latest LTS, supported until Aug 2027). No Spring Boot: Camel Main is enough to read `application.properties` and run one `RouteBuilder`. The start class is Camel's own `org.apache.camel.main.Main`, so we write no `main` method.
- **Internal EMS (Solace):** `camel-jms` with `com.solacesystems:sol-jms-jakarta:10.30.2`. Camel 4 uses `jakarta.jms`, so the old `sol-jms` (which uses `javax.jms`) does not work with it.
- **External EMS (RabbitMQ):** `camel-spring-rabbitmq`. The old `camel-rabbitmq` is gone from Camel 4.
- **Build:** Maven 3.9.16 through the Maven Wrapper 3.3.4 (`./mvnw`), so nobody has to install Maven.
- **Tests:** JUnit 5 (version from the Camel BOM). `./mvnw -q test` runs all tests. The broker tests run against the real dev brokers, only with `bridgetest` names, and need `SOLACE_PASSWORD` and `RABBITMQ_PASSWORD` set.
- **Logs:** `slf4j-simple`.
- **Running:** a two-stage `Containerfile`, run with `podman run --restart=always --network=host`.

Why: these are the most common choices for a small Camel program, and each one needs the least code and setup. What it costs us: the broker tests cannot run without the dev brokers and the passwords. Moving off Camel Main later (for example to Spring Boot) means moving the connection set-up out of the `RouteBuilder`.
