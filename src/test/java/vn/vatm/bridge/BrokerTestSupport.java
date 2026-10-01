package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.camel.main.Main;
import org.junit.jupiter.api.AfterEach;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class BrokerTestSupport {

    static final String TEST_QUEUE = "q/vnm/vatm/dev/bridgetest/sub";

    // One RabbitMQ for the whole JVM, started on first use and reused by all broker tests.
    static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("docker.io/library/rabbitmq:4-alpine").asCompatibleSubstituteFor("rabbitmq"));

    static synchronized RabbitMQContainer rabbitContainer() {
        if (!RABBIT.isRunning()) {
            RABBIT.start();
        }
        return RABBIT;
    }

    protected Main main;
    private Connection rabbitConnection;

    static void requireTestName(String n) {
        if (n == null || !n.contains("bridgetest")) {
            throw new IllegalArgumentException("Refusing non-bridgetest name: " + n);
        }
    }

    Connection rabbit() throws Exception {
        if (rabbitConnection == null) {
            RabbitMQContainer c = rabbitContainer();
            ConnectionFactory f = new ConnectionFactory();
            f.setHost(c.getHost());
            f.setPort(c.getAmqpPort());
            f.setVirtualHost("/");
            f.setUsername(c.getAdminUsername());
            f.setPassword(c.getAdminPassword());
            rabbitConnection = f.newConnection();
        }
        return rabbitConnection;
    }

    void bindTestQueue(String exchange, String key) throws Exception {
        requireTestName(exchange);
        requireTestName(TEST_QUEUE);
        try (Channel ch = rabbit().createChannel()) {
            ch.queueDeclare(TEST_QUEUE, false, false, false, null);
            ch.queueBind(TEST_QUEUE, exchange, key);
        }
    }

    GetResponse awaitMessage(String queue, long millis) throws Exception {
        requireTestName(queue);
        long deadline = System.currentTimeMillis() + millis;
        try (Channel ch = rabbit().createChannel()) {
            while (true) {
                GetResponse r = ch.basicGet(queue, true);
                if (r != null) {
                    return r;
                }
                if (System.currentTimeMillis() >= deadline) {
                    return null;
                }
                Thread.sleep(200);
            }
        }
    }

    void publishToRabbit(String exchange, String key, AMQP.BasicProperties props, byte[] body) throws Exception {
        requireTestName(exchange);
        try (Channel ch = rabbit().createChannel()) {
            ch.basicPublish(exchange, key, props, body);
        }
    }

    static Map<String, String> testProperties(String rules) {
        for (String entry : rules.split(",")) {
            String[] tokens = entry.trim().split("\\s+");
            if (tokens.length == 4 && !(tokens[2].contains("bridgetest") && tokens[3].contains("bridgetest"))) {
                throw new IllegalArgumentException("Refusing non-bridgetest rule: " + entry.trim());
            }
        }
        Map<String, String> props = new LinkedHashMap<>();
        props.put("bridge.rules", rules);
        props.put("bridge.out.exchange", "x.swim.dev.bridgetest.out");
        props.put("bridge.out.durable-prefix", "q/vnm/vatm/dev/bridgetest/out-");
        props.put("bridge.in.exchange", "x.swim.dev.bridgetest.in");
        props.put("bridge.in.queue", "q/vnm/vatm/dev/bridgetest/in");
        props.put("bridge.in.dlq", "q/vnm/vatm/dev/bridgetest/in-dlq");
        RabbitMQContainer c = rabbitContainer();
        props.put("rabbitmq.host", c.getHost());
        props.put("rabbitmq.port", String.valueOf(c.getAmqpPort()));
        props.put("rabbitmq.vhost", "/");
        props.put("rabbitmq.username", c.getAdminUsername());
        props.put("rabbitmq.password", c.getAdminPassword());
        return props;
    }

    void startBridge(String rules) {
        main = new Main();
        testProperties(rules).forEach(main::addOverrideProperty);
        main.start();
        assertTrue(main.getCamelContext().resolvePropertyPlaceholders("{{bridge.in.queue}}").contains("bridgetest"));
    }

    void stopBridge() {
        if (main != null) {
            main.stop();
            main = null;
        }
    }

    @AfterEach
    void cleanUp() throws Exception {
        stopBridge();
        if (!RABBIT.isRunning()) {
            return;
        }
        Connection rabbitConnection = rabbit();
        String[] queues = {TEST_QUEUE, "q/vnm/vatm/dev/bridgetest/in", "q/vnm/vatm/dev/bridgetest/in-dlq"};
        String[] exchanges = {"x.swim.dev.bridgetest.out", "x.swim.dev.bridgetest.in"};
        for (String q : queues) {
            requireTestName(q);
            try (Channel ch = rabbitConnection.createChannel()) {
                ch.queueDelete(q);
            } catch (Exception ignored) {
                // best effort
            }
        }
        for (String x : exchanges) {
            requireTestName(x);
            try (Channel ch = rabbitConnection.createChannel()) {
                ch.exchangeDelete(x);
            } catch (Exception ignored) {
                // best effort
            }
        }
        try {
            rabbitConnection.close();
        } catch (Exception ignored) {
            // best effort
        }
        this.rabbitConnection = null;
    }
}
