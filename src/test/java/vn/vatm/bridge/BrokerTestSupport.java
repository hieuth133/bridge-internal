package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.camel.main.Main;
import org.junit.jupiter.api.AfterEach;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;
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

    // One Solace for the whole JVM, started on first use and reused by all broker tests.
    static final SolaceContainer SOLACE =
            new SolaceContainer(DockerImageName.parse("docker.io/solace/solace-pubsub-standard:10.25.6.3102")
                    .asCompatibleSubstituteFor("solace/solace-pubsub-standard"))
                    // withTopic is what exposes the SMF port (needed for getOrigin)
                    .withTopic("t/vnm/vatm/dev/bridgetest/>", Service.SMF);

    static synchronized SolaceContainer solaceContainer() {
        if (!SOLACE.isRunning()) {
            SOLACE.start();
        }
        return SOLACE;
    }

    protected Main main;
    private Connection rabbitConnection;
    private jakarta.jms.Connection solaceConnection;
    private final List<String> rememberedRules = new ArrayList<>();

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

    jakarta.jms.Connection solace() throws Exception {
        if (solaceConnection == null) {
            SolaceContainer c = solaceContainer();
            SolConnectionFactory f = SolJmsUtility.createConnectionFactory();
            f.setHost(c.getOrigin(Service.SMF));
            f.setVPN(c.getVpn());
            f.setUsername(c.getUsername());
            f.setPassword(c.getPassword());
            f.setDynamicDurables(true);
            solaceConnection = f.createConnection();
            solaceConnection.start();
        }
        return solaceConnection;
    }

    void publishToSolace(String topic, Function<Session, Message> build) throws Exception {
        requireTestName(topic);
        Session session = solace().createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            session.createProducer(session.createTopic(topic)).send(build.apply(session));
        } finally {
            session.close();
        }
    }

    void publishToSolace(String topic, String text) throws Exception {
        publishToSolace(topic, session -> {
            try {
                TextMessage m = session.createTextMessage(text);
                m.setJMSDeliveryMode(jakarta.jms.DeliveryMode.PERSISTENT);
                return m;
            } catch (JMSException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    MessageConsumer subscribeSolace(String topicWithWildcard) throws Exception {
        requireTestName(topicWithWildcard);
        Session session = solace().createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createConsumer(session.createTopic(topicWithWildcard));
    }

    void deleteExchange(String exchange) throws Exception {
        requireTestName(exchange);
        try (Channel ch = rabbit().createChannel()) {
            ch.exchangeDelete(exchange);
        }
    }

    void declareTopicExchange(String exchange) throws Exception {
        requireTestName(exchange);
        try (Channel ch = rabbit().createChannel()) {
            ch.exchangeDeclare(exchange, "topic", true);
        }
    }

    void bindTestQueue(String exchange, String key) throws Exception {
        requireTestName(exchange);
        requireTestName(TEST_QUEUE);
        try (Channel ch = rabbit().createChannel()) {
            ch.queueDeclare(TEST_QUEUE, true, false, false, null);
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
        // Env vars (RABBITMQ_HOST, RABBITMQ_PASSWORD, ...) must never override these test values,
        // or the Bridge would log in with the wrong password or reach a live broker.
        props.put("camel.component.properties.environment-variable-mode", "0");
        props.put("bridge.rules", rules);
        props.put("bridge.out.exchange", "x.swim.dev.bridgetest.out");
        props.put("bridge.out.durable-prefix", "q/vnm/vatm/dev/bridgetest/out-");
        props.put("bridge.in.exchange", "x.swim.dev.bridgetest.in");
        props.put("bridge.in.queue", "q/vnm/vatm/dev/bridgetest/in");
        props.put("bridge.in.dlq", "q/vnm/vatm/dev/bridgetest/in-dlq");
        SolaceContainer sc = solaceContainer();
        props.put("solace.host", sc.getOrigin(Service.SMF));
        props.put("solace.vpn", sc.getVpn());
        props.put("solace.username", sc.getUsername());
        props.put("solace.password", sc.getPassword());
        RabbitMQContainer c = rabbitContainer();
        props.put("rabbitmq.host", c.getHost());
        props.put("rabbitmq.port", String.valueOf(c.getAmqpPort()));
        props.put("rabbitmq.vhost", "/");
        props.put("rabbitmq.username", c.getAdminUsername());
        props.put("rabbitmq.password", c.getAdminPassword());
        return props;
    }

    void startBridge(String rules) {
        rememberedRules.add(rules);
        main = new Main();
        testProperties(rules).forEach(main::addOverrideProperty);
        main.start();
        assertTrue(main.getCamelContext().resolvePropertyPlaceholders("{{bridge.in.queue}}").contains("bridgetest"));
    }

    void startBridgeAndWait(String rules) throws Exception {
        startBridge(rules);
        Thread.sleep(2000);
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
        if (SOLACE.isRunning()) {
            jakarta.jms.Connection sol = solace();
            try (Session session = sol.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                for (String rules : rememberedRules) {
                    for (String entry : rules.split(",")) {
                        String[] t = entry.trim().split("\\s+");
                        if (t.length == 4 && t[0].equalsIgnoreCase("out")) {
                            String durable = "q/vnm/vatm/dev/bridgetest/out-" + t[1];
                            requireTestName(durable);
                            try {
                                session.unsubscribe(durable);
                            } catch (JMSException ignored) {
                                // best effort
                            }
                        }
                    }
                }
            }
            try {
                sol.close();
            } catch (JMSException ignored) {
                // best effort
            }
            solaceConnection = null;
        }
        rememberedRules.clear();
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
