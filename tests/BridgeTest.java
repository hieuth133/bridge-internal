import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manual Bridge test: sends one message through the running Bridge and checks that it arrives on the other broker
 * unchanged. The only change allowed is that VV_ROUTE, which exists only inside RabbitMQ, does not reach Solace.
 * RabbitMQ is reached over AMQP 1.0 only, like every EMS in the region (ADR 0007).
 *
 *   solace-pubsub    Solace topic t/vnm/vatm/dev/atfm/v1/fpl            -> RabbitMQ x/vnm/vatm/dev/ingress (B-04)
 *   solace-rr        Solace topic tr/vnm/vatm/vnm/vna/dev/..., reply-to -> RabbitMQ x/vnm/vatm/dev/ingress (B-04)
 *   rabbitmq-pubsub  RabbitMQ x/vnm/vatm/dev/swim                       -> Solace topic (B-03)
 *   rabbitmq-rr      RabbitMQ x/vnm/vatm/dev/route, VV_ROUTE=VV_VATM    -> Solace topic, VATM selector (B-03)
 *   ping             log in to both brokers, and check that B-03 and B-04 are connected to RabbitMQ over AMQP 1.0
 *
 * Solace to RabbitMQ: a temporary queue bound to the fanout x/vnm/vatm/dev/ingress gets a copy of the message, so
 * nothing is taken out of q/vnm/vatm/dev/router/in. RabbitMQ to Solace: the message is put on swim or route as the
 * Router would, and a temporary Solace subscription receives it. Both sides pick out this run's message by its
 * correlation-id. Settings come from tests/env.sh. Exit code 0 means every check passed, 1 that a check failed,
 * 2 that the test could not run.
 */
public class BridgeTest {
    static final String INGRESS = "x/vnm/vatm/dev/ingress", SWIM = "x/vnm/vatm/dev/swim", ROUTE = "x/vnm/vatm/dev/route";
    static final String CONTENT_TYPE = "application/xml";
    static final int TIMEOUT_SECONDS = 30;
    // Solace_Selector sheet: who VATM is on Solace. '_' is escaped because in LIKE it matches any character.
    static final String VATM_SELECTOR = "(APAC_RECIPIENT_LIST = 'VV_VATM' OR APAC_RECIPIENT_LIST LIKE 'VV\\_VATM,%' ESCAPE '\\'"
            + " OR APAC_RECIPIENT_LIST LIKE '%,VV\\_VATM' ESCAPE '\\' OR APAC_RECIPIENT_LIST LIKE '%,VV\\_VATM,%' ESCAPE '\\')";

    // docs/Documents/Pathfinder_Headers_Metadata_updated 24 Sep 2026.xlsx, sheet "Headers for SIPG Test"
    static Map<String, String> pathfinderHeaders() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("APAC_SOURCE", "VV_VATM");
        h.put("APAC_RECIPIENT_LIST", "WS_CAAS,RJ_JCAB");
        h.put("APAC_CATEGORY", "FIXM");
        h.put("APAC_CATEGORY_VERSION", "FIXM_4_3_FF_ICE");
        h.put("APAC_MESSAGE_TYPE", "FILED_FLIGHT_PLAN");
        h.put("DEP_AIRPORT", "VVTS");
        h.put("ARR_AIRPORT", "WSSS");
        h.put("AIRLINE", "HVN");
        h.put("ACID", "HVN651");
        h.put("GUFI", "ec5c6d8e-6b3b-4b6c-a3a6-2c1f6f3d9a10");
        h.put("GUFI_NAMESPACE_IDENTIFIER", "FF-ICE");
        h.put("EOBT", "2026-10-01T06:00:00Z");
        h.put("FFICE_PHASE", "FILED");
        h.put("APAC_TIMESTAMP", "VV_EEMS_OUT:1790873508104, WS_GEMS_IN: 1790873508200");
        return h;
    }

    // ~19 KB of UTF-8 XML with Vietnamese, arrows, entities, quotes and tabs.
    static byte[] pathfinderPayload() {
        StringBuilder s = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<fx:Flight xmlns:fx=\"http://www.fixm.aero/flight/4.3\">\n");
        for (int i = 0; i < 150; i++) {
            s.append("  <fx:routePoint seq=\"").append(i).append("\">Điểm ").append(i)
             .append(": Tân Sơn Nhất → Changi &amp; &lt;FL350&gt; \"quoted\" 'single'\t(tab)</fx:routePoint>\n");
        }
        return s.append("</fx:Flight>\n").toString().getBytes(StandardCharsets.UTF_8);
    }

    static final String USAGE = String.join("\n",
            "usage: BridgeTest <solace-pubsub|solace-rr|rabbitmq-pubsub|rabbitmq-rr|ping> [options]",
            "  --recipients LIST  APAC_RECIPIENT_LIST to send, e.g. VV_VATM,WS_CAAS",
            "  --payload FILE     send this UTF-8 file instead of the sample FIXM message",
            "  --topic TOPIC      Solace topic to send to (solace-*)",
            "  --key KEY          RabbitMQ routing key to send with (rabbitmq-*)",
            "Run `source tests/env.sh` first.");

    public static void main(String[] args) throws Exception {
        Logger.getLogger("").setLevel(Level.WARNING);  // the Solace API and Netty log at INFO
        if (args.length == 0 || args[0].equals("-h") || args[0].equals("--help")) {
            System.out.println(USAGE);
            return;
        }
        String scenario = args[0];
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            String own = scenario.startsWith("solace-") ? "--topic" : scenario.startsWith("rabbitmq-") ? "--key" : null;
            if (!Arrays.asList("--recipients", "--payload", own).contains(args[i]) || i + 1 >= args.length) {
                fail(USAGE);
            }
            opts.put(args[i].substring(2), args[i + 1]);
        }
        int failures = 0;
        try {
            failures = run(scenario, opts);
        } catch (JMSException e) {
            fail("FAIL Solace: " + e.getMessage());
        } catch (AmqpException e) {
            fail("FAIL RabbitMQ: " + e.getMessage());
        } catch (java.io.IOException e) {
            fail("FAIL " + e);
        }
        System.out.println(failures == 0 ? "\nPASS" : "\nFAIL: " + failures + " check(s) failed");
        System.exit(failures == 0 ? 0 : 1);
    }

    /** Returns the number of failed checks. */
    static int run(String scenario, Map<String, String> opts) throws Exception {
        String run = "bridge-test-" + System.currentTimeMillis();
        byte[] payload = opts.containsKey("payload") ? Files.readAllBytes(Path.of(opts.get("payload"))) : pathfinderPayload();
        Map<String, String> headers = pathfinderHeaders();
        switch (scenario) {
            case "ping":
                return ping(new Env());
            case "solace-pubsub":
                headers.put("APAC_RECIPIENT_LIST", opts.getOrDefault("recipients", "VV_HVN"));
                return solaceToRabbitmq(new Env(), run, opts.getOrDefault("topic", "t/vnm/vatm/dev/atfm/v1/fpl"), headers, null, payload);
            case "solace-rr":
                headers.put("APAC_RECIPIENT_LIST", opts.getOrDefault("recipients", "VV_VATM,VV_HVN,WS_CAAS"));
                return solaceToRabbitmq(new Env(), run, opts.getOrDefault("topic", "tr/vnm/vatm/vnm/vna/dev/fpms/v1/filing/reply"),
                        headers, "q/vnm/vatm/dev/fpms/reply", payload);
            case "rabbitmq-pubsub":
                headers.put("APAC_SOURCE", "VV_ACV");
                // A space in the list: the Bridge must not trim it.
                headers.put("APAC_RECIPIENT_LIST", opts.getOrDefault("recipients", "VV_VATM, WS_CAAS"));
                return rabbitmqToSolace(new Env(), run, SWIM, opts.getOrDefault("key", "t.vnm.acv.dev.aodb.v1.departure.publish.vvts"),
                        headers, null, payload);
            case "rabbitmq-rr":
                // HVN sends to VATM and CAAS; the Router's copy for VATM carries VV_ROUTE=VV_VATM, Solace must get the full list.
                headers.put("APAC_SOURCE", "VV_HVN");
                headers.put("APAC_RECIPIENT_LIST", opts.getOrDefault("recipients", "VV_VATM,WS_CAAS"));
                return rabbitmqToSolace(new Env(), run, ROUTE, opts.getOrDefault("key", "tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request"),
                        headers, "q/vnm/vna/dev/swim/reply", payload);
            default:
                fail(USAGE);
                return 1;
        }
    }

    /** Logs in to both brokers; then the Bridge's own connections must be on RabbitMQ, over AMQP 1.0. */
    static int ping(Env env) throws Exception {
        try (Connection c = env.solace().createConnection()) {
            System.out.println("OK   Solace " + env.solaceHost + " VPN " + env.solaceVpn + " as " + env.solaceUser);
        }
        try (Rabbitmq r = new Rabbitmq(env)) {
            System.out.println("OK   RabbitMQ " + r.where + " as " + env.rabbitUser + ", AMQP 1.0, RabbitMQ "
                    + r.connection.connectionInfo().brokerVersion());
        }
        // B-03 and B-04 name their connections; the management API shows the name and the protocol of each one.
        List<?> connections = (List<?>) new ManagementApi(env).get("/connections");
        Report rep = new Report("Bridge connections on RabbitMQ");
        for (String name : List.of("vatm-bridge B-03", "vatm-bridge B-04")) {
            List<Object> protocols = new ArrayList<>();
            for (Object o : connections) {
                Map<?, ?> c = (Map<?, ?>) o;
                if (c.containsValue(name) || c.get("client_properties") instanceof Map && ((Map<?, ?>) c.get("client_properties")).containsValue(name)) {
                    protocols.add(c.get("protocol"));
                }
            }
            rep.check(name, protocols, List.of("AMQP 1-0"));
        }
        return rep.failures;
    }

    /** Sends on Solace, then reads the copy that B-04 put on x/vnm/vatm/dev/ingress. */
    static int solaceToRabbitmq(Env env, String run, String topic, Map<String, String> headers, String replyTo, byte[] payload)
            throws Exception {
        try (Rabbitmq r = new Rabbitmq(env)) {
            String queue = "q/vnm/vatm/dev/bridge-test/" + run;
            Management management = r.connection.management();
            // expires: RabbitMQ removes the queue by itself if this program dies before deleting it.
            management.queue(queue).classic().queue().expires(Duration.ofMinutes(10)).declare();
            try {
                management.binding().sourceExchange(INGRESS).destinationQueue(queue).key("").bind();
                BlockingQueue<com.rabbitmq.client.amqp.Message> arrivals = new LinkedBlockingQueue<>();
                r.connection.consumerBuilder().queue(queue).messageHandler((context, m) -> {
                    context.accept();
                    if (run.equals(m.correlationId())) {
                        arrivals.add(m);
                    }
                }).build();
                try (Connection c = env.solace().createConnection()) {
                    Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    TextMessage m = s.createTextMessage(new String(payload, StandardCharsets.UTF_8));
                    for (Map.Entry<String, String> h : headers.entrySet()) {
                        m.setStringProperty(h.getKey(), h.getValue());
                    }
                    m.setStringProperty("contentType", CONTENT_TYPE);
                    m.setStringProperty("messageId", run);
                    m.setJMSCorrelationID(run);
                    if (replyTo != null) {
                        m.setJMSReplyTo(s.createQueue(replyTo));
                    }
                    MessageProducer p = s.createProducer(s.createTopic(topic));
                    p.setDeliveryMode(DeliveryMode.PERSISTENT);
                    p.send(m);
                }
                System.out.println("Sent to Solace topic " + topic + ", correlation-id " + run + ". Waiting for it on RabbitMQ " + INGRESS + " ...");
                com.rabbitmq.client.amqp.Message got = arrivals.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (got == null) {
                    System.out.println("FAIL nothing arrived on RabbitMQ in " + TIMEOUT_SECONDS + " s (is the Bridge running, and does"
                            + " Solace queue q/vnm/vatm/dev/bridge/outbound subscribe to " + topic + "?)");
                    return 1;
                }
                Map<String, Object> arrived = new LinkedHashMap<>();
                got.forEachProperty(arrived::put);
                Report rep = new Report("Arrived on RabbitMQ " + got.annotation("x-exchange") + " (AMQP 1.0)");
                rep.check("routing key", got.annotation("x-routing-key"), topic.replace('/', '.'));
                rep.headers(arrived, headers);
                List<String> extra = new ArrayList<>(new TreeSet<>(arrived.keySet()));
                extra.removeAll(headers.keySet());
                rep.check("no other header", extra, List.of());
                rep.check("correlation-id", got.correlationId(), run);
                rep.check("content-type", got.contentType(), CONTENT_TYPE);
                rep.check("message-id", got.messageId(), run);
                rep.check("reply-to", got.replyTo(), replyTo);
                rep.check("durable", got.durable(), true);
                rep.payload(got.body(), payload);
                return rep.failures;
            } finally {
                management.queueDelete(queue);
            }
        }
    }

    /** Subscribes on Solace, puts the message on swim or route as the Router would, and waits for B-03 to deliver it. */
    static int rabbitmqToSolace(Env env, String run, String exchange, String key, Map<String, String> headers, String replyTo,
                                byte[] payload) throws Exception {
        String topic = key.replace('.', '/');
        boolean headerRouting = exchange.equals(ROUTE);
        String selector = "JMSCorrelationID = '" + run + "'" + (headerRouting ? " AND " + VATM_SELECTOR : "");
        try (Connection c = env.solace().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createTopic(topic), selector);
            c.start();

            try (Rabbitmq r = new Rabbitmq(env)) {
                Publisher publisher = r.connection.publisherBuilder().exchange(exchange).key(key).build();
                com.rabbitmq.client.amqp.Message m = publisher.message(payload).durable(true).contentType(CONTENT_TYPE)
                        .correlationId(run).messageId(run);
                if (replyTo != null) {
                    m.replyTo(replyTo);
                }
                headers.forEach(m::property);
                if (headerRouting) {
                    m.property("VV_ROUTE", "VV_VATM");  // the Router's copy for VATM; it must not reach Solace
                }
                CompletableFuture<Publisher.Context> outcome = new CompletableFuture<>();
                publisher.publish(m, outcome::complete);
                Publisher.Status status = outcome.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).status();
                if (status != Publisher.Status.ACCEPTED) {
                    System.out.println("FAIL RabbitMQ " + exchange + " did not accept key " + key + ": " + status);
                    return 1;
                }
            }
            System.out.println("Sent to RabbitMQ " + exchange + " key " + key + (headerRouting ? " with VV_ROUTE=VV_VATM" : "")
                    + ", correlation-id " + run + ". Waiting for it on Solace topic " + topic + " ...");
            Message m = consumer.receive(TIMEOUT_SECONDS * 1000L);
            if (m == null) {
                System.out.println("FAIL nothing arrived on Solace in " + TIMEOUT_SECONDS + " s (is the Bridge running, does"
                        + " q/vnm/vatm/dev/bridge/inbound get key " + key + (headerRouting ? ", and is VV_VATM in the list?)" : "?)"));
                return 1;
            }
            Map<String, Object> arrived = new LinkedHashMap<>();
            for (Enumeration<?> names = m.getPropertyNames(); names.hasMoreElements(); ) {
                String name = (String) names.nextElement();
                arrived.put(name, m.getObjectProperty(name));
            }
            Report rep = new Report("Arrived on Solace" + (headerRouting ? " (VATM selector matched)" : ""));
            rep.check("topic", name(m.getJMSDestination()), "topic " + topic);
            rep.headers(arrived, headers);
            rep.check("VV_ROUTE", arrived.get("VV_ROUTE"), null);
            rep.check("correlation-id", m.getJMSCorrelationID(), run);
            rep.check("content-type", arrived.get("contentType"), CONTENT_TYPE);
            rep.check("message-id", arrived.get("messageId"), run);
            rep.check("reply-to", name(m.getJMSReplyTo()), replyTo == null ? null : "queue " + replyTo);
            byte[] body;
            if (m instanceof TextMessage) {
                body = ((TextMessage) m).getText().getBytes(StandardCharsets.UTF_8);
            } else {
                BytesMessage b = (BytesMessage) m;
                body = new byte[(int) b.getBodyLength()];
                b.readBytes(body);
            }
            rep.payload(body, payload);
            return rep.failures;
        }
    }

    static String name(Destination d) throws Exception {
        if (d instanceof Queue) {
            return "queue " + ((Queue) d).getQueueName();
        }
        return d instanceof Topic ? "topic " + ((Topic) d).getTopicName() : null;
    }

    static void fail(String message) {
        System.err.println(message);
        System.exit(2);
    }

    static class Report {
        int failures;

        Report(String title) {
            System.out.println("\n" + title);
        }

        void check(String name, Object got, Object want) {
            line(Objects.equals(got, want), name, show(got), show(want));
        }

        void headers(Map<String, Object> arrived, Map<String, String> sent) {
            sent.forEach((k, v) -> check(k, arrived.get(k), v));
        }

        void payload(byte[] got, byte[] want) throws Exception {
            line(Arrays.equals(got, want), "payload", digest(got), digest(want));
        }

        void line(boolean ok, String name, String got, String want) {
            failures += ok ? 0 : 1;
            System.out.printf("  %s %-27s %s%s%n", ok ? "OK  " : "FAIL", name, got, ok ? "" : "  (expected " + want + ")");
        }

        static String show(Object o) {
            return o == null ? "absent" : o instanceof String ? "'" + o + "'" : String.valueOf(o);
        }

        static String digest(byte[] b) throws Exception {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(b);
            StringBuilder s = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                s.append(String.format("%02x", d[i]));
            }
            return b.length + " bytes, sha256 " + s;
        }
    }

    /** Settings from tests/env.sh. */
    static class Env {
        final String solaceHost = need("SOLACE_HOST"), solaceVpn = need("SOLACE_VPN"), solaceUser = need("SOLACE_USERNAME");
        final String rabbitHost = need("RABBITMQ_HOST"), rabbitPort = need("RABBITMQ_PORT"), vhost = need("RABBITMQ_VHOST");
        final String rabbitUser = need("RABBITMQ_USERNAME"), managementPort = need("RABBITMQ_MANAGEMENT_PORT");

        static String need(String name) {
            String v = System.getenv(name);
            if (v == null || v.isEmpty()) {
                fail(name + " is not set. Run `source tests/env.sh` first.");
            }
            return v;
        }

        SolConnectionFactory solace() throws Exception {
            SolConnectionFactory cf = SolJmsUtility.createConnectionFactory();
            cf.setHost(solaceHost);
            cf.setVPN(solaceVpn);
            cf.setUsername(solaceUser);
            cf.setPassword(need("SOLACE_PASSWORD"));
            cf.setDirectTransport(false);  // guaranteed: persistent sends, and the selector runs on the broker
            return cf;
        }
    }

    /** RabbitMQ over AMQP 1.0. */
    static class Rabbitmq implements AutoCloseable {
        final Environment environment = new AmqpEnvironmentBuilder().build();
        final com.rabbitmq.client.amqp.Connection connection;
        final String where;

        Rabbitmq(Env env) {
            where = env.rabbitHost + ":" + env.rabbitPort + " vhost " + env.vhost;
            try {
                connection = environment.connectionBuilder().host(env.rabbitHost).port(Integer.parseInt(env.rabbitPort))
                        .virtualHost(env.vhost).username(env.rabbitUser).password(Env.need("RABBITMQ_PASSWORD")).name("bridge-test").build();
            } catch (RuntimeException e) {
                environment.close();
                throw e;
            }
        }

        @Override
        public void close() {
            environment.close();  // closes the connection too
        }
    }

    /** RabbitMQ management HTTP API, read only: it shows the protocol of each connection, which AMQP itself does not. */
    static class ManagementApi {
        final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)  // RabbitMQ drops h2c upgrades
                .connectTimeout(Duration.ofSeconds(10)).build();
        final String base, auth;

        ManagementApi(Env env) {
            base = "http://" + env.rabbitHost + ":" + env.managementPort + "/api";
            auth = "Basic " + Base64.getEncoder().encodeToString((env.rabbitUser + ":" + Env.need("RABBITMQ_PASSWORD"))
                    .getBytes(StandardCharsets.UTF_8));
        }

        Object get(String path) throws Exception {
            HttpRequest req = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(30)).header("Authorization", auth).build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 300) {
                String err = res.body();
                fail("FAIL RabbitMQ management GET " + path + " -> " + res.statusCode() + ": " + err.substring(0, Math.min(200, err.length())));
            }
            return Json.read(res.body());
        }
    }

    /** Just enough JSON to read the management API. */
    static class Json {
        final String s;
        int i;

        Json(String s) {
            this.s = s;
        }

        static Object read(String s) {
            return new Json(s).value();
        }

        Object value() {
            skip();
            char c = s.charAt(i);
            if (c == '{') {
                Map<String, Object> m = new LinkedHashMap<>();
                i++;
                skip();
                if (s.charAt(i) == '}') {
                    i++;
                    return m;
                }
                while (true) {
                    String k = (String) value();
                    skip();
                    i++;  // ':'
                    m.put(k, value());
                    skip();
                    if (s.charAt(i++) == '}') {
                        return m;
                    }
                }
            }
            if (c == '[') {
                List<Object> l = new ArrayList<>();
                i++;
                skip();
                if (s.charAt(i) == ']') {
                    i++;
                    return l;
                }
                while (true) {
                    l.add(value());
                    skip();
                    if (s.charAt(i++) == ']') {
                        return l;
                    }
                }
            }
            if (c == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while ((c = s.charAt(i++)) != '"') {
                    if (c != '\\') {
                        b.append(c);
                        continue;
                    }
                    c = s.charAt(i++);
                    if (c == 'u') {
                        b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    } else {
                        b.append(c == 'n' ? '\n' : c == 't' ? '\t' : c == 'r' ? '\r' : c == 'b' ? '\b' : c == 'f' ? '\f' : c);
                    }
                }
                return b.toString();
            }
            int start = i;
            while (i < s.length() && ",}] \n\r\t".indexOf(s.charAt(i)) < 0) {
                i++;
            }
            String word = s.substring(start, i);
            if (word.equals("true") || word.equals("false")) {
                return Boolean.valueOf(word);
            }
            if (word.equals("null")) {
                return null;
            }
            return word.matches("-?\\d+") ? (Object) Long.parseLong(word) : (Object) Double.parseDouble(word);
        }

        void skip() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
