package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import jakarta.jms.DeliveryMode;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MessageContentBrokerTest extends BrokerTestSupport {

    static final String RULES =
            "out atfm t/vnm/vatm/dev/bridgetest/atfm/ t/vnm/vatm/dev/bridgetest/atfm/";
    static final String TOPIC = "t/vnm/vatm/dev/bridgetest/atfm/v1/fpl";
    static final String OUT_EXCHANGE = "x.swim.dev.bridgetest.out";

    @Test
    void outboundKeepsProperties() throws Exception {
        startBridgeAndWait(RULES);
        bindTestQueue(OUT_EXCHANGE, TOPIC);

        AtomicReference<TextMessage> sent = new AtomicReference<>();
        publishToSolace(TOPIC, session -> {
            try {
                TextMessage m = session.createTextMessage("{\"id\":1}");
                m.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                m.setStringProperty("contentType", "application/json");
                m.setJMSCorrelationID("corr-out-1");
                m.setStringProperty("station", "VVNB");
                m.setIntProperty("count", 7);
                m.setBooleanProperty("urgent", true);
                sent.set(m);
                return m;
            } catch (JMSException e) {
                throw new IllegalStateException(e);
            }
        });
        String sentMessageId = sent.get().getJMSMessageID();
        assertNotNull(sentMessageId);

        GetResponse r = awaitMessage(TEST_QUEUE, 10000);
        assertNotNull(r);
        assertEquals("application/json", r.getProps().getContentType());
        assertEquals("corr-out-1", r.getProps().getCorrelationId());
        assertEquals(sentMessageId, r.getProps().getMessageId());
        Map<String, Object> headers = r.getProps().getHeaders();
        assertNotNull(headers);
        assertEquals("VVNB", String.valueOf(headers.get("station")));
        assertEquals(7, ((Number) headers.get("count")).intValue());
        assertEquals(Boolean.TRUE, headers.get("urgent"));
        assertEquals("{\"id\":1}", new String(r.getBody(), StandardCharsets.UTF_8));
    }

    static final String IN_RULES = "in ext bridgetest/ext/ t/vnm/vatm/dev/bridgetest/ext/";
    static final String IN_EXCHANGE = "x.swim.dev.bridgetest.in";
    static final String IN_KEY = "bridgetest/ext/met/metar";

    @Test
    void inboundKeepsPropertiesAndJsonIsText() throws Exception {
        startBridgeAndWait(IN_RULES);
        MessageConsumer consumer = subscribeSolace("t/vnm/vatm/dev/bridgetest/ext/>");
        try {
            Map<String, Object> headers = new HashMap<>();
            headers.put("station", "VVNB");
            headers.put("count", 7);
            headers.put("urgent", true);
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .correlationId("corr-in-1")
                    .messageId("mid-in-1")
                    .headers(headers)
                    .build();
            publishToRabbit(IN_EXCHANGE, IN_KEY, props, "{\"id\":2}".getBytes(StandardCharsets.UTF_8));

            Message m = consumer.receive(10000);
            assertNotNull(m);
            TextMessage t = assertInstanceOf(TextMessage.class, m);
            assertEquals("{\"id\":2}", t.getText());
            assertEquals("corr-in-1", t.getJMSCorrelationID());
            assertEquals("application/json", t.getStringProperty("contentType"));
            assertEquals("mid-in-1", t.getStringProperty("messageId"));
            assertEquals("VVNB", t.getStringProperty("station"));
            assertEquals(7, t.getIntProperty("count"));
            assertTrue(t.getBooleanProperty("urgent"));
        } finally {
            consumer.close();
        }
    }

    @Test
    void inboundBinaryIsBytes() throws Exception {
        startBridgeAndWait(IN_RULES);
        MessageConsumer consumer = subscribeSolace("t/vnm/vatm/dev/bridgetest/ext/>");
        try {
            byte[] body = {0, 1, 2, (byte) 0xFF};
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/octet-stream")
                    .build();
            publishToRabbit(IN_EXCHANGE, IN_KEY, props, body);

            Message m = consumer.receive(10000);
            assertNotNull(m);
            BytesMessage b = assertInstanceOf(BytesMessage.class, m);
            byte[] got = new byte[(int) b.getBodyLength()];
            b.readBytes(got);
            assertArrayEquals(body, got);
        } finally {
            consumer.close();
        }
    }
}
