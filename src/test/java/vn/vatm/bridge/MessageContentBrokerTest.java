package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rabbitmq.client.GetResponse;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
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
}
