package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LoopGuardBrokerTest extends BrokerTestSupport {

    static final String RULES = "out loop t/vnm/vatm/dev/bridgetest/loop/ t/vnm/vatm/dev/bridgetest/loop/,"
            + " in loopin bridgetest/loop/ t/vnm/vatm/dev/bridgetest/loop/";

    private List<GetResponse> collect(String queue, long millis) throws Exception {
        requireTestName(queue);
        List<GetResponse> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + millis;
        try (Channel ch = rabbit().createChannel()) {
            while (System.currentTimeMillis() < deadline) {
                GetResponse r = ch.basicGet(queue, true);
                if (r != null) {
                    all.add(r);
                } else {
                    Thread.sleep(200);
                }
            }
        }
        return all;
    }

    @Test
    void bridgeMarksMessagesAndDropsMarkedOnes() throws Exception {
        startBridgeAndWait(RULES);
        bindTestQueue("x.swim.dev.bridgetest.out", "#");
        MessageConsumer consumer = subscribeSolace("t/vnm/vatm/dev/bridgetest/loop/>");
        try {
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().contentType("text/plain").build();
            publishToRabbit("x.swim.dev.bridgetest.in", "bridgetest/loop/x", props,
                    "from-rabbit".getBytes(StandardCharsets.UTF_8));

            Message m = consumer.receive(10000);
            assertNotNull(m);
            assertEquals("from-rabbit", assertInstanceOf(TextMessage.class, m).getText());
            assertEquals("rabbitmq", m.getStringProperty("bridgeOrigin"));

            publishToSolace("t/vnm/vatm/dev/bridgetest/loop/y", "from-solace");

            List<GetResponse> got = collect(TEST_QUEUE, 6000);
            assertEquals(1, got.size());
            GetResponse r = got.get(0);
            assertEquals("from-solace", new String(r.getBody(), StandardCharsets.UTF_8));
            assertNotNull(r.getProps().getHeaders());
            assertEquals("solace", String.valueOf(r.getProps().getHeaders().get("bridgeOrigin")));
        } finally {
            consumer.close();
        }
    }
}
