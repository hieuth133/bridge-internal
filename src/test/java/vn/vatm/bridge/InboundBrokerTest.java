package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rabbitmq.client.AMQP;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class InboundBrokerTest extends BrokerTestSupport {

    static final String RULES = "in ext bridgetest/ext/ t/vnm/vatm/dev/bridgetest/ext/";
    static final String IN_EXCHANGE = "x.swim.dev.bridgetest.in";
    static final String KEY = "bridgetest/ext/met/metar";
    static final String MAPPED = "t/vnm/vatm/dev/bridgetest/ext/met/metar";
    static final String WILDCARD = "t/vnm/vatm/dev/bridgetest/ext/>";

    private void publish(String text) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().contentType("text/plain").build();
        publishToRabbit(IN_EXCHANGE, KEY, props, text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void rabbitMessageReachesSolaceOnMappedTopic() throws Exception {
        startBridgeAndWait(RULES);
        MessageConsumer consumer = subscribeSolace(WILDCARD);
        try {
            publish("metar-1");

            Message m = consumer.receive(10000);
            assertNotNull(m);
            TextMessage t = assertInstanceOf(TextMessage.class, m);
            assertEquals("metar-1", t.getText());
            assertEquals(MAPPED, ((Topic) m.getJMSDestination()).getTopicName());
        } finally {
            consumer.close();
        }
    }

    @Test
    void messageSentWhileBridgeStoppedIsDeliveredAfterRestart() throws Exception {
        startBridge(RULES);
        stopBridge();
        MessageConsumer consumer = subscribeSolace(WILDCARD);
        try {
            publish("metar-2");
            startBridge(RULES);

            Message m = consumer.receive(15000);
            assertNotNull(m);
            assertEquals("metar-2", assertInstanceOf(TextMessage.class, m).getText());
        } finally {
            consumer.close();
        }
    }
}
