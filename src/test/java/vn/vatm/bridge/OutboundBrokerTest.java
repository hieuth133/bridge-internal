package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class OutboundBrokerTest extends BrokerTestSupport {

    static final String RULES =
            "out atfm t/vnm/vatm/dev/bridgetest/atfm/ t/vnm/vatm/dev/bridgetest/atfm/";
    static final String TOPIC = "t/vnm/vatm/dev/bridgetest/atfm/v1/fpl";
    static final String OUT_EXCHANGE = "x.swim.dev.bridgetest.out";

    @Test
    void solaceMessageReachesOutboundExchangeWithMappedRoutingKey() throws Exception {
        startBridgeAndWait(RULES);
        bindTestQueue(OUT_EXCHANGE, TOPIC);

        publishToSolace(TOPIC, "fpl-1");

        GetResponse r = awaitMessage(TEST_QUEUE, 10000);
        assertNotNull(r);
        assertEquals("fpl-1", new String(r.getBody(), StandardCharsets.UTF_8));
        assertEquals(TOPIC, r.getEnvelope().getRoutingKey());
    }

    @Test
    void messageSentWhileBridgeStoppedIsDeliveredAfterRestart() throws Exception {
        startBridgeAndWait(RULES);
        stopBridge();
        bindTestQueue(OUT_EXCHANGE, TOPIC);

        publishToSolace(TOPIC, "fpl-2");
        startBridge(RULES);

        GetResponse r = awaitMessage(TEST_QUEUE, 15000);
        assertNotNull(r);
        assertEquals("fpl-2", new String(r.getBody(), StandardCharsets.UTF_8));
    }
}
