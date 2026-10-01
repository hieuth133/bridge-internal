package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AckAfterAcceptBrokerTest extends BrokerTestSupport {

    static final String RULES =
            "out atfm t/vnm/vatm/dev/bridgetest/atfm/ t/vnm/vatm/dev/bridgetest/atfm/";
    static final String TOPIC = "t/vnm/vatm/dev/bridgetest/atfm/v1/fpl";
    static final String OUT_EXCHANGE = "x.swim.dev.bridgetest.out";

    @Test
    void messageRefusedByRabbitIsHeldInSolaceAndDeliveredLater() throws Exception {
        startBridgeAndWait(RULES);
        deleteExchange(OUT_EXCHANGE);

        publishToSolace(TOPIC, "held-1");
        Thread.sleep(3000);

        declareTopicExchange(OUT_EXCHANGE);
        bindTestQueue(OUT_EXCHANGE, TOPIC);

        GetResponse r = awaitMessage(TEST_QUEUE, 15000);
        assertNotNull(r, "message was lost instead of being held in Solace");
        assertEquals("held-1", new String(r.getBody(), StandardCharsets.UTF_8));
    }
}
