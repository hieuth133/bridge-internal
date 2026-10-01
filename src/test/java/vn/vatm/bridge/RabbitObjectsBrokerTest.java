package vn.vatm.bridge;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

class RabbitObjectsBrokerTest extends BrokerTestSupport {

    @Test
    void bridgeCreatesItsExchangesAndQueuesAtStartup() throws Exception {
        startBridge("");

        for (String x : new String[] {"x.swim.dev.bridgetest.out", "x.swim.dev.bridgetest.in"}) {
            requireTestName(x);
            try (Channel ch = rabbit().createChannel()) {
                ch.exchangeDeclarePassive(x);
            }
        }
        for (String q : new String[] {"q/vnm/vatm/dev/bridgetest/in", "q/vnm/vatm/dev/bridgetest/in-dlq"}) {
            requireTestName(q);
            try (Channel ch = rabbit().createChannel()) {
                ch.queueDeclarePassive(q);
            }
        }
    }
}
