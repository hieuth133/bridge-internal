package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.camel.main.Main;
import org.junit.jupiter.api.AfterEach;

public abstract class BrokerTestSupport {

    protected Main main;

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
    void cleanUp() {
        stopBridge();
    }
}
