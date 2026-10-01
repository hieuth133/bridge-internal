package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.camel.main.Main;
import org.junit.jupiter.api.Test;

class BridgeStartupTest {

    @Test
    void refusesToStartOnOverlappingRules() {
        Main main = new Main();
        BrokerTestSupport.testProperties(
                "in ext bridgetest/ext/ t/vnm/vatm/dev/bridgetest/ext/, "
                + "in extmet bridgetest/ext/met/ t/vnm/vatm/dev/bridgetest/extmet/")
                .forEach(main::addOverrideProperty);
        try {
            Throwable t = assertThrows(Throwable.class, main::start);
            while (t != null && !(t instanceof IllegalArgumentException)) {
                t = t.getCause();
            }
            assertNotNull(t, "expected an IllegalArgumentException in the cause chain");
            assertEquals("Bridge Rules ext and extmet overlap (IN): bridgetest/ext/ and bridgetest/ext/met/",
                    t.getMessage());
        } finally {
            main.stop();
        }
    }
}
