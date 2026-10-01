package vn.vatm.bridge;

import java.util.List;
import org.apache.camel.builder.RouteBuilder;

public class BridgeRoutes extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        List<BridgeRule> rules = BridgeRule.parse(getContext().resolvePropertyPlaceholders("{{bridge.rules}}"));
    }
}
