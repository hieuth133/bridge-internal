package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import vn.vatm.bridge.BridgeRule.Direction;

class BridgeRuleTest {

    private static final String TEXT =
            "out atfm t/vnm/vatm/dev/atfm/ t/vnm/vatm/dev/atfm/,\n  in  ext  ext/   t/vnm/vatm/dev/ext/";

    @Test
    void parsesRules() {
        List<BridgeRule> rules = BridgeRule.parse(TEXT);
        assertEquals(2, rules.size());
        assertEquals(
                new BridgeRule(Direction.OUT, "atfm", "t/vnm/vatm/dev/atfm/", "t/vnm/vatm/dev/atfm/"),
                rules.get(0));
    }

    @Test
    void mapsNamesByDirection() {
        List<BridgeRule> rules = BridgeRule.parse(TEXT);
        assertEquals("t/vnm/vatm/dev/ext/met/metar", BridgeRule.map(rules, Direction.IN, "ext/met/metar"));
        assertEquals("t/vnm/vatm/dev/atfm/v1/fpl",
                BridgeRule.map(rules, Direction.OUT, "t/vnm/vatm/dev/atfm/v1/fpl"));
        assertNull(BridgeRule.map(rules, Direction.OUT, "ext/met/metar"));
        assertNull(BridgeRule.map(rules, Direction.IN, "other/x"));
    }

    @Test
    void blankTextGivesNoRules() {
        assertTrue(BridgeRule.parse("").isEmpty());
        assertTrue(BridgeRule.parse("  ").isEmpty());
    }
}
