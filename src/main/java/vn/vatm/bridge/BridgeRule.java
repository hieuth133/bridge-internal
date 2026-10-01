package vn.vatm.bridge;

import java.util.ArrayList;
import java.util.List;

public record BridgeRule(Direction direction, String name, String source, String target) {

    public enum Direction { OUT, IN }

    public static List<BridgeRule> parse(String text) {
        List<BridgeRule> rules = new ArrayList<>();
        for (String entry : text.split(",")) {
            String e = entry.trim();
            if (e.isEmpty()) {
                continue;
            }
            String[] t = e.split("\\s+");
            rules.add(new BridgeRule(Direction.valueOf(t[0].toUpperCase()), t[1], t[2], t[3]));
        }
        return rules;
    }

    public String map(String topic) {
        return target + topic.substring(source.length());
    }

    public static String map(List<BridgeRule> rules, Direction d, String topic) {
        for (BridgeRule r : rules) {
            if (r.direction == d && topic.startsWith(r.source)) {
                return r.map(topic);
            }
        }
        return null;
    }
}
