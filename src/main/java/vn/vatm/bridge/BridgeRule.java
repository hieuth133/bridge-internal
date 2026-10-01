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
            String dir = t[0].toUpperCase(java.util.Locale.ROOT);
            if (t.length != 4 || !(dir.equals("IN") || dir.equals("OUT"))) {
                throw new IllegalArgumentException("Bad Bridge Rule: '" + e + "'");
            }
            BridgeRule b = new BridgeRule(Direction.valueOf(dir), t[1], t[2], t[3]);
            if (b.direction == Direction.OUT && !b.source.endsWith("/")) {
                throw new IllegalArgumentException(
                        "Outbound Bridge Rule " + b.name + ": source prefix must end with '/'");
            }
            for (BridgeRule a : rules) {
                if (a.direction == b.direction
                        && (a.source.startsWith(b.source) || b.source.startsWith(a.source))) {
                    throw new IllegalArgumentException("Bridge Rules " + a.name() + " and " + b.name()
                            + " overlap (" + a.direction() + "): " + a.source() + " and " + b.source());
                }
            }
            rules.add(b);
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
