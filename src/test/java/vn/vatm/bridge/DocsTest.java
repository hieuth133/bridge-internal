package vn.vatm.bridge;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DocsTest {

    @Test
    void containerAndRunCommandDocumented() throws IOException {
        Path containerfile = Path.of("Containerfile");
        assertTrue(Files.exists(containerfile), "Containerfile missing");
        String container = Files.readString(containerfile);
        assertTrue(container.contains("org.apache.camel.main.Main"),
                "Containerfile must start org.apache.camel.main.Main");

        Path readmeFile = Path.of("README.md");
        assertTrue(Files.exists(readmeFile), "README.md missing");
        String readme = Files.readString(readmeFile);
        for (String needle : new String[] {"podman run", "--restart=always", "--network=host",
                "podman-restart", "SOLACE_PASSWORD", "RABBITMQ_PASSWORD"}) {
            assertTrue(readme.contains(needle), "README.md must mention: " + needle);
        }
    }

    @Test
    void readmeExplainsDemo() throws IOException {
        Path readmeFile = Path.of("README.md");
        assertTrue(Files.exists(readmeFile), "README.md missing");
        String readme = Files.readString(readmeFile);
        for (String needle : new String[] {"```mermaid", "q/vnm/vatm/dev/bridge/out-atfm",
                "x.swim.dev.bridge.out", "x.swim.dev.bridge.in", "q/vnm/vatm/dev/bridge/in-dlq",
                "t/vnm/vatm/dev/ext/met/metar", "## Demo", "## Add a Bridge Rule"}) {
            assertTrue(readme.contains(needle), "README.md must mention: " + needle);
        }
    }
}
