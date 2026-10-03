# 1. Setup for the manual Bridge tests. Load it into your shell, on either machine:
#
#     source tests/env.sh
#
# It sets where the brokers are and which accounts to use. To change one, export it before sourcing, for example
# `export SOLACE_HOST=tcp://solace.tailfac6af.ts.net:55555` on the RabbitMQ machine. It asks for the passwords that
# are not set yet, downloads the jars if they are missing, and checks it can log in to both brokers.
# The passwords stay in this shell only; nothing is written to disk.

_bridge_test_env() {
    local root
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

    export SOLACE_HOST="${SOLACE_HOST:-tcp://localhost:55555}"
    export SOLACE_VPN="${SOLACE_VPN:-default}"
    export SOLACE_USERNAME="${SOLACE_USERNAME:-hieu}"
    export RABBITMQ_HOST="${RABBITMQ_HOST:-192.168.121.61}"
    export RABBITMQ_MANAGEMENT_PORT="${RABBITMQ_MANAGEMENT_PORT:-15672}"
    export RABBITMQ_VHOST="${RABBITMQ_VHOST:-swim_sg}"
    export RABBITMQ_USERNAME="${RABBITMQ_USERNAME:-hieu}"

    if [ -z "${SOLACE_PASSWORD:-}" ]; then
        read -rsp "Solace password for $SOLACE_USERNAME: " SOLACE_PASSWORD; echo; export SOLACE_PASSWORD
    fi
    if [ -z "${RABBITMQ_PASSWORD:-}" ]; then
        read -rsp "RabbitMQ password for $RABBITMQ_USERNAME: " RABBITMQ_PASSWORD; echo; export RABBITMQ_PASSWORD
    fi

    local version major
    version="$(java -version 2>&1 | awk -F'"' '/version/ {print $2}')"
    major="${version%%.*}"
    if [ -z "$version" ] || [ "$major" = 1 ] || [ "$major" -lt 11 ]; then
        echo "FAIL java: need Java 11 or newer, found '${version:-none}'"; return 1
    fi
    echo "OK   java $version"

    # The Solace jars (the same ones NiFi uses) and the JMS API, which NiFi brings itself and so is not in lib/.
    local list dir url
    for list in "nifi/solace-jars.txt:lib" "tests/jars.txt:tests/lib"; do
        dir="$root/${list#*:}"
        mkdir -p "$dir"
        while read -r url; do
            # Via a .part file, so a broken download is never taken for a jar.
            [ -f "$dir/${url##*/}" ] || { curl -fsSL -o "$dir/${url##*/}.part" "$url" && command mv -f "$dir/${url##*/}.part" "$dir/${url##*/}"; } \
                || { echo "FAIL download $url"; return 1; }
        done < "$root/${list%%:*}"
    done
    echo "OK   jars in lib/ and tests/lib/"

    java -cp "$root/lib/*:$root/tests/lib/*" "$root/tests/BridgeTest.java" ping || {
        unset SOLACE_PASSWORD RABBITMQ_PASSWORD
        echo "Passwords cleared. Fix the setting above, then run \`source tests/env.sh\` again."
        return 1
    }
}
_bridge_test_env
unset -f _bridge_test_env
