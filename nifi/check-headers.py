#!/usr/bin/env python3
"""Check that the Bridge keeps every Pathfinder (SIPG) header and the payload, byte for byte.

Sends one message on each path (Outbound, Inbound, Dead letter) through the running Bridge
and compares what arrives with what was sent. It works through the NiFi REST API only:
it builds a temporary process group inside "Solace RabbitMQ Bridge" that uses the Bridge's
own Parameter Context, so it needs no broker password. It removes everything it created.

    python3 nifi/check-headers.py        (asks for the NiFi password, or reads NIFI_PASSWORD)

Optional: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (default admin),
RABBITMQ_MANAGEMENT_PORT (default 15672). Exit code 0 means every check passed.
"""
import base64, getpass, hashlib, json, os, ssl, sys, time, urllib.parse, urllib.request

NIFI = os.environ.get("NIFI_URL", "https://localhost:8443") + "/nifi-api"
MGMT_PORT = os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672")
BRIDGE_GROUP = "Solace RabbitMQ Bridge"
OUT_TOPIC = "t/vnm/vatm/dev/atfm/v1/fpl"                      # matched by Bridge Rule "out atfm"
OUT_EXCHANGE = "x.swim.dev.bridge.out"
IN_EXCHANGE = "x.swim.dev.bridge.in"
IN_KEY = "ext/fixm/fpl"                                        # Bridge Rule "in ext"
DLQ_KEY, DLQ = "foo/fixm", "q/vnm/vatm/dev/bridge/in-dlq"      # matches no Bridge Rule
CHECK_QUEUE = "q/vnm/vatm/dev/bridgetest/check-headers"       # created and deleted by this script

# docs/Documents/Pathfinder_Headers_Metadata_updated 24 Sep 2026.xlsx, sheet "Headers for SIPG Test"
HEADERS = {
    "APAC_SOURCE": "VV_VATM", "APAC_RECIPIENT_LIST": "WS_CAAS,RJ_JCAB", "APAC_CATEGORY": "FIXM",
    "APAC_CATEGORY_VERSION": "FIXM_4_3_FF_ICE", "APAC_MESSAGE_TYPE": "FILED_FLIGHT_PLAN",
    "DEP_AIRPORT": "VVTS", "ARR_AIRPORT": "WSSS", "AIRLINE": "HVN", "ACID": "HVN651",
    "GUFI": "ec5c6d8e-6b3b-4b6c-a3a6-2c1f6f3d9a10", "GUFI_NAMESPACE_IDENTIFIER": "FF-ICE",
    "EOBT": "2026-10-01T06:00:00Z", "FFICE_PHASE": "FILED",
    "APAC_TIMESTAMP": "VV_EEMS_OUT:1790873508104, WS_GEMS_IN: 1790873508200",
}
# ~19 KB of UTF-8 XML with Vietnamese, arrows, entities, quotes and tabs.
PAYLOAD = ('<?xml version="1.0" encoding="UTF-8"?>\n<fx:Flight xmlns:fx="http://www.fixm.aero/flight/4.3">\n'
           + "".join(f'  <fx:routePoint seq="{i}">Điểm {i}: Tân Sơn Nhất → Changi &amp; &lt;FL350&gt; "quoted" \'single\'\t(tab)</fx:routePoint>\n'
                     for i in range(150))
           + "</fx:Flight>\n").encode()

TLS = ssl._create_unverified_context()  # NiFi's certificate is self-signed
TOKEN = None
V = {"version": 0}


def api(method, path, body=None, raw=False):
    req = urllib.request.Request(NIFI + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=TLS) as r:
            out = r.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()}") from None
    return out if raw else (json.loads(out) if out else None)


def login():
    global TOKEN
    password = os.environ.get("NIFI_PASSWORD") or getpass.getpass("NiFi password: ")
    form = urllib.parse.urlencode({"username": os.environ.get("NIFI_USERNAME", "admin"), "password": password}).encode()
    with urllib.request.urlopen(urllib.request.Request(NIFI + "/access/token", data=form), context=TLS) as r:
        TOKEN = r.read().decode()


def find_group(parent="root"):
    for g in api("GET", f"/process-groups/{parent}/process-groups")["processGroups"]:
        if g["component"]["name"] == BRIDGE_GROUP:
            return g
        found = find_group(g["id"])
        if found:
            return found
    return None


def wait(cond, seconds=20):
    for _ in range(seconds * 2):
        result = cond()
        if result:
            return result
        time.sleep(0.5)
    return cond()


class TestGroup:
    """A temporary process group inside the Bridge group, with helpers to build and read it."""

    def __init__(self, bridge):
        ctx = bridge["component"]["parameterContext"]["id"]
        self.id = api("POST", f"/process-groups/{bridge['id']}/process-groups", {"revision": V, "component": {
            "name": "check-headers (temporary)", "position": {"x": 1600, "y": 0}, "parameterContext": {"id": ctx}}})["id"]
        self.vhost = next(p["parameter"]["value"] for p in api("GET", f"/parameter-contexts/{ctx}")["component"]["parameters"]
                          if p["parameter"]["name"] == "rabbitmq.vhost")
        self.jms = next(s["id"] for s in api("GET", f"/flow/process-groups/{bridge['id']}/controller-services")["controllerServices"]
                        if s["component"]["type"].endswith("JndiJmsConnectionFactoryProvider"))

    def processor(self, kind, name, x, y, props, auto=(), period="0 sec"):
        return api("POST", f"/process-groups/{self.id}/processors", {"revision": V, "component": {
            "type": kind, "name": name, "position": {"x": x, "y": y},
            "config": {"properties": props, "autoTerminatedRelationships": list(auto), "schedulingPeriod": period}}})["id"]

    def funnel(self, x, y):
        return api("POST", f"/process-groups/{self.id}/funnels", {"revision": V, "component": {"position": {"x": x, "y": y}}})["id"]

    def connect(self, source, dest, rels, dest_type="PROCESSOR"):
        return api("POST", f"/process-groups/{self.id}/connections", {"revision": V, "component": {
            "source": {"id": source, "groupId": self.id, "type": "PROCESSOR"},
            "destination": {"id": dest, "groupId": self.id, "type": dest_type}, "selectedRelationships": rels}})["id"]

    def state(self, pid, state):
        p = api("GET", f"/processors/{pid}")
        api("PUT", f"/processors/{pid}/run-status", {"revision": p["revision"], "state": state})

    def set_props(self, pid, props):
        p = api("GET", f"/processors/{pid}")
        api("PUT", f"/processors/{pid}", {"revision": p["revision"], "component": {"id": pid, "config": {"properties": props}}})

    def flowfiles(self, cid):
        """[(attributes, content bytes)] queued in a connection."""
        r = api("POST", f"/flowfile-queues/{cid}/listing-requests")["listingRequest"]
        while not r["finished"]:
            time.sleep(0.3)
            r = api("GET", f"/flowfile-queues/{cid}/listing-requests/{r['id']}")["listingRequest"]
        api("DELETE", f"/flowfile-queues/{cid}/listing-requests/{r['id']}")
        q = f"/flowfile-queues/{cid}/flowfiles/"
        return [(api("GET", q + f["uuid"])["flowFile"]["attributes"], api("GET", q + f["uuid"] + "/content", raw=True) if f["size"] else b"")
                for f in r.get("flowFileSummaries", [])]

    def empty(self, cid):
        r = api("POST", f"/flowfile-queues/{cid}/drop-requests")["dropRequest"]
        while not r["finished"]:
            time.sleep(0.3)
            r = api("GET", f"/flowfile-queues/{cid}/drop-requests/{r['id']}")["dropRequest"]
        api("DELETE", f"/flowfile-queues/{cid}/drop-requests/{r['id']}")

    def remove(self):
        api("PUT", f"/flow/process-groups/{self.id}", {"id": self.id, "state": "STOPPED"})
        time.sleep(3)
        for c in api("GET", f"/process-groups/{self.id}/connections")["connections"]:
            self.empty(c["id"])
        g = api("GET", f"/process-groups/{self.id}")
        api("DELETE", f"/process-groups/{self.id}?version={g['revision']['version']}")


def check(label, headers, body):
    print(f"\n{label}")
    failures = 0
    for name, sent in HEADERS.items():
        got = headers.get(name)
        failures += got != sent
        print(f"  {'OK  ' if got == sent else 'FAIL'} {name:27} {got!r}" + ("" if got == sent else f"  (sent {sent!r})"))
    same = body == PAYLOAD
    failures += not same
    print(f"  {'OK  ' if same else 'FAIL'} payload: {len(body)} bytes, sha256 {hashlib.sha256(body).hexdigest()[:16]}"
          f" (sent {len(PAYLOAD)} bytes, sha256 {hashlib.sha256(PAYLOAD).hexdigest()[:16]})")
    return failures


def main():
    login()
    bridge = find_group()
    if not bridge:
        sys.exit(f'No process group "{BRIDGE_GROUP}" in NiFi. Import nifi/bridge-flow.json first.')
    t = TestGroup(bridge)
    sol = {"Connection Factory Service": t.jms, "User Name": "#{solace.username}", "Password": "#{solace.password}"}
    rmq = {"Host Name": "#{rabbitmq.host}", "Port": "#{rabbitmq.port}", "Virtual Host": "#{rabbitmq.vhost}",
           "Username": "#{rabbitmq.username}", "Password": "#{rabbitmq.password}"}
    gen, text, names = "org.apache.nifi.processors.standard.GenerateFlowFile", PAYLOAD.decode(), "|".join(HEADERS)
    amqp = {"amqp$contentType": "application/xml", "amqp$correlationId": "check-headers"}
    vh, enc = urllib.parse.quote(t.vhost, safe=""), lambda name: urllib.parse.quote(name, safe="")
    created_queue = False
    try:
        g_out = t.processor(gen, "make outbound message", 0, 0, {**HEADERS, "Custom Text": text, "contentType": "application/xml"}, period="1 day")
        p_out = t.processor("org.apache.nifi.jms.processors.PublishJMS", f"publish {OUT_TOPIC}", 0, 200, {**sol,
            "Destination Name": OUT_TOPIC, "Destination Type": "TOPIC", "Message Body Type": "text",
            "Attributes to Send as JMS Headers": names + "|contentType"}, auto=["success"])
        g_in = t.processor(gen, "make inbound message", 500, 0, {**HEADERS, **amqp, "Custom Text": text, "rk": IN_KEY, "amqp$messageId": "check-headers-in"}, period="1 day")
        g_dlq = t.processor(gen, "make dead letter message", 500, 200, {**HEADERS, **amqp, "Custom Text": text, "rk": DLQ_KEY, "amqp$messageId": "check-headers-dlq"}, period="1 day")
        p_in = t.processor("org.apache.nifi.amqp.processors.PublishAMQP", f"publish {IN_EXCHANGE}", 500, 400, {**rmq,
            "Exchange Name": IN_EXCHANGE, "Routing Key": "${rk}", "Delivery Guarantee": "AT_LEAST_ONCE",
            "Headers Source": "FLOWFILE_ATTRIBUTES", "Headers Pattern": names}, auto=["success"])
        sub = t.processor("org.apache.nifi.jms.processors.ConsumeJMS", "subscribe t/vnm/vatm/dev/ext/>", 1000, 0, {**sol,
            "Destination Name": "t/vnm/vatm/dev/ext/>", "Destination Type": "TOPIC"})
        req = t.processor(gen, "management request", 1500, 0, {"Custom Text": "{}", "m": "GET", "u": "overview"}, period="1 day")
        http = t.processor("org.apache.nifi.processors.standard.InvokeHTTP", "RabbitMQ management API", 1500, 200, {
            "HTTP Method": "${m}", "HTTP URL": f"http://#{{rabbitmq.host}}:{MGMT_PORT}/api/${{u}}",
            "Request Username": "#{rabbitmq.username}", "Request Password": "#{rabbitmq.password}",
            "Request Content-Type": "application/json"}, auto=["Original"])
        f_fail, f_sol, f_http = t.funnel(300, 700), t.funnel(1000, 400), t.funnel(1500, 400)
        t.connect(g_out, p_out, ["success"]); t.connect(g_in, p_in, ["success"]); t.connect(g_dlq, p_in, ["success"])
        c_fail = [t.connect(p_out, f_fail, ["failure"], "FUNNEL"), t.connect(p_in, f_fail, ["failure"], "FUNNEL")]
        c_sol = t.connect(sub, f_sol, ["success"], "FUNNEL")
        t.connect(req, http, ["success"])
        c_http = t.connect(http, f_http, ["Response", "No Retry", "Retry", "Failure"], "FUNNEL")

        def rabbit(method, path, body=None):
            t.set_props(req, {"m": method, "u": path, "Custom Text": json.dumps(body or {})})
            t.state(req, "RUN_ONCE")
            got = wait(lambda: t.flowfiles(c_http))
            t.empty(c_http)
            if not got:
                raise RuntimeError(f"no answer from the RabbitMQ management API for {method} {path}")
            attrs, content = got[0]
            return attrs.get("invokehttp.status.code"), content
        t.rabbit = rabbit

        for pid in (http, sub, p_out, p_in):
            t.state(pid, "RUNNING")
        code, _ = rabbit("PUT", f"queues/{vh}/{enc(CHECK_QUEUE)}", {"durable": False, "auto_delete": False, "arguments": {}})
        if code != "201":
            raise RuntimeError(f"queue {CHECK_QUEUE} already exists or could not be created (HTTP {code}); not touching it")
        created_queue = True
        rabbit("POST", f"bindings/{vh}/e/{enc(OUT_EXCHANGE)}/q/{enc(CHECK_QUEUE)}", {"routing_key": OUT_TOPIC, "arguments": {}})
        time.sleep(5)  # let the Solace subscription settle
        for pid in (g_out, g_in, g_dlq):
            t.state(pid, "RUN_ONCE")
        time.sleep(12)

        failures = sum(len(t.flowfiles(c)) for c in c_fail)
        if failures:
            print(f"FAIL: {failures} message(s) could not be sent to the brokers")
        _, body = rabbit("POST", f"queues/{vh}/{enc(CHECK_QUEUE)}/get", {"count": 5, "ackmode": "ack_requeue_false", "encoding": "base64"})
        outbound = json.loads(body)
        for m in outbound:
            failures += check(f"OUTBOUND  Solace {OUT_TOPIC} -> RabbitMQ {OUT_EXCHANGE} (routing key {m['routing_key']})",
                              m["properties"].get("headers", {}), base64.b64decode(m["payload"]))
        inbound = t.flowfiles(c_sol)
        for attrs, content in inbound:
            failures += check(f"INBOUND  RabbitMQ {IN_KEY} -> Solace {attrs.get('jms_destination')} ({attrs.get('jms.messagetype')})", attrs, content)
        _, body = rabbit("POST", f"queues/{vh}/{enc(DLQ)}/get", {"count": 100, "ackmode": "ack_requeue_true", "encoding": "base64"})
        in_dlq = json.loads(body)
        dead = [m for m in in_dlq if m["properties"].get("message_id") == "check-headers-dlq"]
        for m in dead:
            failures += check(f"DEAD LETTER  RabbitMQ {DLQ_KEY} -> {DLQ}", m["properties"].get("headers", {}), base64.b64decode(m["payload"]))
        if len(in_dlq) == 1 and dead:
            rabbit("POST", f"queues/{vh}/{enc(DLQ)}/get", {"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"})
        elif dead:
            print(f"\nNote: {DLQ} holds other messages too, so the test message was left there.")
        arrived = [len(outbound), len(inbound), len(dead)]
        failures += sum(n != 1 for n in arrived)
        print(f"\nMessages arrived (outbound, inbound, dead letter): {arrived}, expected [1, 1, 1]")
        print("PASS" if not failures else f"FAIL ({failures} problem(s))")
        return 0 if not failures else 1
    finally:
        if created_queue:
            t.rabbit("DELETE", f"queues/{vh}/{enc(CHECK_QUEUE)}")
        t.remove()


if __name__ == "__main__":
    sys.exit(main())
