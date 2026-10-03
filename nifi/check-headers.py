#!/usr/bin/env python3
"""Check that the Bridge carries messages between Solace and RabbitMQ without changing them.

Sends one message per scenario through the running Bridge, Solace to RabbitMQ (B-04) and RabbitMQ to Solace (B-03),
and checks where it arrives, its routing key or topic, every header (APAC_TIMESTAMP included), message-id,
correlation-id, content-type, reply-to and the payload byte for byte. The only change allowed is that VV_ROUTE,
which exists only inside RabbitMQ, does not reach Solace.

RabbitMQ to Solace messages are put straight on x/vnm/vatm/dev/swim and x/vnm/vatm/dev/route, as the Router would.
Solace to RabbitMQ messages are read from q/vnm/vatm/dev/router/in, the queue behind x/vnm/vatm/dev/ingress, so no
Router may be reading that queue while this runs.

It works through the NiFi REST API only: it builds a temporary process group inside "Solace RabbitMQ Bridge"
that uses the Bridge's own Parameter Context, so it needs no broker password. It removes everything it created,
and takes its messages out of q/vnm/vatm/dev/router/in when that queue holds nothing else.

    python3 nifi/check-headers.py        (asks for the NiFi password, or reads NIFI_PASSWORD)

Optional: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (default admin),
RABBITMQ_MANAGEMENT_PORT (default 15672). Exit code 0 means every check passed.
"""
import base64, getpass, hashlib, json, os, ssl, sys, time, urllib.parse, urllib.request

NIFI = os.environ.get("NIFI_URL", "https://localhost:8443") + "/nifi-api"
MGMT_PORT = os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672")
BRIDGE_GROUP = "Solace RabbitMQ Bridge"
SWIM, ROUTE = "x/vnm/vatm/dev/swim", "x/vnm/vatm/dev/route"   # where the Router puts messages for bridge/inbound
ROUTER_IN = "q/vnm/vatm/dev/router/in"      # B-04 publishes into x/vnm/vatm/dev/ingress, which feeds this queue
SOLACE_TOPIC = "t/vnm/acv/dev/>"            # topic subscription on Solace
SOLACE_HEADER = "tr/*/*/vnm/vatm/dev/>"     # header subscription on Solace: selector on APAC_RECIPIENT_LIST
SELECTOR = " OR ".join(["APAC_RECIPIENT_LIST = 'VV_VATM'"] + [f"APAC_RECIPIENT_LIST LIKE '{p}' ESCAPE '\\'"
                                                                for p in (r"VV\_VATM,%", r"%,VV\_VATM", r"%,VV\_VATM,%")])
RUN = f"check-{int(time.time())}"

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
CONTENT_TYPE = "application/xml"
SOLACE_MESSAGE_ID = {"out-header"}  # these Solace senders set the property messageId; the others get Solace's JMS id

# name: (sent from, topic or routing key, header changes, reply-to or None, (where it must arrive, routing key or topic))
SCENARIOS = {
    "out-topic": ("solace", "t/vnm/vatm/dev/atfm/v1/fpl", {"APAC_RECIPIENT_LIST": "VV_HVN"}, None,
                  (ROUTER_IN, "t.vnm.vatm.dev.atfm.v1.fpl")),
    # PublishJMS, the sender here, only sets a reply-to whose name has "queue" or "topic" in it.
    "out-header": ("solace", "tr/vnm/vatm/vnm/vna/dev/fpms/v1/filing/reply", {"APAC_RECIPIENT_LIST": "VV_VATM,VV_HVN,WS_CAAS"},
                   "q/vnm/vatm/dev/fpms/queue/reply", (ROUTER_IN, "tr.vnm.vatm.vnm.vna.dev.fpms.v1.filing.reply")),
    # A space in the list: the Bridge must not trim it.
    "in-topic": ("rabbitmq", "t.vnm.acv.dev.aodb.v1.departure.publish.vvts", {"APAC_SOURCE": "VV_ACV", "APAC_RECIPIENT_LIST": "VV_VATM, WS_CAAS"},
                 None, (SOLACE_TOPIC, "t/vnm/acv/dev/aodb/v1/departure/publish/vvts")),
    # HVN sends to VATM and CAAS; the Router's copy for VATM carries VV_ROUTE=VV_VATM, Solace must get the full list.
    "in-header": ("rabbitmq", "tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request", {"APAC_SOURCE": "VV_HVN", "APAC_RECIPIENT_LIST": "VV_VATM,WS_CAAS"},
                  "q/vnm/vna/dev/swim/reply", (SOLACE_HEADER, "tr/vnm/vna/vnm/vatm/dev/swim/v1/filing/request")),
}

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
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = cond()
        if result:
            return result
        time.sleep(0.5)
    return cond()


class TestGroup:
    """A temporary process group inside the Bridge group, with helpers to build and read it."""

    def __init__(self, bridge):
        self.ctx = ctx = bridge["component"]["parameterContext"]["id"]
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


def check(label, headers, props, body, sent, only_sent):
    """Compare one arrival with what was sent. only_sent: no header may arrive that was not sent."""
    changes, corr, reply, message_id = sent
    print(f"\n{label}")
    failures = 0

    def report(ok, name, got, want):
        nonlocal failures
        failures += not ok
        print(f"  {'OK  ' if ok else 'FAIL'} {name:27} {got!r}" + ("" if ok else f"  (expected {want})"))

    expected = {**HEADERS, **changes}
    for name, want in expected.items():
        report(headers.get(name) == want, name, headers.get(name), repr(want))
    if only_sent:
        extra = sorted(set(headers) - set(expected))
        report(not extra, "no other header", extra, "none")
    else:
        report("VV_ROUTE" not in headers, "VV_ROUTE", headers.get("VV_ROUTE"), "absent")
    report(props["correlation_id"] == corr, "correlation-id", props["correlation_id"], repr(corr))
    report(props["content_type"] == CONTENT_TYPE, "content-type", props["content_type"], repr(CONTENT_TYPE))
    report(props["reply_to"] == reply, "reply-to", props["reply_to"], repr(reply) if reply else "absent")
    # A sent message-id must arrive as is; without one, Solace's own JMS message id is used.
    want_id = props["message_id"] == message_id if message_id else bool(props["message_id"])
    report(want_id, "message-id", props["message_id"], repr(message_id) if message_id else "set")
    report(body == PAYLOAD, "payload", f"{len(body)} bytes, sha256 {hashlib.sha256(body).hexdigest()[:16]}",
           f"{len(PAYLOAD)} bytes, sha256 {hashlib.sha256(PAYLOAD).hexdigest()[:16]}")
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
    vh, enc = urllib.parse.quote(t.vhost, safe=""), lambda name: urllib.parse.quote(name, safe="")
    drain = None
    try:
        p_sol = t.processor("org.apache.nifi.jms.processors.PublishJMS", "publish to Solace", 0, 300, {**sol,
            "Destination Name": "${topic}", "Destination Type": "TOPIC", "Message Body Type": "text",
            "Attributes to Send as JMS Headers": names + "|contentType|messageId|jms_correlationId|jms_replyTo"}, auto=["success"])
        p_rmq = t.processor("org.apache.nifi.amqp.processors.PublishAMQP", "publish to ${exchange} as the Router would", 500, 300, {**rmq,
            "Exchange Name": "${exchange}", "Routing Key": "${rk}", "Delivery Guarantee": "AT_LEAST_ONCE",
            "Headers Source": "FLOWFILE_ATTRIBUTES", "Headers Pattern": names + "|VV_ROUTE"}, auto=["success"])
        senders = {}
        for i, (name, (side, address, changes, reply, _)) in enumerate(SCENARIOS.items()):
            corr = f"{RUN}-{name}"
            props = {**HEADERS, **changes, "Custom Text": text}
            if side == "solace":
                props.update({"topic": address, "contentType": CONTENT_TYPE, "jms_correlationId": corr})
                props.update({"jms_replyTo": reply} if reply else {})
                props.update({"messageId": corr} if name in SOLACE_MESSAGE_ID else {})
            else:
                topic_routing = address.startswith("t.")
                props.update({"exchange": SWIM if topic_routing else ROUTE, "rk": address, "amqp$contentType": CONTENT_TYPE,
                              "amqp$correlationId": corr, "amqp$messageId": corr})
                props.update({} if topic_routing else {"VV_ROUTE": "VV_VATM"})
                props.update({"amqp$replyTo": reply} if reply else {})
            senders[name] = t.processor(gen, f"make {name}", i * 250, 0, props, period="1 day")
            t.connect(senders[name], p_sol if side == "solace" else p_rmq, ["success"])
        s_topic = t.processor("org.apache.nifi.jms.processors.ConsumeJMS", f"subscribe {SOLACE_TOPIC}", 1000, 300, {**sol,
            "Destination Name": SOLACE_TOPIC, "Destination Type": "TOPIC"})
        s_header = t.processor("org.apache.nifi.jms.processors.ConsumeJMS", f"subscribe {SOLACE_HEADER} with selector", 1500, 300, {**sol,
            "Destination Name": SOLACE_HEADER, "Destination Type": "TOPIC", "Message Selector": SELECTOR})
        req = t.processor(gen, "management request", 2000, 300, {"Custom Text": "{}", "m": "GET", "u": "overview"}, period="1 day")
        http = t.processor("org.apache.nifi.processors.standard.InvokeHTTP", "RabbitMQ management API", 2000, 500, {
            "HTTP Method": "${m}", "HTTP URL": f"http://#{{rabbitmq.host}}:{MGMT_PORT}/api/${{u}}",
            "Request Username": "#{rabbitmq.username}", "Request Password": "#{rabbitmq.password}",
            "Request Content-Type": "application/json"}, auto=["Original"])
        f_fail, f_sol, f_http = t.funnel(300, 700), t.funnel(1250, 700), t.funnel(2000, 700)
        c_fail = [t.connect(p_sol, f_fail, ["failure"], "FUNNEL"), t.connect(p_rmq, f_fail, ["failure"], "FUNNEL")]
        c_sol = {SOLACE_TOPIC: t.connect(s_topic, f_sol, ["success"], "FUNNEL"), SOLACE_HEADER: t.connect(s_header, f_sol, ["success"], "FUNNEL")}
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

        def peek(queue):
            code, body = rabbit("POST", f"queues/{vh}/{enc(queue)}/get", {"count": 500, "ackmode": "ack_requeue_true", "encoding": "base64"})
            if code != "200":
                raise RuntimeError(f"cannot read queue {queue} (HTTP {code}); create the test objects from README step 1")
            return json.loads(body)

        def arrivals():
            """[(scenario, where, routing key or topic, headers, properties, payload)] for this run's messages."""
            out = []
            for m in peek(ROUTER_IN):
                p = m["properties"]
                corr = p.get("correlation_id", "")
                if corr.startswith(RUN + "-"):
                    props = {k: p.get(k) for k in ("message_id", "correlation_id", "content_type", "reply_to")}
                    out.append((corr[len(RUN) + 1:], ROUTER_IN, m["routing_key"], p.get("headers", {}), props, base64.b64decode(m["payload"])))
            for where, c in c_sol.items():
                for attrs, content in t.flowfiles(c):
                    corr = attrs.get("jms_correlationId", "")
                    if corr.startswith(RUN + "-"):
                        props = {"message_id": attrs.get("messageId"), "correlation_id": corr,
                                 "content_type": attrs.get("contentType"), "reply_to": attrs.get("jms_replyTo")}
                        out.append((corr[len(RUN) + 1:], where, attrs.get("jms_destination"), attrs, props, content))
            return out

        def drain():
            held = peek(ROUTER_IN)
            if held and all(m["properties"].get("correlation_id", "").startswith(RUN + "-") for m in held):
                rabbit("POST", f"queues/{vh}/{enc(ROUTER_IN)}/get", {"count": len(held), "ackmode": "ack_requeue_false", "encoding": "auto"})
            elif any(m["properties"].get("correlation_id", "").startswith(RUN + "-") for m in held):
                print(f"\nNote: {ROUTER_IN} holds other messages too, so this run's messages were left there.")

        for pid in (http, s_topic, s_header, p_sol, p_rmq):
            t.state(pid, "RUNNING")
        time.sleep(5)  # let the Solace subscriptions settle
        for name in SCENARIOS:
            t.state(senders[name], "RUN_ONCE")
        wait(lambda: {a[0] for a in arrivals()} >= set(SCENARIOS), 60)
        time.sleep(5)  # a copy that should not exist has time to show up
        got = arrivals()
        failures = 0
        for name, (side, address, changes, reply, (where, key)) in SCENARIOS.items():
            mine = [a for a in got if a[0] == name]
            actual = sorted((a[1], a[2]) for a in mine)
            ok = actual == [(where, key)]
            failures += not ok
            print(f"\n== {name}: {side} {address}, APAC_RECIPIENT_LIST={changes.get('APAC_RECIPIENT_LIST', HEADERS['APAC_RECIPIENT_LIST'])!r}")
            print(f"  {'OK  ' if ok else 'FAIL'} arrived at {actual}" + ("" if ok else f"\n       expected {[(where, key)]}"))
            for _, at, k, headers, props, body in mine:
                corr = f"{RUN}-{name}"
                message_id = None if side == "solace" and name not in SOLACE_MESSAGE_ID else corr
                failures += check(f"  {at}  ({k})", headers, props, body, (changes, corr, reply, message_id), side == "solace")
        failures += sum(len(t.flowfiles(c)) for c in c_fail)
        print("\nPASS" if not failures else f"\nFAIL ({failures} problem(s))")
        return 0 if not failures else 1
    finally:
        try:
            if drain:
                drain()
        finally:
            t.remove()


if __name__ == "__main__":
    sys.exit(main())
