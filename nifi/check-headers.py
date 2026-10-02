#!/usr/bin/env python3
"""Check that the Bridge routes by topic and by header, and keeps every Pathfinder header and the payload.

Sends one message per scenario through the running Bridge, Solace to RabbitMQ and RabbitMQ to Solace,
and checks where each copy arrives, its routing key or topic, every header, and the payload byte for byte.
It works through the NiFi REST API only: it builds a temporary process group inside "Solace RabbitMQ Bridge"
that uses the Bridge's own Parameter Context, so it needs no broker password. It removes everything it created,
and takes its messages out of the test queues when they hold nothing else.

    python3 nifi/check-headers.py        (asks for the NiFi password, or reads NIFI_PASSWORD)

Optional: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (default admin),
RABBITMQ_MANAGEMENT_PORT (default 15672). Exit code 0 means every check passed.
"""
import base64, getpass, hashlib, json, os, re, ssl, sys, time, urllib.parse, urllib.request

NIFI = os.environ.get("NIFI_URL", "https://localhost:8443") + "/nifi-api"
MGMT_PORT = os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672")
BRIDGE_GROUP = "Solace RabbitMQ Bridge"
INGRESS = "x/vnm/vatm/dev/ingress"
VNA, GEMS = "q/vnm/vna/dev/swim/flight", "q/vnm/vatm/dev/eems/to-gems"
DLQ, UNROUTED = "q/vnm/vatm/dev/eems/dlq", "q/vnm/vatm/dev/eems/unrouted"
SOLACE_TOPIC = "t/vnm/acv/dev/>"            # topic routing into Solace
SOLACE_HEADER = "tr/*/*/vnm/vatm/dev/>"     # header routing into Solace: selector on APAC_RECIPIENT_LIST
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

# name: (sent from, topic or routing key, header changes, {where it must arrive: [(routing key or topic, VV_ROUTE)]})
SCENARIOS = {
    "out-topic": ("solace", "t/vnm/vatm/dev/atfm/v1/fpl", {"APAC_RECIPIENT_LIST": "VV_HVN"},
                  {VNA: [("t.vnm.vatm.dev.atfm.v1.fpl", None)]}),
    "out-header": ("solace", "tr/vnm/vatm/vnm/vna/dev/fpms/v1/filing/reply", {"APAC_RECIPIENT_LIST": "VV_VATM,VV_HVN,WS_CAAS"},
                   {VNA: [("tr.vnm.vatm.vnm.vna.dev.fpms.v1.filing.reply", "VV_HVN")],          # no copy back to VV_VATM
                    GEMS: [("tr.vnm.vatm.vnm.vna.dev.fpms.v1.filing.reply", "GEMS")]}),
    "in-topic": ("rabbitmq", "t.vnm.acv.dev.aodb.v1.departure.publish.vvts", {"APAC_SOURCE": "VV_ACV", "APAC_RECIPIENT_LIST": "VV_VATM"},
                 {SOLACE_TOPIC: [("t/vnm/acv/dev/aodb/v1/departure/publish/vvts", None)]}),
    "in-header": ("rabbitmq", "tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request", {"APAC_SOURCE": "VV_HVN", "APAC_RECIPIENT_LIST": "VV_VATM,WS_CAAS"},
                  {SOLACE_HEADER: [("tr/vnm/vna/vnm/vatm/dev/swim/v1/filing/request", "VV_VATM")],
                   GEMS: [("tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request", "GEMS")]}),
    "unknown-recipient": ("rabbitmq", "tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request", {"APAC_SOURCE": "VV_HVN", "APAC_RECIPIENT_LIST": "VV_XYZ"},
                          {DLQ: [("tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request", None)]}),
    "sender-mismatch": ("rabbitmq", "tr.vnm.vatm.vnm.vna.dev.fpms.v1.filing.reply", {"APAC_SOURCE": "VV_ACV", "APAC_RECIPIENT_LIST": "VV_VATM"},
                        {DLQ: [("tr.vnm.vatm.vnm.vna.dev.fpms.v1.filing.reply", None)]}),   # would loop VATM -> VATM
    "no-header-binding": ("rabbitmq", "tr.vnm.vatm.vnm.vna.dev.aim.v1.notam.reply", {"APAC_RECIPIENT_LIST": "VV_HVN", "APAC_CATEGORY": "JSON"},
                          {UNROUTED: [("tr.vnm.vatm.vnm.vna.dev.aim.v1.notam.reply", "VV_HVN")]}),
}
DLQ_REASON = {"unknown-recipient": "unknown recipient VV_XYZ", "sender-mismatch": "APAC_SOURCE VV_ACV does not match"}
GEMS_TOPIC_MODE = {   # sent with router.gems.mode = topic: one copy per foreign recipient
    "gems-topic-mode": ("rabbitmq", "tr.vnm.vna.sgp.caas.dev.swim.v1.filing.request", {"APAC_SOURCE": "VV_HVN", "APAC_RECIPIENT_LIST": "WS_CAAS,VT_AEROTHAI"},
                        {GEMS: [("tr.vnm.vna.sgp.caas.dev.swim.v1.filing.request", "GEMS"),
                                ("tr.vnm.vna.tha.aerothai.dev.swim.v1.filing.request", "GEMS")]}),
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



def set_parameter(ctx, name, value):
    c = api("GET", f"/parameter-contexts/{ctx}")
    r = api("POST", f"/parameter-contexts/{ctx}/update-requests", {"revision": c["revision"], "id": ctx, "component": {
        "id": ctx, "parameters": [{"parameter": {"name": name, "value": value}}]}})["request"]
    while not r["complete"]:
        time.sleep(0.5)
        r = api("GET", f"/parameter-contexts/{ctx}/update-requests/{r['requestId']}")["request"]
    api("DELETE", f"/parameter-contexts/{ctx}/update-requests/{r['requestId']}")
    if r.get("failureReason"):
        raise RuntimeError(f"could not set parameter {name}: {r['failureReason']}")


def check(label, headers, body, changes, out_stamp):
    print(f"\n{label}")
    failures = 0
    expected = {**HEADERS, **changes}
    stamp = re.escape(expected.pop("APAC_TIMESTAMP")) + r",VV_EEMS_IN:\d+" + (r",VV_EEMS_OUT:\d+" if out_stamp else "") + "$"
    for name, want in expected.items():
        got = headers.get(name)
        failures += got != want
        print(f"  {'OK  ' if got == want else 'FAIL'} {name:27} {got!r}" + ("" if got == want else f"  (expected {want!r})"))
    got = headers.get("APAC_TIMESTAMP", "")
    good = bool(re.match(stamp, got))
    failures += not good
    print(f"  {'OK  ' if good else 'FAIL'} {'APAC_TIMESTAMP':27} {got!r}" + ("" if good else f"  (expected {stamp})"))
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
    vh, enc = urllib.parse.quote(t.vhost, safe=""), lambda name: urllib.parse.quote(name, safe="")
    mode_changed, drain = False, None
    try:
        p_sol = t.processor("org.apache.nifi.jms.processors.PublishJMS", "publish to Solace", 0, 300, {**sol,
            "Destination Name": "${topic}", "Destination Type": "TOPIC", "Message Body Type": "text",
            "Attributes to Send as JMS Headers": names + "|contentType|jms_correlationId"}, auto=["success"])
        p_rmq = t.processor("org.apache.nifi.amqp.processors.PublishAMQP", f"publish to {INGRESS}", 500, 300, {**rmq,
            "Exchange Name": INGRESS, "Routing Key": "${rk}", "Delivery Guarantee": "AT_LEAST_ONCE",
            "Headers Source": "FLOWFILE_ATTRIBUTES", "Headers Pattern": names}, auto=["success"])
        senders = {}
        for i, (name, (side, address, changes, _)) in enumerate({**SCENARIOS, **GEMS_TOPIC_MODE}.items()):
            corr = f"{RUN}-{name}"
            props = {**HEADERS, **changes, "Custom Text": text}
            props.update({"topic": address, "contentType": "application/xml", "jms_correlationId": corr} if side == "solace" else
                         {"rk": address, "amqp$contentType": "application/xml", "amqp$correlationId": corr, "amqp$messageId": corr})
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
            """[(scenario, where, routing key or topic, headers, payload)] for this run's messages."""
            out = []
            for queue in (VNA, GEMS, DLQ, UNROUTED):
                for m in peek(queue):
                    corr = m["properties"].get("correlation_id", "")
                    if corr.startswith(RUN + "-"):
                        out.append((corr[len(RUN) + 1:], queue, m["routing_key"], m["properties"].get("headers", {}), base64.b64decode(m["payload"])))
            for where, c in c_sol.items():
                for attrs, content in t.flowfiles(c):
                    corr = attrs.get("jms_correlationId", "")
                    if corr.startswith(RUN + "-"):
                        out.append((corr[len(RUN) + 1:], where, attrs.get("jms_destination"), attrs, content))
            return out

        def send_and_check(scenarios):
            for name in scenarios:
                t.state(senders[name], "RUN_ONCE")
            want = sum(len(v) for _, _, _, dest in scenarios.values() for v in dest.values())
            wait(lambda: len([a for a in arrivals() if a[0] in scenarios]) >= want, 60)
            time.sleep(5)  # a copy that should not exist has time to show up
            got = [a for a in arrivals() if a[0] in scenarios]
            failures = 0
            for name, (side, address, changes, dest) in scenarios.items():
                expected = sorted((w, key, route) for w, copies in dest.items() for key, route in copies)
                mine = sorted((a for a in got if a[0] == name), key=lambda a: (a[1], a[2]))
                actual = sorted((a[1], a[2], a[3].get("VV_ROUTE")) for a in mine)
                ok = actual == expected
                failures += not ok
                print(f"\n== {name}: {side} {address}, APAC_RECIPIENT_LIST={changes.get('APAC_RECIPIENT_LIST', HEADERS['APAC_RECIPIENT_LIST'])}")
                print(f"  {'OK  ' if ok else 'FAIL'} arrived at {[(w, k) for w, k, _ in actual]}" + ("" if ok else f"\n       expected {[(w, k) for w, k, _ in expected]}"))
                for _, where, key, headers, body in mine:
                    failures += check(f"  {where}  ({key}, VV_ROUTE={headers.get('VV_ROUTE')})", headers, body, changes, where != DLQ)
                    if where == DLQ:
                        reason = headers.get("VV_DLX_REASON", "")
                        good = DLQ_REASON[name] in reason
                        failures += not good
                        print(f"  {'OK  ' if good else 'FAIL'} {'VV_DLX_REASON':27} {reason!r}")
            return failures

        def drain():
            for queue in (VNA, GEMS, DLQ, UNROUTED):
                held = peek(queue)
                if held and all(m["properties"].get("correlation_id", "").startswith(RUN + "-") for m in held):
                    rabbit("POST", f"queues/{vh}/{enc(queue)}/get", {"count": len(held), "ackmode": "ack_requeue_false", "encoding": "auto"})
                elif any(m["properties"].get("correlation_id", "").startswith(RUN + "-") for m in held):
                    print(f"\nNote: {queue} holds other messages too, so this run's messages were left there.")

        for pid in (http, s_topic, s_header, p_sol, p_rmq):
            t.state(pid, "RUNNING")
        time.sleep(5)  # let the Solace subscriptions settle
        failures = send_and_check(SCENARIOS)
        mode_changed = True
        set_parameter(t.ctx, "router.gems.mode", "topic")
        failures += send_and_check(GEMS_TOPIC_MODE)
        failures += sum(len(t.flowfiles(c)) for c in c_fail)
        print("\nPASS" if not failures else f"\nFAIL ({failures} problem(s))")
        return 0 if not failures else 1
    finally:
        try:
            if drain:
                drain()
        finally:
            try:
                if mode_changed:
                    set_parameter(t.ctx, "router.gems.mode", "header")
            finally:
                t.remove()


if __name__ == "__main__":
    sys.exit(main())
