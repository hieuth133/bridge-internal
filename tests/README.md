# Manual Bridge tests

These scripts let a tester check, by hand, that the Bridge carries a message from one broker to the other **without changing it**. Each script sends one message and checks it on the other side.

The only change the Bridge may make is to remove the header `VV_ROUTE`, which only exists inside RabbitMQ, so it never reaches Solace. Everything else must arrive exactly as sent: the topic or routing key, every header, correlation-id, content-type, message-id, reply-to and every byte of the payload.

The scripts talk to RabbitMQ over AMQP 1.0 only, like the Bridge and every EMS in the region (`../docs/adr/0007-bridge-talks-amqp-1-0.md`).

## The files

| File | What it is |
|---|---|
| `env.sh` | **Script 1. Setup.** Load it with `source` (do not run it with `bash`). It sets the broker addresses and user names, asks for the passwords, downloads the jars if they are missing, checks that it can log in to Solace and RabbitMQ, and checks that the Bridge's two RabbitMQ connections are AMQP 1.0. |
| `2-pubsub-solace-to-rabbitmq.sh` | **Script 2. Pub/Sub, Solace → RabbitMQ.** Run it on the Solace machine. |
| `3-rr-solace-to-rabbitmq.sh` | **Script 3. Async Request/Reply, Solace → RabbitMQ.** Run it on the Solace machine. |
| `4-pubsub-rabbitmq-to-solace.sh` | **Script 4. Pub/Sub, RabbitMQ → Solace.** Run it on the RabbitMQ machine. |
| `5-rr-rabbitmq-to-solace.sh` | **Script 5. Async Request/Reply, RabbitMQ → Solace.** Run it on the RabbitMQ machine. |
| `BridgeTest.java` | The program that scripts 2–5 call to send and check. Java runs it straight from the source file; there is nothing to build. |
| `jars.txt` | Download links for `jakarta.jms-api` and `slf4j-nop`, the jars the program needs besides the Solace and RabbitMQ jars. |
| `lib/` | Created by `env.sh`. It holds those two jars. Git ignores it. |

The Solace jars are listed in `../nifi/solace-jars.txt` and go into `../lib/`; RabbitMQ's AMQP 1.0 client is listed in `../nifi/rabbitmq-jars.txt` and goes into `../lib-rabbitmq/`. These are the same folders NiFi uses. `jakarta.jms-api` and `slf4j-nop` are kept apart in `tests/lib/`: NiFi already has its own copies, and a second copy in NiFi's folders could clash with them.

## What each script sends

| Script | Sends | Must arrive at |
|---|---|---|
| 2 | Solace topic `t/vnm/vatm/dev/atfm/v1/fpl` | RabbitMQ exchange `x/vnm/vatm/dev/ingress`, routing key `t.vnm.vatm.dev.atfm.v1.fpl` |
| 3 | Solace topic `tr/vnm/vatm/vnm/vna/dev/fpms/v1/filing/reply`, `APAC_RECIPIENT_LIST` = `VV_VATM,VV_HVN,WS_CAAS`, reply-to = queue `q/vnm/vatm/dev/fpms/reply` | RabbitMQ `x/vnm/vatm/dev/ingress`, with `reply-to` = `q/vnm/vatm/dev/fpms/reply` |
| 4 | RabbitMQ exchange `x/vnm/vatm/dev/swim`, key `t.vnm.acv.dev.aodb.v1.departure.publish.vvts`, `APAC_RECIPIENT_LIST` = `VV_VATM, WS_CAAS` (the space is there on purpose: the Bridge must not remove it) | Solace topic `t/vnm/acv/dev/aodb/v1/departure/publish/vvts` |
| 5 | RabbitMQ exchange `x/vnm/vatm/dev/route` with `VV_ROUTE` = `VV_VATM`, key `tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request`, `APAC_RECIPIENT_LIST` = `VV_VATM,WS_CAAS`, reply-to `q/vnm/vna/dev/swim/reply` | Solace topic `tr/vnm/vna/vnm/vatm/dev/swim/v1/filing/request`, received through VATM's selector, with the full list `VV_VATM,WS_CAAS` and no `VV_ROUTE` |

The message has the 14 Pathfinder headers (with values that contain commas, colons and `-`) and a FIXM XML payload of about 19 KB (Vietnamese text, `→`, `&amp;`, quotes, tabs).

Scripts 4 and 5 put the message straight on `swim`/`route`, the way the Router would. The Router is a separate application and is not running yet.

Request/Reply is checked one way only: the request must reach the other side with its reply-to and correlation-id unchanged. Sending the reply is the receiver's job, not the Bridge's.

## What you need

On each machine:
- Java 11 or newer (`java -version`);
- `curl`;
- a copy of this repository.

Both machines must be able to reach:
- Solace, port `55555`;
- RabbitMQ, port `5672` (AMQP 1.0);
- RabbitMQ management, port `15672`. The scripts only read the list of connections there.

None of these links is encrypted yet: AMQP 1.0 runs on plain `5672`, and the management API on plain HTTP with Basic auth. TLS on `5671` is a known gap (`../docs/adr/0007-bridge-talks-amqp-1-0.md`), so run the tests only on a trusted network.

The Bridge must be running.

## How to run

### 1. Setup, on each machine

From the repository folder:

```
source tests/env.sh
```

It asks for the two passwords. They stay in this shell only and are never written to a file.

A good result looks like this:

```
OK   java 21.0.12.1
OK   jars in lib/, lib-rabbitmq/ and tests/lib/
OK   Solace tcp://localhost:55555 VPN default as hieu
OK   RabbitMQ 192.168.121.61:5672 vhost swim_sg as hieu, AMQP 1.0, RabbitMQ 4.2.4

Bridge connections on RabbitMQ
  OK   vatm-bridge B-03            [AMQP 1-0]
  OK   vatm-bridge B-04            [AMQP 1-0]

PASS
```

The last two lines check the Bridge itself: B-03 and B-04 each have a connection to RabbitMQ, named `vatm-bridge B-03` and `vatm-bridge B-04`, and both use AMQP 1.0. A missing connection shows as `[]`, an old AMQP 0-9-1 Bridge as `[AMQP 0-9-1]`. Either one means the Bridge is not running, or is not the AMQP 1.0 version: start the group `Solace RabbitMQ Bridge` in NiFi, or load the current `nifi/bridge-flow.json`.

If a login fails, both passwords are cleared, so the next `source tests/env.sh` asks for them again.

**Settings and defaults.** To change one, `export` it **before** `source tests/env.sh`:

| Variable | Default |
|---|---|
| `SOLACE_HOST` | `tcp://localhost:55555` |
| `SOLACE_VPN` | `default` |
| `SOLACE_USERNAME` | `hieu` |
| `RABBITMQ_HOST` | `192.168.121.61` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_MANAGEMENT_PORT` | `15672` |
| `RABBITMQ_VHOST` | `swim_sg` |
| `RABBITMQ_USERNAME` | `hieu` |

On the RabbitMQ machine, Solace is not on `localhost`, so set where it is first. For example:

```
export SOLACE_HOST=tcp://solace.tailfac6af.ts.net:55555
source tests/env.sh
```

Do not type `export SOLACE_PASSWORD=...` yourself, because the password would then stay in your shell history. Let the script ask.

### 2. The tests

On the Solace machine:

```
tests/2-pubsub-solace-to-rabbitmq.sh
tests/3-rr-solace-to-rabbitmq.sh
```

On the RabbitMQ machine:

```
tests/4-pubsub-rabbitmq-to-solace.sh
tests/5-rr-rabbitmq-to-solace.sh
```

Each script waits up to 30 seconds for its message on the other broker, then prints one line per check. Below is the shape of the output, with some lines cut:

```
Sent to RabbitMQ x/vnm/vatm/dev/route key tr.vnm.vna.vnm.vatm.dev.swim.v1.filing.request with VV_ROUTE=VV_VATM, correlation-id bridge-test-1791007955728. Waiting for it on Solace topic tr/vnm/vna/vnm/vatm/dev/swim/v1/filing/request ...

Arrived on Solace (VATM selector matched)
  OK   topic                       'topic tr/vnm/vna/vnm/vatm/dev/swim/v1/filing/request'
  OK   APAC_SOURCE                 'VV_HVN'
  OK   APAC_RECIPIENT_LIST         'VV_VATM,WS_CAAS'
  ...
  OK   VV_ROUTE                    absent
  OK   correlation-id              'bridge-test-1791007955728'
  OK   reply-to                    'queue q/vnm/vna/dev/swim/reply'
  OK   payload                     19087 bytes, sha256 0c8bf313da9964aa

PASS
```

A wrong value is printed as `FAIL`, followed by the value that was expected.

### What the result means

| Last line | Exit code | Meaning |
|---|---|---|
| `PASS` | 0 | The message arrived and nothing was changed. |
| `FAIL: N check(s) failed` | 1 | The message arrived, but N values differ. Each one is marked `FAIL`. |
| `FAIL nothing arrived ...` | 1 | Nothing arrived within 30 seconds. The line says what to check: whether the Bridge is running, whether the topic or key reaches the Bridge's queue, and whether VATM is in the list. |
| `FAIL Solace: ...`, `FAIL RabbitMQ ...`, `... is not set` | 2 | The test could not run: a wrong password, a broker that cannot be reached, or a missing `source tests/env.sh`. |

## What you see in the admin UIs

The script's own output is the proof. You can also watch the message pass in the two admin UIs:
- **Solace Broker Manager** at `http://<solace host>:18080`, Message VPN `default`;
- **RabbitMQ management** at `http://192.168.121.61:15672`, vhost `swim_sg`.

Write down the numbers below **before** you run a script, then compare them after.

The two UIs count differently:
- **Solace** keeps a running total of the messages each queue has received (on the queue's **Stats** tab; the SEMP field is `spooledMsgCount`). One test adds exactly **1**.
- **RabbitMQ** shows only what is waiting in a queue now (**Ready**, **Total**) and how fast messages move (the **Message rates** charts). It keeps no running total. A message that the Bridge takes straight away shows as a short bump in the chart, not as a +1. The page refreshes every 5 seconds.

### Scripts 2 and 3 (Solace → RabbitMQ)

| Where | What changes |
|---|---|
| Solace: **Queues** → `q/vnm/vatm/dev/bridge/outbound` → **Stats** | Total messages spooled **+1**. **Messages Queued** goes back to 0 within a second, because NiFi (B-04) takes the message straight away. If it stays at 1 or more, the Bridge is not reading. |
| Solace: **Queues** → **Topic Endpoints** tab → `q/vnm/vatm/dev/bridge/out-atfm` | **Script 2 only:** **+1**, and the message **stays** there. This is the old Bridge's endpoint (see the note below). Script 3 does not touch it. |
| RabbitMQ: **Queues and Streams** → `q/vnm/vatm/dev/router/in` | **Ready +1**, and it **stays**. This is the original copy, waiting for the Router. To see it: **Get messages**, Ack Mode `Nack message requeue true`; its `correlation_id` is the `bridge-test-...` value the script printed. The UI shows AMQP 1.0 fields under their old names: `correlation_id`, `content_type`, `delivery_mode: 2` for durable, and the application-properties as `headers`. |
| RabbitMQ: **Queues and Streams** → `q/vnm/vatm/dev/bridge-test/bridge-test-...` | The script's temporary queue (feature `Exp`), made over AMQP 1.0. It exists only for the few seconds the script runs, so you may not catch it. Afterwards it is gone. |
| RabbitMQ: **Exchanges** → `x/vnm/vatm/dev/ingress` | A short bump in **Message rates** in and out. |

### Scripts 4 and 5 (RabbitMQ → Solace)

| Where | What changes |
|---|---|
| RabbitMQ: **Exchanges** → `x/vnm/vatm/dev/swim` (script 4) or `x/vnm/vatm/dev/route` (script 5) | A short bump in **Message rates** in and out. |
| RabbitMQ: **Queues and Streams** → `q/vnm/vatm/dev/bridge/inbound` | **Ready** stays 0, because NiFi (B-03) takes the message straight away. **Message rates** shows a short bump in *Publish*, *Deliver* and *Ack*. If **Ready** stays at 1 or more, the Bridge is not reading. Today no other queue gets these two messages. |
| RabbitMQ: **Queues and Streams** → `q/vnm/vatm/dev/eems/unrouted` | **No change.** If **Ready** goes up, the key (script 4) or the headers (script 5) matched no binding, so the message never reached the Bridge. |
| Solace: **Queues** → **Topic Endpoints** tab | While the script waits (a few seconds), a temporary endpoint appears. It has a long generated name that contains the topic, for example `t/vnm/acv/dev/aodb/v1/departure/publish/vvts`, and it receives 1 message. It disappears when the script ends. |
| Solace: **Queues** | **No change.** No durable queue on Solace subscribes to these two topics, so no total goes up. Queue `q/vnm/vna/dev/swim/reply` is only the reply-to *name* in script 5; nothing is sent to it. |

**Note on `out-atfm`:** the old Bridge's Topic Endpoint `q/vnm/vatm/dev/bridge/out-atfm` subscribes to `t/vnm/vatm/dev/atfm/>` and has no consumer. Every run of script 2 therefore leaves one more message there, and so does every `atfm` message anyone sends. The root `README.md` ("Object cũ") explains how to delete it once it is no longer needed.

## Options

| Option | Scripts | What it does |
|---|---|---|
| `--recipients LIST` | 2–5 | Send this `APAC_RECIPIENT_LIST` instead of the default. |
| `--payload FILE` | 2–5 | Send this UTF-8 file instead of the sample FIXM message. |
| `--topic TOPIC` | 2, 3 | Send to this Solace topic. It must be in the subscriptions of Solace queue `q/vnm/vatm/dev/bridge/outbound`, otherwise the Bridge never sees it. |
| `--key KEY` | 4, 5 | Send with this RabbitMQ routing key. It must reach `q/vnm/vatm/dev/bridge/inbound`, otherwise the Bridge never sees it. |

An option that does not belong to a script (for example `--topic` on script 4) stops the script with the usage text.

Examples:

```
# HVN sends only to CAAS. VATM is not in the list, so VATM's selector must not match,
# and the expected result is "FAIL nothing arrived".
tests/5-rr-rabbitmq-to-solace.sh --recipients WS_CAAS

# Send your own message
tests/2-pubsub-solace-to-rabbitmq.sh --payload ~/my-flight-plan.xml

# Send to another topic that the Bridge reads
tests/2-pubsub-solace-to-rabbitmq.sh --topic t/vnm/vatm/dev/met/v1/metar
```

## What the scripts leave behind

Each run uses its own correlation-id, `bridge-test-<time>`, so it only ever picks up its own message.

- **Scripts 2 and 3** create a temporary RabbitMQ queue, `q/vnm/vatm/dev/bridge-test/<correlation-id>`, bound to `x/vnm/vatm/dev/ingress`. They read their copy of the message there and delete the queue at the end. If a script is stopped halfway, RabbitMQ deletes the queue by itself after 10 minutes.
- **Scripts 4 and 5** create a temporary Solace subscription. It disappears when the script ends.

**The test messages are real messages.** Other queues on the same exchange also get a copy:
- After scripts 2 and 3, a copy stays in `q/vnm/vatm/dev/router/in`. When the Router runs, it delivers that copy to the parties in `APAC_RECIPIENT_LIST`.
- Script 2 also leaves a copy in the old Solace Topic Endpoint `q/vnm/vatm/dev/bridge/out-atfm`.
- Scripts 4 and 5 reach only `q/vnm/vatm/dev/bridge/inbound` today. If someone later binds another queue to `swim` or `route` that matches, that queue gets a copy too.

Outside the `dev` environment, use `--recipients` with codes that are not real partners.

When you have finished testing, remove the passwords from the shell:

```
unset SOLACE_PASSWORD RABBITMQ_PASSWORD
```

## See also

- `../README.md`, section "Kiểm tra bằng tay".
