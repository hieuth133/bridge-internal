# Context

Glossary for how work gets done in this project.

## Workflow

**Orchestrator** — The main Claude session. Gives each Role its job, runs the tests itself, and talks to the human.
_Avoid:_ boss, manager

**Role** — One job in the team, done by one AI agent: BA, Tech Lead, Tester, Developer, or Reviewer.

**BA (Business Analyst)** — Role that turns a Feature idea into requirements: user stories and acceptance criteria. No code, no tech choices.

**Tech Lead** — Role that picks the design and splits a Feature into Steps.

**Tester** — Role that writes one failing test for one Step, before the code exists.

**Developer** — Role that writes the smallest code that makes the Step's test pass.

**Reviewer** — Role that checks each Step's change and answers PASS or FIX.

**Feature** — One thing a user wants, started with `/feature`. Has one Requirements issue, one branch, and one pull request.

**Requirements issue** — The GitHub issue the BA writes for a Feature.

**Step** — The smallest unit of work: one behavior, one test, one commit, about 100 changed lines or less. Tracked as a child issue of the Requirements issue.
_Avoid:_ task, ticket, slice

**Checkpoint** — A point where the human must approve before work goes on. There are two: after the Requirements issue, and after the Tech Lead's plan.

**Retry** — Sending a Step back to the Developer after a failed test or a FIX review. At most 2 per Step, then the human is asked.

## Messaging

**Internal EMS** — The Solace broker. Internal applications publish and subscribe here.
_Avoid:_ Solace side, inside broker

**External EMS** — The RabbitMQ broker in the DMZ. External EMSs and outside applications connect here.
_Avoid:_ Rabbit side, DMZ broker

**Bridge** — The application that moves messages between the Internal EMS and the External EMS, in both directions, without changing the payload, the headers or the message properties.
_Avoid:_ connector, relay, shovel

**Outbound** — The direction from the Internal EMS to the External EMS.

**Inbound** — The direction from the External EMS to the Internal EMS.

**SWIM topic** — A topic name that follows the SWIM naming convention, with `/` between levels. A Pub/Sub topic starts `t/<country>/<organisation>/<environment>/<system>/<version>/...`. A Request/Reply topic names the sender and the receiver, `tr/<country>/<organisation>/<country>/<organisation>/<environment>/...`.

**Routing key** — The name a message carries on the External EMS: the SWIM topic with `.` in place of `/` (see ADR 0005).

**Router** — The step on the External EMS that every message passes through, in both directions. A separate application, not part of the Bridge. It checks the mandatory APAC headers, adds the EEMS stamps, and sends a Pub/Sub message on by its Routing key and a Request/Reply message to each of its Recipient codes.
_Avoid:_ Bridge Rule, dispatcher

**EEMS stamp** — An entry `VV_EEMS_IN:<ms>` or `VV_EEMS_OUT:<ms>` that the Router appends to the `APAC_TIMESTAMP` header when a message enters or leaves the External EMS. Apart from the sender, who creates the header, nothing else adds to it.
_Avoid:_ bridge stamp

**Recipient code** — One entry of the `APAC_RECIPIENT_LIST` header, `<ICAO prefix>_<organisation>`, for example `VV_VATM`, `VV_HVN`, `WS_CAAS`. Codes that start `VV_` are inside Viet Nam; every other code is reached through GEMS.

**Route header** — The header `VV_ROUTE` that the Router puts on each copy of a Request/Reply message: one value per copy, the Recipient code, or `GEMS` for recipients outside Viet Nam. It exists only inside the External EMS; the Bridge removes it before a message enters the Internal EMS.
_Avoid:_ routing header, destination header

**Topic routing** — Delivering a message by its SWIM topic or Routing key alone: Solace subscriptions and RabbitMQ topic bindings. Used for Pub/Sub.

**Header routing** — Delivering a message by its headers: on RabbitMQ a headers binding on the Route header and `APAC_CATEGORY`, on Solace a selector on `APAC_RECIPIENT_LIST`. Used for Request/Reply.

**GEMS mode** — How the Router hands Request/Reply messages for recipients outside Viet Nam to GEMS: `header` sends one copy for all of them, `topic` sends one copy per recipient with that recipient in the Routing key.

**Dead letter queue** — The queue on the External EMS where the Router puts a message it cannot route, with the reason in the header `VV_DLX_REASON`.
_Avoid:_ DLQ (in prose), error queue
