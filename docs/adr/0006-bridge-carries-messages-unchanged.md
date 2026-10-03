# The Bridge carries messages unchanged; the Router is not part of it

Changes part of ADR 0004: B-03 now publishes to Solace from a Groovy script, not `PublishJMS`.

Commit 84b3354 put Router R-01 inside the NiFi flow. It read every message entering the External EMS, partner-to-partner traffic too, and it changed them: it trimmed `APAC_RECIPIENT_LIST`, dropped partner headers, added `VV_ROUTE` and `VV_DLX_REASON`, and stamped `APAC_TIMESTAMP`. B-03 then carried `VV_ROUTE` into Solace, so an HVN message to `VV_VATM,WS_CAAS` looked on Solace as if it were only for `VV_VATM`.

Decision:

- **Scope.** NiFi runs only the Bridge between VATM's Solace and RabbitMQ: B-03 (Inbound) and B-04 (Outbound). Router R-01 is a separate application on the External EMS side and is not in this repo.
- **Nothing changes.** The Bridge never changes the payload, any header, or message-id, correlation-id, content-type and reply-to. The one exception: B-03 drops `VV_ROUTE`, which exists only inside RabbitMQ (v4 doc §4.1).
- **EEMS stamps belong to the Router.** Only `VV_EEMS_IN` and `VV_EEMS_OUT` are ever added to `APAC_TIMESTAMP`, and the Router adds them (v4 Bảng 11, steps 4 and 9). v4 §8.3 says router and bridge keep payload, message-id, correlation-id, reply-to and APAC headers; we chose to give the stamp to one component only, the Router, so the Bridge writes none.
- **reply-to is carried both ways, name unchanged.** NiFi 2.12 `PublishJMS` only builds a reply-to whose name contains `queue` or `topic`, so SWIM names such as `q/vnm/vna/dev/swim/reply` were dropped. B-03 is therefore one `ExecuteGroovyScript` that publishes to Solace itself through the existing JNDI connection factory service: a name starting `q/` becomes a queue, any other a topic. B-04 copies a RabbitMQ property only when the Solace message has it, because `PublishAMQP` would send an empty value as `""`.

What this costs us:

- Nothing in this repo routes RabbitMQ traffic any more. Inbound header routing, partner-to-partner traffic, GEMS and the Dead letter queue depend on the separate Router.
- Until that Router runs, the `dev` environment only gets Inbound messages that are put directly on `x/vnm/vatm/dev/swim` or `x/vnm/vatm/dev/route` (the demo and `nifi/check-headers.py` do this), and Outbound messages wait in `q/vnm/vatm/dev/router/in`.
- B-03's publisher is our code, not a stock processor: about 50 lines of Groovy that keep one Solace connection open and reconnect after an error.
