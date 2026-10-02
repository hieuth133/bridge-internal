# The Routing key is the SWIM topic with `/` turned into `.`

Supersedes ADR 0002.

VATM's EMS design (`docs/Documents/SWIM_APAC_Giai_thich_EMS_v4.docx`, `SWIM_APAC_Cau_hinh_EMS_v4.xlsx`) puts a Router on the External EMS and routes by topic exchange as well as by header. A topic exchange only matches wildcards on `.`-separated words, so the Bridge now turns `/` into `.` when a message goes to RabbitMQ (`t/vnm/vatm/dev/atfm/v1/fpl` → `t.vnm.vatm.dev.atfm.v1.fpl`) and back again when it comes to Solace. Subscribers on RabbitMQ can then bind `t.vnm.vatm.dev.atfm.#`, just as Solace subscribers use `t/vnm/vatm/dev/atfm/>`.

What this costs us: the partners on `vhost swim_sg` that publish with `/` in the key do not match these bindings. They have to publish to `x/vnm/vatm/dev/ingress` with `.` keys. Topic levels must not contain `.` themselves.
