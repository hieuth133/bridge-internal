# Bridge

Bridge chuyển message giữa Solace (Internal EMS) và RabbitMQ (External EMS) theo cả hai chiều. Các thuật ngữ được định nghĩa trong `CONTEXT.md`.

Bridge là một flow Apache NiFi (`nifi/bridge-flow.json`), chạy bằng image chính thức `docker.io/apache/nifi:2.12.0`. Không có code để build. Lý do chọn cách này nằm ở `docs/adr/0004-apache-nifi-replaces-camel.md`.

Các lệnh dưới đây dùng `podman`. Nếu dùng Docker, thay `podman` bằng `docker`, mọi thứ khác giữ nguyên.

## 1. Tạo object trên RabbitMQ (một lần)

NiFi không tự tạo exchange hay queue trên RabbitMQ. Mở RabbitMQ UI `http://192.168.121.61:15672` và tạo các object dưới đây trong vhost `swim_sg`. Tất cả đều là object mới, không sửa object nào đang có.

| Bước | Ở đâu | Làm gì |
|---|---|---|
| 1 | Exchanges → Add a new exchange | Name `x.swim.dev.bridge.out`, Type `topic`, Durability `Durable` |
| 2 | Exchanges → Add a new exchange | Name `x.swim.dev.bridge.in`, Type `topic`, Durability `Durable` |
| 3 | Queues and Streams → Add a new queue | Name `q/vnm/vatm/dev/bridge/in`, Type `Classic`, Durability `Durable` |
| 4 | Queues and Streams → Add a new queue | Name `q/vnm/vatm/dev/bridge/in-dlq`, Type `Classic`, Durability `Durable` |
| 5 | Mở queue `q/vnm/vatm/dev/bridge/in` → Bindings | From exchange `x.swim.dev.bridge.in`, Routing key `#` |

Trên Solace không cần làm gì. Lần đầu chạy, Bridge tự tạo Durable Topic Endpoint `q/vnm/vatm/dev/bridge/out-atfm` nếu nó chưa có.

## 2. Tải thư viện Solace JMS

NiFi cần jar của Solace để nói chuyện với Solace. Danh sách jar nằm trong `nifi/solace-jars.txt`, tất cả lấy từ Maven Central. Chạy trong thư mục của repo:

```
mkdir -p lib
(cd lib && xargs -n1 curl -fsSLO < ../nifi/solace-jars.txt)
ls lib | wc -l     # phải ra 18
```

## 3. Chạy NiFi

Chọn mật khẩu đăng nhập NiFi, dài ít nhất 12 ký tự, và export nó trong shell. Không ghi mật khẩu vào file nào.

```
export SINGLE_USER_CREDENTIALS_PASSWORD='...'
```

Liệt kê các tên mà bạn sẽ gõ trên trình duyệt để mở NiFi, dạng `tên:8443`, cách nhau bằng dấu phẩy. Tên máy (`hostname`) và `localhost` đã có sẵn, không cần ghi. Ví dụ với tên Tailscale của máy (xem bằng `tailscale status --self`):

```
export NIFI_WEB_PROXY_HOST='solace.tailfac6af.ts.net:8443'
```

Chạy trong thư mục của repo (vì lệnh mount `./lib`):

```
podman run -d --name vatm-bridge --network=host --restart=always \
  -e NIFI_WEB_HTTPS_HOST=0.0.0.0 -e NIFI_WEB_PROXY_HOST \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin -e SINGLE_USER_CREDENTIALS_PASSWORD \
  -v vatm-bridge-conf:/opt/nifi/nifi-current/conf \
  -v vatm-bridge-state:/opt/nifi/nifi-current/state \
  -v vatm-bridge-database:/opt/nifi/nifi-current/database_repository \
  -v vatm-bridge-flowfile:/opt/nifi/nifi-current/flowfile_repository \
  -v vatm-bridge-content:/opt/nifi/nifi-current/content_repository \
  -v vatm-bridge-provenance:/opt/nifi/nifi-current/provenance_repository \
  -v ./lib:/opt/nifi/solace-lib:ro,Z \
  docker.io/apache/nifi:2.12.0
```

- `--network=host` để NiFi tới được Solace ở `localhost:55555`.
- `NIFI_WEB_HTTPS_HOST=0.0.0.0` để NiFi nghe trên mọi địa chỉ của máy (LAN, Tailscale...). Mặc định nó chỉ nghe trên địa chỉ ứng với tên máy.
- NiFi tạo chứng chỉ HTTPS tự ký ở lần chạy đầu tiên. Chứng chỉ chỉ chứa `localhost`, tên máy và các tên trong `NIFI_WEB_PROXY_HOST`. Mở bằng tên khác, hay bằng địa chỉ IP, sẽ bị lỗi `Invalid SNI`. Muốn thêm tên sau này thì phải gỡ hẳn rồi chạy lại từ đầu (xem Vận hành) và nạp lại flow.
- Các volume `vatm-bridge-*` giữ flow, mật khẩu đã nhập và các message đang trên đường đi. Đừng xoá chúng khi Bridge còn dùng.
- Mật khẩu đăng nhập chỉ được đặt ở lần chạy đầu tiên, sau đó nó được lưu trong volume `vatm-bridge-conf`.

Chờ khoảng 1 phút rồi xem log. Khi thấy dòng `Started Application` là NiFi đã sẵn sàng:

```
podman logs vatm-bridge 2>&1 | grep 'Started Application'
```

`--restart=always` chưa đủ để Bridge tự chạy lại sau khi khởi động lại máy. Cần bật thêm dịch vụ restart của Podman:

```
sudo systemctl enable --now podman-restart.service
```

Nếu chạy Podman bằng user thường (rootless):

```
systemctl --user enable --now podman-restart.service
loginctl enable-linger $USER
```

Với Docker thì chỉ cần `sudo systemctl enable --now docker`.

## 4. Nạp flow vào NiFi (một lần)

1. Mở `https://<tên>:8443/nifi` bằng một trong các tên đã khai báo ở bước 3, ví dụ `https://solace.tailfac6af.ts.net:8443/nifi` hoặc `https://solace.vnaic.vn:8443/nifi`. Không dùng địa chỉ IP. Trình duyệt sẽ cảnh báo chứng chỉ tự ký, hãy chấp nhận cảnh báo đó. Nếu bị timeout, kiểm tra firewall đã mở port 8443 chưa: `sudo firewall-cmd --permanent --zone=public --add-port=8443/tcp && sudo firewall-cmd --reload`.
2. Đăng nhập bằng user `admin` và mật khẩu đã đặt ở bước 3.
3. Kéo biểu tượng **Process Group** trên thanh công cụ thả vào canvas. Trong hộp thoại, bấm biểu tượng **Browse** (upload) cạnh ô tên, chọn file `nifi/bridge-flow.json`, rồi bấm **Add**. Một group tên `Solace RabbitMQ Bridge` xuất hiện, kèm Parameter Context `bridge`.
4. Nhập mật khẩu broker: menu ☰ (góc trên bên phải) → **Parameter Contexts** → dòng `bridge` → ⋮ → **Edit** → tab **Parameters**. Sửa `solace.password` và `rabbitmq.password` → **Apply**. Hai giá trị này được NiFi mã hoá và không bao giờ nằm trong file flow.
5. Nếu Solace hay RabbitMQ của bạn khác mặc định, sửa luôn các tham số khác ở đây: `solace.host`, `solace.vpn`, `solace.username`, `rabbitmq.host`, `rabbitmq.port`, `rabbitmq.vhost`, `rabbitmq.username`.
6. Chuột phải vào group `Solace RabbitMQ Bridge` → **Enable All Controller Services**, rồi chuột phải lần nữa → **Start**.

Group con `Bridge demo` vẫn ở trạng thái Disabled, nó chỉ dùng cho phần [Demo](#demo).

Bridge đã chạy. Trên mỗi processor, biểu tượng ▶ màu xanh nghĩa là đang chạy. Nếu có lỗi, một ô đỏ sẽ hiện ở góc processor; di chuột vào để đọc lỗi.

## Object

Mọi tên dưới đây là tên thật trên broker.

| Chiều | Người gửi gửi vào | Bridge đọc từ | Bridge ghi vào | Người nhận đọc từ |
|---|---|---|---|---|
| Outbound (Solace sang RabbitMQ) | Solace topic `t/vnm/vatm/dev/atfm/v1/fpl` | Durable Topic Endpoint `q/vnm/vatm/dev/bridge/out-atfm` (subscription `t/vnm/vatm/dev/atfm/>`) | Exchange `x.swim.dev.bridge.out`, Routing key `t/vnm/vatm/dev/atfm/v1/fpl` | Queue của người nhận, bind với Routing key đó |
| Inbound (RabbitMQ sang Solace) | Exchange `x.swim.dev.bridge.in`, Routing key `ext/met/metar` | Queue `q/vnm/vatm/dev/bridge/in` (bind `#`) | Solace topic `t/vnm/vatm/dev/ext/met/metar` | Solace subscription `t/vnm/vatm/dev/ext/>` |
| Dead letter | Exchange `x.swim.dev.bridge.in`, Routing key `foo/bar` (không khớp Bridge Rule nào) | Queue `q/vnm/vatm/dev/bridge/in` | Dead letter queue `q/vnm/vatm/dev/bridge/in-dlq` | Queue `q/vnm/vatm/dev/bridge/in-dlq` |

Routing key trên RabbitMQ chính là SWIM topic, giữ nguyên cả dấu `/` (xem `docs/adr/0002-routing-key-is-swim-topic-verbatim.md`).

## Flow

```mermaid
flowchart LR
  subgraph Outbound
    ST["Solace topic t/vnm/vatm/dev/atfm/..."] --> C1["ConsumeJMS<br/>durable q/vnm/vatm/dev/bridge/out-atfm"]
    C1 --> U1["UpdateAttribute<br/>routing key + properties"]
    U1 --> P1["PublishAMQP<br/>x.swim.dev.bridge.out"]
  end
  subgraph Inbound
    Q["Queue q/vnm/vatm/dev/bridge/in"] --> C2["ConsumeAMQP"]
    C2 --> G["ExecuteGroovyScript<br/>RabbitMQ headers to JMS properties"]
    G --> U2["UpdateAttribute<br/>RabbitMQ properties to JMS"]
    U2 --> R["RouteOnAttribute<br/>một nhánh cho mỗi Bridge Rule"]
    R -- ext --> U3["UpdateAttribute<br/>ext/ to t/vnm/vatm/dev/ext/"]
    U3 --> P2["PublishJMS<br/>Solace topic"]
    R -- unmatched --> D["PublishAMQP<br/>q/vnm/vatm/dev/bridge/in-dlq"]
  end
```

Mỗi processor gửi đi có một nhánh `failure` quay về chính nó. Khi broker bên kia không nhận, NiFi giữ message lại và thử gửi lại, không làm mất message.

## Demo

Phần này demo trên UI rằng message mang header theo template Pathfinder (`docs/Documents/Pathfinder_Headers_Metadata_updated 24 Sep 2026.xlsx`, sheet "Headers for SIPG Test") đi qua Bridge mà không mất hay đổi header, và payload giữ nguyên. Muốn kiểm tra tự động, so từng byte, thì xem [Kiểm tra bằng script](#kiểm-tra-bằng-script).

Bạn dùng ba trang:
- NiFi `https://<tên>:8443/nifi`
- RabbitMQ UI `http://192.168.121.61:15672`
- Solace Broker Manager `http://localhost:18080` (không bắt buộc)

Phía Solace được demo bằng NiFi, vì Try-Me trong Broker Manager chỉ đặt được correlation id, priority, TTL và delivery mode, không đặt hay hiển thị được user property (header).

Trong group `Solace RabbitMQ Bridge` có sẵn group con **`Bridge demo`**, đi kèm `nifi/bridge-flow.json`. Mọi processor trong đó đang ở trạng thái Disabled, nên nó không gửi gì khi bạn Start Bridge. Group gồm:

| Processor | Làm gì |
|---|---|
| `Solace sender: Pathfinder message` → `Solace sender: publish t/vnm/vatm/dev/atfm/v1/fpl` | Tạo message FIXM mẫu có đủ 14 header Pathfinder, gửi lên Solace. Bản đã gửi nằm lại ở connection `sent to Solace`. |
| `Solace receiver: subscribe t/vnm/vatm/dev/ext/>` | Nhận message Bridge chuyển sang Solace. Message nhận được nằm ở connection `received from Solace`. |
| `RabbitMQ sender: Pathfinder message` → `RabbitMQ sender: publish x.swim.dev.bridge.in` | Tạo cùng message mẫu, gửi vào exchange `x.swim.dev.bridge.in` với routing key lấy từ property `routingKey` (mặc định `ext/fixm/fpl`). Bản đã gửi nằm ở `sent to RabbitMQ`. |

**Xem header và payload của một message trong NiFi:** chuột phải vào connection (mũi tên có tên, ví dụ `received from Solace`) → **List Queue**. Ở dòng message, bấm ⋮ → **View Details**. Tab **Attributes** là các header, nút **View** (hoặc ⋮ → **View Content**) mở payload.

**Sửa header hay payload mẫu:** chuột phải vào processor `... sender: Pathfinder message` → **Configure** → tab **Properties**. Mỗi header là một property: tên property là tên header, giá trị là giá trị header. Bấm **+** để thêm, biểu tượng thùng rác để xoá. Payload nằm ở property `Custom Text`.

### Chuẩn bị

1. NiFi: mở group `Solace RabbitMQ Bridge`. Chuột phải vào `Bridge demo` → **Enable**, rồi bấm đúp để mở group.
2. Giữ Shift, chọn 3 processor `Solace receiver: ...`, `Solace sender: publish ...` và `RabbitMQ sender: publish ...`. Chuột phải → **Start**. Không Start cả group, vì như vậy hai processor `... Pathfinder message` sẽ gửi message ngay.
3. RabbitMQ UI: tạo queue `q/vnm/vatm/dev/bridgedemo/userb` (Queues and Streams → Add a new queue). Mở queue đó → **Bindings** → From exchange `x.swim.dev.bridge.out`, Routing key `t/vnm/vatm/dev/atfm/v1/fpl` → **Bind**.

   Phải làm bước này **trước** khi gửi Outbound. Nếu không có queue nào bind với routing key, RabbitMQ từ chối message (`NO_ROUTE`) và Bridge sẽ thử gửi lại mãi (xem [Vận hành](#vận-hành)).

### 1. Outbound: Solace sang RabbitMQ

1. NiFi: chuột phải vào `Solace sender: Pathfinder message` → **Run Once**. Message được gửi lên Solace topic `t/vnm/vatm/dev/atfm/v1/fpl`.
2. RabbitMQ UI: mở queue `q/vnm/vatm/dev/bridgedemo/userb` → **Get messages** → **Get Message(s)**.
3. Kết quả mong đợi:
   - Routing key là `t/vnm/vatm/dev/atfm/v1/fpl`.
   - Mục **Properties** có `headers` với đủ 14 header (`APAC_SOURCE`, `APAC_RECIPIENT_LIST`, `APAC_CATEGORY`, `APAC_CATEGORY_VERSION`, `APAC_MESSAGE_TYPE`, `DEP_AIRPORT`, `ARR_AIRPORT`, `AIRLINE`, `ACID`, `GUFI`, `GUFI_NAMESPACE_IDENTIFIER`, `EOBT`, `FFICE_PHASE`, `APAC_TIMESTAMP`), và các giá trị giống hệt trong `sent to Solace`.
   - Ngoài ra có `content_type: application/xml` và `delivery_mode: 2`.
   - **Payload** giống hệt nội dung message đã gửi.

### 2. Inbound: RabbitMQ sang Solace

Cách nhanh, dùng message mẫu có sẵn 14 header:

1. NiFi: chuột phải vào `RabbitMQ sender: Pathfinder message` → **Run Once**.
2. Connection `received from Solace` hiện 1 message. **List Queue** → **View Details**:
   - attribute `jms_destination` = `t/vnm/vatm/dev/ext/fixm/fpl`;
   - đủ 14 header với giá trị như trong `sent to RabbitMQ`;
   - `contentType` = `application/xml`, `jms.messagetype` = `TextMessage`.
   - **View Content** cho thấy payload giống hệt.

Cách tự gõ trong RabbitMQ UI:

1. RabbitMQ UI: Exchanges → `x.swim.dev.bridge.in` → **Publish message**.
   - Routing key: `ext/fixm/fpl`.
   - Headers: mỗi dòng một header, ví dụ `APAC_SOURCE` = `VV_VATM`, `APAC_MESSAGE_TYPE` = `FILED_FLIGHT_PLAN`, `APAC_TIMESTAMP` = `VV_EEMS_OUT:1790873508104`. Kiểu chọn `String`.
   - Properties: `content_type` = `application/xml`, `message_id` = `demo-2`.
   - Payload: dán một đoạn XML bất kỳ.
2. Bấm **Publish message**, rồi xem `received from Solace` như trên. Mỗi header bạn gõ hiện ra là một attribute cùng tên và cùng giá trị.

Solace Try-Me (subscribe `t/vnm/vatm/dev/ext/>`) cũng nhận được message, nhưng chỉ hiện payload, không hiện header.

### 3. Dead letter

1. NiFi: chuột phải vào `RabbitMQ sender: Pathfinder message` → **Configure** → đổi `routingKey` thành `foo/fixm` → **Apply** → chuột phải → **Run Once**. Không Bridge Rule nào khớp với key này.
2. RabbitMQ UI: mở queue `q/vnm/vatm/dev/bridge/in-dlq` → **Get messages**. Message nằm ở đó, với đủ 14 header và payload giữ nguyên.
3. Đặt lại `routingKey` = `ext/fixm/fpl`.

### Dọn dẹp

1. NiFi: chuột phải vào từng connection `sent to Solace`, `received from Solace` và `sent to RabbitMQ` → **Empty Queue**.
2. Chuột phải vào nền trống trong group `Bridge demo` → **Stop**, rồi lại chuột phải → **Disable**. Group trở về như lúc mới nạp.
3. RabbitMQ UI: xoá queue `q/vnm/vatm/dev/bridgedemo/userb` (mở queue → **Delete**).
4. Lấy message demo ra khỏi `q/vnm/vatm/dev/bridge/in-dlq`: **Get messages** với Ack Mode `Automatic ack`. Chỉ dùng **Purge** khi chắc chắn trong queue chỉ có message demo.

### Kiểm tra bằng script

`nifi/check-headers.py` gửi một message có đủ 14 header Pathfinder (cả giá trị có dấu phẩy, dấu hai chấm, dấu `-`) và payload XML khoảng 19 KB (tiếng Việt, `→`, `&amp;`, dấu nháy, tab) qua cả ba đường. Rồi nó so từng header và từng byte payload. Chỉ cần Python 3 có sẵn trên máy, không cần thư viện nào thêm. Chạy trên máy có NiFi, khi Bridge đang chạy:

```
python3 nifi/check-headers.py
```

Script hỏi mật khẩu đăng nhập NiFi (hoặc đọc biến `NIFI_PASSWORD`). Nó không cần mật khẩu broker, vì nó tạo một group tạm trong NiFi dùng chính Parameter Context `bridge`. Mọi thứ nó tạo đều bị xoá khi chạy xong:
- group tạm;
- queue tạm `q/vnm/vatm/dev/bridgetest/check-headers`;
- message test trong `in-dlq`. Message này chỉ bị xoá nếu queue không còn message nào khác, nếu không script để nó lại và báo.

Kết quả đúng kết thúc bằng:

```
Messages arrived (outbound, inbound, dead letter): [1, 1, 1], expected [1, 1, 1]
PASS
```

Mỗi header sai được in `FAIL` kèm giá trị đã gửi, và script thoát với mã 1. Các biến tuỳ chọn: `NIFI_URL` (mặc định `https://localhost:8443`), `NIFI_USERNAME` (mặc định `admin`), `RABBITMQ_MANAGEMENT_PORT` (mặc định `15672`).

## Những gì Bridge giữ lại

- Nội dung message giữ nguyên.
- Outbound: thuộc tính JMS `contentType`, JMS message id, JMS correlation id và JMS delivery mode (persistent hay không) trở thành `content_type`, `message_id`, `correlation_id` và `delivery_mode` trên RabbitMQ. Các JMS property khác do người gửi đặt (ví dụ `priority=high`) trở thành header trên RabbitMQ.
- Inbound: `content_type`, `message_id` và `correlation_id` trên RabbitMQ trở thành JMS property `contentType`, `messageId` và JMS correlation id trên Solace. Mỗi header của RabbitMQ trở thành một JMS property cùng tên trên Solace (ví dụ header `x-trace-id` = `abc-1` thành property `x-trace-id` = `abc-1`). Giá trị luôn được gửi dạng chuỗi, nên header số `5` thành chuỗi `"5"`. Header có tên `uuid`, `filename`, `path`, `topic`, `contentType`, `messageId` hoặc bắt đầu bằng `jms_` không được chuyển, vì NiFi dùng các tên này cho việc riêng. Message tới Solace luôn là TextMessage (UTF-8), vì SWIM payload là XML hoặc JSON.
- Dead letter: message giữ nguyên nội dung, các thuộc tính và header.

## Thêm Bridge Rule

Một Bridge Rule gồm chiều (out hoặc in), prefix nguồn và prefix đích. Phần tên phía sau prefix nguồn được giữ nguyên. Mỗi rule là một vài processor trong group `Solace RabbitMQ Bridge`. Chọn đúng processor rồi chuột phải → **Configure** → tab **Properties** để sửa.

Lưu ý:
- Trong cùng một chiều, các prefix nguồn không được chồng lên nhau, nếu không một message sẽ được chuyển hai lần.
- Đích của rule `in` không được nằm trong nguồn của rule `out` nào, nếu không message sẽ chạy vòng giữa hai broker.

### Rule out (Solace sang RabbitMQ)

Ví dụ: rule `met` chuyển `t/vnm/vatm/dev/met/...` sang Routing key giữ nguyên tên.

1. Chọn cả hai processor `out atfm: ...`, nhấn Ctrl+C rồi Ctrl+V.
2. Ở bản sao của **ConsumeJMS**:
   - `Destination Name` = `t/vnm/vatm/dev/met/>` (prefix nguồn, kết thúc bằng `/`, rồi `>`)
   - `Subscription Name` = `q/vnm/vatm/dev/bridge/out-met` (mỗi rule một tên riêng; đây là tên Durable Topic Endpoint)
3. Ở bản sao của **UpdateAttribute**, sửa `routingKey` = `<prefix đích>${jms_destination:substringAfter('<prefix nguồn>')}`, ví dụ `t/vnm/vatm/dev/met/${jms_destination:substringAfter('t/vnm/vatm/dev/met/')}`.
4. Đổi tên hai processor cho dễ đọc, nối bản sao UpdateAttribute (`success`) vào `Publish to RabbitMQ x.swim.dev.bridge.out`, rồi Start hai processor mới.

### Rule in (RabbitMQ sang Solace)

Ví dụ: rule `aim` chuyển Routing key `aim/...` sang `t/vnm/vatm/dev/aim/...`.

1. Ở processor **in: match Bridge Rule** (RouteOnAttribute), bấm **+** để thêm property tên `aim`, giá trị `${'amqp$routingKey':startsWith('aim/')}`.
2. Copy processor `in ext: ...`. Ở bản sao, sửa `topic` = `t/vnm/vatm/dev/aim/${'amqp$routingKey':substringAfter('aim/')}`.
3. Nối `in: match Bridge Rule` (relationship `aim`) vào bản sao, rồi nối bản sao (`success`) vào `Publish to Solace topic`. Start bản sao.

### Lưu flow sau khi sửa

NiFi tự lưu flow vào volume. Để repo luôn có bản mới nhất: chuột phải vào group → **Download Flow Definition** → **Without External Services**, rồi ghi đè `nifi/bridge-flow.json` và commit. File này không chứa mật khẩu.

## Vận hành

Xem log:

```
podman logs -f vatm-bridge
```

Dừng hoặc chạy lại Bridge (flow và message đang chờ vẫn còn):

```
podman stop vatm-bridge
podman start vatm-bridge
```

Lên phiên bản NiFi mới: xoá container cũ, rồi chạy lại lệnh ở bước 3 với tag mới. Các volume vẫn giữ flow và mật khẩu.

```
podman rm -f vatm-bridge
podman run ... docker.io/apache/nifi:<phiên bản mới>
```

Message Outbound không vào được queue nào: nếu không queue nào bind với routing key, RabbitMQ từ chối message (`NO_ROUTE`). Khi đó `Publish to RabbitMQ x.swim.dev.bridge.out` thử gửi lại mỗi giây, ghi lỗi `NO_ROUTE` vào log và hiện ô đỏ. Message không mất, nó chờ ở connection `failure` vòng quanh processor đó. Cách xử lý:
- Bind một queue với routing key đó, message sẽ đi ngay.
- Hoặc, nếu message không cần nữa: chuột phải vào connection vòng đó → **List Queue** để xem, rồi **Empty Queue** để bỏ.

Gỡ hẳn Bridge (mất flow và các message đang chờ trong NiFi):

```
podman rm -f vatm-bridge
podman volume rm vatm-bridge-conf vatm-bridge-state vatm-bridge-database vatm-bridge-flowfile vatm-bridge-content vatm-bridge-provenance
```
