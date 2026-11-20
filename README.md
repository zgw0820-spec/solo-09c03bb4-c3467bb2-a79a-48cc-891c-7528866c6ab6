# 机房救援镜像 TFTP 分发

给无法从本地盘启动的机器分发一份固定救援镜像。Python asyncio 实现，
`docker compose` 一键启动两个服务：

| 服务 | 端口 | 说明 |
|---|---|---|
| `tftp-server` | UDP 69（宿主映射 6969/udp） | asyncio TFTP 服务端，只服务固定镜像 `rescue.img`（octet） |
| `api` | TCP 8000 | FastAPI。打开 **http://localhost:8000/docs** 在 Swagger 里 `POST /download` 发起一次**真实 UDP TFTP 下载**，可模拟丢包，返回收到镜像的 SHA-256 并比对 |

## 启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/docs -> POST /download -> Try it out
```

`/download` 参数（都有默认值，可直接 Execute）：

- `window`：请求的 RFC 7440 windowsize（服务端最多协商到 16）
- `loss`：模拟丢弃入向 DATA 的概率（0–0.95）
- `ack_loss`：模拟丢弃出向 ACK 的概率（0–1，1.0 可演示"重试用尽释放端口"）
- `drop_first_oack`：丢掉第一个 OACK，迫使客户端重发 RRQ（验证"原传输端口应答"）
- `seed` / `timeout` / `max_retries`：丢包随机种子与超时重试参数

`GET /image/info` 返回镜像大小、块数、预期 SHA-256。

镜像由 `(IMAGE_SEED, IMAGE_SIZE)` 确定性生成，两个容器用同一组环境变量，
因此 API 能独立算出预期 SHA-256 进行核对：

```bash
# 默认 131072（256*512，恰好整除 -> 以空块收尾）
docker compose up --build
# 触发块号 65535 -> 0 回绕的尺寸（65540*512 = 33556480）
IMAGE_SIZE=33556480 docker compose up --build
```

## 协议行为（对应需求）

- **octet 下载、每块 512 字节**：`blksize` 一律协商回 512。
- **RFC 7440 窗口协商，最多 16 块**：`windowsize` 取 `min(请求值, 16)`，OACK 应答。
- **协商答复丢失，重发 RRQ 由原传输端口应答**：69 端口按客户端地址登记会话；
  重复 RRQ 转交原会话，从**原传输端口**重发 OACK（不开新会话）。
- **各台机器独立累计确认**：每客户端一个会话、一个独立 UDP 端口、独立的
  累积 ACK/窗口状态，互不影响。
- **超时只重发未确认的块**：重传区间严格为 `[send_base, last_sent]`，
  即窗口内尚未被累积确认的部分。
- **重试用尽释放端口**：超过 `TFTP_MAX_RETRIES` 次无进展即关闭会话 socket、
  注销会话并记日志。
- **块号 65535 -> 0 回绕**：绝对块号取模 65536 得到线上块号，回绕后按序继续。
- **长度为块长整数倍时空块收尾**：总块数 = `size // 512 + 1`，整除时最后一块
  为 0 字节 DATA。

## 本地验证（无需 Docker）

```bash
python3 tests/e2e.py     # 48 项检查：基本/短尾块/丢包重传/重复 RRQ/重试耗尽/并发/回绕
```

## 布局

```
common/protocol.py     共享线格式：操作码、块号回绕、RRQ/OACK/DATA/ACK 编解码
common/imagegen.py     确定性镜像生成（seed+size -> bytes / sha256）
server/tftp_server.py  asyncio TFTP 服务端（会话=独立传输端口）
api/tftp_client.py     asyncio TFTP 客户端（丢包模拟 + 统计）
api/main.py            FastAPI：/docs、/image/info、/download
tests/e2e.py           端到端验证脚本
Dockerfile.server / Dockerfile.api / docker-compose.yml
```

## 已知限制

- 只服务一个固定镜像、只支持 RRQ（无 WRQ），这是有意为之。
- 客户端按序接收、丢弃乱序块（累计确认语义下的合法简化，丢包时由服务端重传补齐）。
- 未实现 RFC 2347 选项之外的扩展（多播、邮箱等）；`tsize` 请求会被应答。
