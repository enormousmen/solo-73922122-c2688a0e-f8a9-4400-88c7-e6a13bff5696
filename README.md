# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status

# 查看回源熔断状态（state / 冷却剩余 / 探测键 / 在途填充数）
curl http://localhost:8080/origin/status
```

源站控制（换内容 / 延迟 / 断流 / 5xx / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 所有包请求直接返回 503（模拟源站 5xx 故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' -d '{"fail_status":503}'

# 恢复源站（fail_status 置 null）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' -d '{"fail_status":null}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源（受下述熔断约束）。
* **回源故障隔离**：连续 2 次因连接失败、源站 5xx 或传输中断导致填充失败后，
  代理打开熔断并冷却 5 秒。冷却期内，没有任何在途填充可加入的未命中请求
  立即返回 `503`（带 `Retry-After`），不会打到源站；完整缓存命中照常服务，
  已开始的填充及其同路径读者不受影响、正常读完。冷却期满后只放行一个未命中
  请求作为探测：探测在途时，其他新填充（含其他路径）返回 `503`，探测自身
  路径的新请求仍加入同一填充。完整填充成功、或源站返回 404（不缓存，视为
  源站可用）即恢复正常；探测失败则重新进入 5 秒冷却。失败判定只针对源站
  原因（`ClientConnectionError` / `ClientPayloadError` / 读超时 / 5xx），
  本地磁盘错误不计入；熔断状态见 `GET /origin/status`。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。熔断状态仅在进程内，重启后为关闭状态。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越，以及回源熔断：连续 5xx / 连接拒绝 / 传输中断开路、冷却期
`503` 且零回源、冷却期满单探测、探测在途阻断其他新填充、探测失败重新冷却、
完整填充或 404 恢复、成功清零连续计数、开路期间缓存命中与在途填充不受影响。
`tests/run_server.py` 是测试专用启动器（本沙箱禁止进程间信号，用控制文件代替
SIGTERM 停服），交付应用本身不含测试钩子。
