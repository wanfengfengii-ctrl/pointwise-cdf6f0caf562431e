# 青湾站日交班湖

供数站的观测以整数千克报送。日班冻结原批，核账员把错归行转到同一天的正确站，发行员按区站订阅分路出件。隔离区站在源仓关闭后继续收件，接班和审计人员凭旧账号或新账号核对原据。

## 运行

需要 Python 3.11 和标准库。控制台物化时提供 `check_project.py`。在项目根目录执行完整回归和演示。

```sh
python3 -B check_project.py
python3 -B -m tests.test_demo
```

`samples/station-day.json` 给出东站的 7 kg、显式零和缺测各一行，西站为空。`samples/dispatch.json` 给出正式转记和订阅。演示打开独立临时库，正式转记 7 kg，然后关闭源仓。双站先到东片时西站仍待收，旧账为东 7、西 0。仅东区站此时可以结清为东 0。双站补到西片后一次发布东 0、西 7。演示逐一核对旧账、新账、来源和首执，并输出一条交班场景记录。

## Python 接口

使用 `audit.snapshots.connect_database(path)` 打开连接，操作后由调用者关闭。源侧调用 `lake.opening.initialize_source(db)`，隔离侧调用 `receiver.posting.initialize_receiver(db)`。初始化只补齐表结构。下列写入入口各自管理事务，调用时连接应处于空闲状态。底层写入函数由这些入口在同一事务内使用。

| 工作 | 调用 | 结果 |
| --- | --- | --- |
| 建立空原账 | `lake.opening.open_day(db, day, stations)` | 已发布的 `PublishedBook`。同日同站集返回原开账成果。 |
| 入湖 | `lake.intake.accept_batch(db, day, station, batch_id, rows)` | `FrozenBatch` 含原账号。每行格式为 `{'kg': 7, 'origin': 'sensor-17/1'}`，`kg` 可为 `None`。 |
| 正式转记 | `lake.transfers.commit_transfer(db, transfer_id, day, reading_id, before, after)` | `FormalTransfer` 含提交序、原读数和前后站。`lake.source_audit.read_transfer_postings` 读取实际落地的有向分录。 |
| 冻结订阅 | `exchange.subscriptions.register_subscription(db, subscription_id, day, stations)` | `FrozenSubscription`。站集输入顺序和重复站名不改变站集。 |
| 安装隔离底账 | `receiver.posting.install_opening_link(local_db, subscription, opening_book)` | 对应原账在该订阅内的底账号。重复安装同一个底账保留实收历史。 |
| 发行或重领 | `exchange.issue.issue_transfer(db, subscription_id, transfer_id)` 或 `load_issue(db, subscription_id, transfer_id)` | `IssuedDelivery` 含 `manifest` 和按站排序的 `(站号, bytes)` 分路材料。空投影保留发行登记。 |
| 实收 | `receiver.receive.receive_packet(local_db, subscription_id, bytes)` | 缺片时返回 `PendingDelivery`，用 `transfer_id` 和 `missing_stations()` 交班。结清或精确重投时返回 `FirstReceipt`，其中 `result_id` 是联账号。 |
| 按号核账 | `audit.handover.collect_handover(db, result_id)` | `book` 记录冻结范围、转记号和适用首执号，`provenance` 区分当前原据与有向变更，`accounts` 含各站重量、缺测数及净变化。 |

`lake.source_ledger.read_source_station_book` 使用开账读数及已落地分录核对当前源账。`receiver.posting.current_linked_result` 返回区站当前可领账号。`receiver.pending.load_pending` 和 `receiver.receipts.load_first_receipt` 可分别查询指定订阅转记的待片和首执，未知记录返回 `None`。

## 数据和错误语义

- 日期为 `YYYY-MM-DD`，标识为无首尾空白的非空文本。每个报送批至少一行，行位置从一开始。批号在源库唯一，读数身份还记录计量日和报送原站。千克以 Python 整数计算，布尔值和浮点数不作为重量。缺测以 `None` 保存并单独计数。有向分录对重量和缺测数使用同一方向。
- 原账保存当时已验收的报送明细，正式转记保留原值、出处和行身份。领取站账时按批号、行位置排序。区站底账选用包含其本地待核读数的同日原账，底账固定后源仓新增原批仍留在源仓。正式站片只搬运对应转记。关联原行的连续转记按订阅内可见前序推进，独立原行可各自办理。
- 每个站片为版本 1 的规范 JSON 字节，携带冻结清册。清册内容标识和站片摘要用 SHA-256 绑定确切范围和正文，收件人在断源时使用本地登记对照材料。材料由业务发行端经既定搬运渠道提供，摘要用于身份及一致性校验。重领原发行返回原字节。空投影无实收件。
- 非法输入、冲突重投、无法应用的过账和错件以 `ValueError` 表示，未知登记或发行查询以 `KeyError` 表示，未知或未发布成果以 `audit.lookup.UnknownResult` 表示。SQLite 写入失败原样报错，所在账务事务回滚。待片、拒收和失败提交都保留已发布历史。合法片重投可跨重开复领原首执。
- 订阅固定日和站集。动态改订、跨日转记及分支合账在当前工作流范围之外。
