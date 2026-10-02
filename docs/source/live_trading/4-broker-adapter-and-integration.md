# 券商适配层与集成

> 本章面向准备对接新券商（或未来 QMT）的用户与开发者：模拟券商之上的一层「统一柜台接口」长什么样、如何扩展。

**读者说明**：若您刚入门模拟实盘，读完 :doc:`2-configuration-and-run` 与 :doc:`3-risk-and-order-lifecycle` 即可；本章可**暂时跳过**。当您需要接入真实柜台或自定义 Broker 时再回来阅读。

亲爱的用户，在 qteasy 里，**Broker（券商）** 负责「收单 → 告诉您是否受理 → 异步推送成交」。默认的 **simulator** 用 live 价模拟这一切；我们设计了统一适配层，以便日后换成 QMT 等真实柜台时，Trader 主体逻辑尽量不用改。

## 0. 适用场景

- 您希望对接新的券商，并与现有 Trader 逻辑兼容  
- 您需要先打通「提交 → 受理结果 → 成交回报 → 状态更新」最小闭环  

## 1. 核心概念：分层结构

典型 live 调用链（由外到内）：

```text
Operator.run_live_trade()
  → Trader（日程、风控、账本）
       → BrokerFacade（券商适配外壳：统一 API）
            → SimulatorBroker / 您注册的 Broker
```

**BrokerFacade（适配外壳）**：像给不同券商柜台套上的**统一翻译层**——Trader 只跟 Facade 说「connect / submit /  poll_fills」，不必关心底层是模拟还是 QMT。

要点：

- Facade 对外提供 `connect` / `disconnect` / `submit_with_ack` / `poll_fills` / `get_remote_*` 等  
- CLI 中 `broker status|connect|disconnect` 反映 **`is_connected`**（simulator 上表示「适配层已准备好」，**不是**真实券商登录）  
- Trader 主循环通过 **`poll_fills`** 消费成交回报，而不是直接掏内部队列  

## 2. 核心接口（用户/开发者向）

**Broker 适配层**是 Trader 与具体券商（simulator、未来的 QMT 等）之间的统一接口。无论底层是谁，Trader 都用同一套「连接 → 递单 → 听受理 → 轮询成交 → 必要时查远端账本」流程。下表先给出**接口名与日常类比**，便于阅读后文对照表与接入清单。

**各列含义**：**接口**为适配层方法名；**做什么**为在 live 链路中的职责；**您可以理解为**为帮助记忆的比喻。

**如何使用**：接入新券商时，按 §6 清单逐项实现；日常运维只需关心 `connect`、`submit_with_ack`、`poll_fills` 与 CLI `broker status`。

| 接口 | 做什么 | 您可以理解为 |
|------|--------|----------------|
| `connect` / `disconnect` | 建立/断开适配层会话 | 柜台窗口开/关 |
| `is_connected` | 是否已 connect | 窗口是否开着 |
| `submit_with_ack` | 递单并**同步**得到受理与否 | 递单后立即听「收/不收」 |
| `cancel` | 撤单 | 撤销未成交委托 |
| `poll_fills` | 取异步成交回报 | 问「有没有新成交」 |
| `get_remote_*` | 查远端现金/持仓/订单 | 与券商账本对账（simulator 多为占位） |

### 2.1 接口职责对照

同一接口在 **simulator** 与 **真实 QMT** 上行为深度不同：前者用 live 价模拟，后者对接柜台。下表帮助您建立预期——避免把 simulator 上的 `connect` 当成真实登录，也便于规划 QMT 接入时要补的能力。

**各列含义**：**接口**同 §2；**语义**为抽象职责；**simulator 上您会看到**为当前默认模拟行为；**真实 QMT（规划中）** 为后续目标（非当前版本承诺）。

**如何使用**：排错时先看 simulator 列是否已满足「受理 + 回报」；规划 QMT 时以最后一列为目标能力勾选。

| 接口 | 语义 | simulator 上您会看到 | 真实 QMT（规划中） |
|------|------|----------------------|---------------------|
| `connect` / `disconnect` | 会话 | 内部标志置位；非真实登录 | 真实登录与重连 |
| `submit_with_ack` | 同步受理 | 返回 accepted、模拟 broker_order_id 等 | 柜台真实受理结果（见 :doc:`4a-xtquant-broker-adapter-contract-v1`） |
| `poll_fills` | 异步回报 | 模拟成交队列 | 真实成交推送/轮询 |
| `get_remote_*` | 远端账本 | 常为空或 None | 对账、门禁、sync 的数据源 |
| `cancel` | 撤单 | 模拟实现 | 真实撤单号映射 |

建议按「最小闭环」理解优先级：

1. 连接：`connect` / `is_connected`  
2. 交易：`submit_with_ack` / `cancel`  
3. 回报：`poll_fills`  
4. 查询：`get_remote_*`（对接真实柜台后才具备完整业务意义）  

## 3. submit_with_ack 与本地订单

**与风控拒单不同**：风控在更前面，**不入订单表**（见 :doc:`3-risk-and-order-lifecycle`）。

`submit_with_ack` 同步返回券商是否**受理**委托。下表仅描述**受理阶段**本地订单与 `broker_order_id` 的对应关系；成交进度见 :doc:`3-risk-and-order-lifecycle` 状态表。

**各列含义**：**受理结果**为 ACK 中的 accepted 语义；**本地订单**为订单表状态走向；**broker_order_id** 为是否回写券商委托号。

**如何使用**：订单 `rejected` 且 broker 号为空 → 查「accepted=False」行；有 broker 号但长期不成交 → 查状态表与 `poll_fills`。

| 受理结果 | 本地订单 | broker_order_id |
|----------|----------|-----------------|
| `accepted=True` | 继续流转 | **回写**券商号与名称 |
| `accepted=False` | 常为 `rejected` | **保持空** |

详细对照见 :doc:`6-trader-snapshot-gate`。

提交前订单结构与回报字段均经契约校验，格式不对时会尽早报错，避免「脏数据进账本」。

## 4. 扩展新 Broker 类型

**XtQuant / MiniQMT**：配置项 ``live_trade_broker_type='xtquant'`` 已在 2.5.2 白名单中；实现细节与禁止双重下单等规则见英文契约 :doc:`4a-xtquant-broker-adapter-contract-v1`（协作方 PR 评审基准）。

注册工厂（示例）：

```python
from qteasy.broker import register_broker_factory

register_broker_factory('my_broker', MyBrokerFactory)
# qt.configure(live_trade_broker_type='my_broker')
```

独立扩展包（推荐用于 QMT）：

```python
import qteasy_xtquant

qteasy_xtquant.register()
# qt.configure(live_trade_broker_type='xtquant')
```

**CLI `sync`（别名 `pull-state`）**：当前为**预留**，执行会打印 `[NOT_IMPLEMENTED] ...`，表示真实「从券商拉状态」尚未实现；对接 QMT 后将替换为可用实现。

## 5. 边界行为（接入时请注意）

- 未 `connect` 就 `submit` / `poll_fills`：应得到明确错误，而非静默失败  
- `cancel` 未知委托号：应稳定返回「未撤成」  
- `poll_fills` 超时：应返回空列表，而非脏数据  
- simulator 上 **`connect` 不代表真实券商会话**，仅表示适配层可受理  

## 6. 最小接入清单

1. 实现 `connect` / `disconnect` / `is_connected`  
2. 打通 `submit_with_ack` → `poll_fills`  
3. 回报字段满足契约校验  
4. 实现 `cancel` 与异常路径  
5. （若需门禁/对账/sync）实现 `get_remote_*`  
6. 在 CLI 下验证：`broker status`、`reconcile`、订单映射  

## 7. 最小验收标准

- [ ] `connect` 后 `is_connected=True`，可 `submit_with_ack`  
- [ ] 受理成功：本地订单含 `broker_order_id`  
- [ ] 受理拒单：`rejected` 且 broker 字段空  
- [ ] `poll_fills` 回报可校验  
- [ ] 未连接 / 未知单 / 超时行为稳定  
- [ ] CLI `broker` / `reconcile` 可观测  

## 8. 相关跳转

- 生命周期与风控：:doc:`3-risk-and-order-lifecycle`  
- 产物与排错：:doc:`5-artifacts-and-troubleshooting`  
- 快照/门禁：:doc:`6-trader-snapshot-gate`  
- CLI 对照：:doc:`8-cli-trader-capability-matrix`  
- 设计说明：[design/10-live-trading-s1.3-architecture.md](../design/10-live-trading-s1.3-architecture.md)  
