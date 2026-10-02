模拟实盘交易（Live Trading）
============================

亲爱的用户，欢迎阅读 qteasy 的**模拟实盘**文档。

> **模拟实盘是什么？**  
> 回测是在历史数据上「快进」验证策略；模拟实盘（配置 ``mode=0``）则让策略在**真实时间**里运行——像真的在盯盘一样按日程算信号、下单、记日志，但成交通常由本地**模拟券商**完成，不涉及真实资金。  
> 这样您可以在投入真钱之前，先体验「回测之外、实盘之前」的那一段路，并尽量与回测使用**同一套**策略逻辑，减少「回测很好、实盘走样」的落差。

**若您尚未完成回测或第一次使用 qteasy**，建议先阅读 :doc:`../getting_started` 与 :doc:`../tutorials/3-start-first-strategy`，再回到本模块。

本模块帮您回答：

- 如何配置并启动模拟实盘
- 风控与订单从提交到成交的全过程
- 日志与产物在哪里、出问题先查什么
- （进阶）如何扩展券商适配、如何做运维冒烟

读者分层
--------

- **初学者**：按下方「建议阅读顺序」读 1 → 2 → 3 → 5 即可跑通并会排错。
- **已跑通 live**：可查阅第 6、8 章做快照/门禁与 CLI 运维。
- **准备对接真实柜台（如 QMT）**：阅读 :doc:`4a-xtquant-broker-adapter-contract-v1`（英文契约 v1），再读 :doc:`4-broker-adapter-and-integration`。

建议阅读顺序
------------

与下方目录顺序一致：

1. :doc:`1-overview` — 总览与术语速查
2. :doc:`2-configuration-and-run` — 配置与启动
3. :doc:`3-risk-and-order-lifecycle` — 风控与订单状态
4. :doc:`5-artifacts-and-troubleshooting` — 产物与排错（建议先于 Broker 集成阅读）
5. :doc:`4-broker-adapter-and-integration` — 券商适配（进阶）
6. :doc:`4a-xtquant-broker-adapter-contract-v1` — XtQuant/MiniQMT 契约 v1（英文，协作必读）
7. :doc:`6-trader-snapshot-gate` — 策略快照与启动门禁
8. :doc:`7-manual-smoke-live-grid-roadmap` — 手工冒烟清单
9. :doc:`8-cli-trader-capability-matrix` — CLI 能力对照表

.. toctree::
   :maxdepth: 1

   1-overview
   2-configuration-and-run
   3-risk-and-order-lifecycle
   5-artifacts-and-troubleshooting
   4-broker-adapter-and-integration
   4a-xtquant-broker-adapter-contract-v1
   6-trader-snapshot-gate
   7-manual-smoke-live-grid-roadmap
   8-cli-trader-capability-matrix
