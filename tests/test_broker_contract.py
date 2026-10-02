# coding=utf-8
# ======================================
# File: test_broker_contract.py
# Author: Jackie PENG
# Contact: jackie.pengzhao@gmail.com
# Created: 2026-04-13
# Desc:
# Unittest for Broker adapter contract (S1.3 P6)
# ======================================

import unittest
import threading
import time
from unittest.mock import patch

from qteasy.broker import (
    Broker,
    BrokerFacade,
    SimpleBroker,
    SimulatorBroker,
    get_broker,
    register_broker_factory,
    unregister_broker_factory,
)
from qteasy.trade_io import validate_raw_trade_result


class MinimalBrokerForContractTest(Broker):
    """用于契约测试的最小可实例化 Broker。"""

    def __init__(self):
        super().__init__(data_source=None)
        self.broker_name = 'MinimalBroker'

    def _parse_order(self, order):
        """避免访问数据库，直接从订单字典读取字段。"""
        return (
            order['order_type'],
            order.get('symbol', '000001.SH'),
            float(order['qty']),
            float(order['price']),
            order['direction'],
            order.get('position', 'long'),
        )

    def transaction(self, symbol, order_qty, order_price, direction, position='long', order_type='market'):
        half_qty = round(float(order_qty) / 2, 4)
        remain_qty = round(float(order_qty) - half_qty, 4)
        yield 'partial-filled', half_qty, float(order_price), 2.5
        yield 'filled', remain_qty, float(order_price), 2.5


class LegacyMinimalBroker(Broker):
    """用于验证 legacy _get_result 路径金标准。"""

    def __init__(self):
        super().__init__(data_source=None)
        self.broker_name = 'LegacyMinimalBroker'

    def _parse_order(self, order):
        return (
            order['order_type'],
            order.get('symbol', '000001.SH'),
            float(order['qty']),
            float(order['price']),
            order['direction'],
            order.get('position', 'long'),
        )

    def transaction(self, symbol, order_qty, order_price, direction, position='long', order_type='market'):
        yield 'filled', float(order_qty), float(order_price), 5.0


class SlowAsyncBrokerForIdleWaitTest(Broker):
    """用于验证 wait_until_idle 的慢速异步 Broker。"""

    def __init__(self):
        super().__init__(data_source=None)
        self.broker_name = 'SlowAsyncBroker'

    def _parse_order(self, order):
        return (
            order['order_type'],
            order.get('symbol', '000001.SH'),
            float(order['qty']),
            float(order['price']),
            order['direction'],
            order.get('position', 'long'),
        )

    def transaction(self, symbol, order_qty, order_price, direction, position='long', order_type='market'):
        time.sleep(0.3)
        yield 'filled', float(order_qty), float(order_price), 5.0


class TestBrokerContract(unittest.TestCase):

    def setUp(self) -> None:
        self.order = {
            'order_id': 1,
            'pos_id': 1,
            'direction': 'buy',
            'order_type': 'limit',
            'qty': 100.0,
            'price': 10.0,
            'status': 'submitted',
            'submitted_time': '2026-04-13 09:30:00',
            'symbol': '000001.SH',
            'position': 'long',
        }

    def test_adapter_methods_exist_on_simulator_and_simple(self):
        print('\n[TestBrokerContract] 检查 Simulator/Simple 新接口存在性')
        for broker in [SimulatorBroker(reject_submit_probability=0.0), SimpleBroker()]:
            for method_name in [
                'connect', 'disconnect', 'submit', 'submit_with_ack', 'cancel', 'poll_fills', 'poll_messages',
                'get_remote_orders', 'get_remote_positions', 'get_remote_cash', 'drain_order_queue',
            ]:
                print(f' broker={broker.broker_name}, method={method_name}')
                self.assertTrue(callable(getattr(broker, method_name)))
            self.assertIsNone(broker.connect())
            self.assertIsNone(broker.disconnect())

    def test_simulator_submit_with_ack_random_reject_branch(self):
        print('\n[TestBrokerContract] SimulatorBroker submit_with_ack 随机拒单分支（patch）')
        broker = SimulatorBroker(reject_submit_probability=0.1)
        broker.connect()
        fixed_reason = 'Order rejected: simulated test fixture reason.'
        with patch('qteasy.broker.random.random', return_value=0.05):
            with patch('qteasy.broker.random.choice', return_value=fixed_reason):
                ack = broker.submit_with_ack(self.order)
        print(' ack:', ack)
        self.assertFalse(ack['accepted'])
        self.assertEqual(ack['broker_order_id'], '')
        self.assertEqual(ack['reason_code'], 'SimulatedBrokerReject')
        self.assertEqual(ack['reason'], fixed_reason)
        self.assertEqual(ack['order_id'], self.order['order_id'])

    def test_simulator_submit_with_ack_accepts_when_roll_above_threshold(self):
        print('\n[TestBrokerContract] SimulatorBroker 随机拒单未触发时受理成功')
        broker = SimulatorBroker(reject_submit_probability=0.1)
        broker.connect()
        with patch('qteasy.broker.random.random', return_value=0.99):
            ack = broker.submit_with_ack(self.order)
        print(' ack:', ack)
        self.assertTrue(ack['accepted'])
        self.assertNotEqual(ack['broker_order_id'], '')
        self.assertEqual(ack['reason'], '')
        self.assertEqual(ack['reason_code'], '')

    def test_simulator_reject_probability_one_uses_reason_pool(self):
        print('\n[TestBrokerContract] 随机拒单理由取自预置英文池（概率=1）')
        from qteasy.broker import _SIMULATOR_SUBMIT_REJECT_REASONS
        broker = SimulatorBroker(reject_submit_probability=1.0)
        broker.connect()
        with patch('qteasy.broker.random.random', return_value=0.0):
            ack = broker.submit_with_ack(self.order)
        print(' ack reason:', ack.get('reason'))
        self.assertFalse(ack['accepted'])
        self.assertIn(ack['reason'], _SIMULATOR_SUBMIT_REJECT_REASONS)

    def test_simulator_reject_probability_zero_skips_random_reject(self):
        print('\n[TestBrokerContract] reject_submit_probability=0 跳过随机拒单')
        broker = SimulatorBroker(reject_submit_probability=0.0)
        broker.connect()
        with patch('qteasy.broker.random.random', return_value=0.01):
            ack = broker.submit_with_ack(self.order)
        print(' ack:', ack)
        self.assertTrue(ack['accepted'])

    def test_submit_returns_non_empty_broker_order_id(self):
        print('\n[TestBrokerContract] submit 返回非空 broker_order_id')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        first_id = broker.submit(self.order)
        second_id = broker.submit(self.order)
        print(' first_id:', first_id)
        print(' second_id:', second_id)
        self.assertIsInstance(first_id, str)
        self.assertGreater(len(first_id), 0)
        self.assertNotEqual(first_id, second_id)

    def test_poll_fills_each_item_passes_validate_raw_trade_result(self):
        print('\n[TestBrokerContract] poll_fills 返回 raw_trade_result 契约')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        broker_order_id = broker.submit(self.order)
        print(' broker_order_id:', broker_order_id)
        all_fills = []
        all_fills.extend(broker.poll_fills())
        all_fills.extend(broker.poll_fills())
        print(' fills:', all_fills)
        self.assertGreaterEqual(len(all_fills), 1)
        for raw in all_fills:
            self.assertEqual(raw['order_id'], self.order['order_id'])
            self.assertGreaterEqual(raw['filled_qty'], 0)
            self.assertGreaterEqual(raw['canceled_qty'], 0)
            self.assertGreaterEqual(raw['transaction_fee'], 0)
            self.assertTrue(raw['delivery_status'])
            validate_raw_trade_result(raw, context='test.poll')

    def test_submit_then_poll_semantics_partial_then_done(self):
        print('\n[TestBrokerContract] submit 同步受理 + poll 异步分批')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        broker.submit(self.order)
        fills_round_1 = broker.poll_fills()
        fills_round_2 = broker.poll_fills()
        print(' round1:', fills_round_1)
        print(' round2:', fills_round_2)
        self.assertEqual(len(fills_round_1), 1)
        self.assertEqual(len(fills_round_2), 1)
        self.assertEqual(fills_round_1[0]['status'], 'partial-filled')
        self.assertEqual(fills_round_2[0]['status'], 'filled')
        total_filled = fills_round_1[0]['filled_qty'] + fills_round_2[0]['filled_qty']
        print(' total_filled:', total_filled, ' expected:', self.order['qty'])
        self.assertAlmostEqual(total_filled, self.order['qty'])
        self.assertEqual(fills_round_1[0]['price'], self.order['price'])
        self.assertEqual(fills_round_2[0]['price'], self.order['price'])

    def test_submit_with_ack_returns_structured_accept_result(self):
        print('\n[TestBrokerContract] submit_with_ack 返回结构化 accept 回报')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        ack = broker.submit_with_ack(self.order)
        print(' ack:', ack)
        self.assertIsInstance(ack, dict)
        self.assertTrue(ack['accepted'])
        self.assertEqual(ack['order_id'], self.order['order_id'])
        self.assertTrue(isinstance(ack['broker_order_id'], str) and ack['broker_order_id'])
        self.assertEqual(ack['reason'], '')

    def test_submit_with_ack_returns_structured_reject_result(self):
        print('\n[TestBrokerContract] submit_with_ack 返回结构化 reject 回报')
        broker = MinimalBrokerForContractTest()
        ack = broker.submit_with_ack(self.order)
        print(' ack:', ack)
        self.assertIsInstance(ack, dict)
        self.assertFalse(ack['accepted'])
        self.assertEqual(ack['order_id'], self.order['order_id'])
        self.assertEqual(ack['broker_order_id'], '')
        self.assertTrue(isinstance(ack['reason'], str) and ack['reason'])
        self.assertEqual(ack['reason_code'], 'RuntimeError')

    def test_legacy_queue_get_result_unchanged(self):
        print('\n[TestBrokerContract] legacy _get_result -> result_queue 路径')
        broker = LegacyMinimalBroker()
        broker._get_result(self.order)
        result = broker.result_queue.get()
        print(' legacy result:', result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['order_id'], self.order['order_id'])
        self.assertEqual(result['filled_qty'], self.order['qty'])
        self.assertEqual(result['price'], self.order['price'])
        self.assertEqual(result['transaction_fee'], 5.0)
        validate_raw_trade_result(result, context='test.legacy')

    def test_get_result_partial_fills_use_order_qty_not_shadowed(self):
        print('\n[TestBrokerContract] 分段成交 order_qty 不被循环内 qty 覆盖')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        order_qty = float(self.order['qty'])
        half_qty = round(order_qty / 2, 4)
        remain_qty = round(order_qty - half_qty, 4)
        print(' order_qty:', order_qty, ' expected fills:', half_qty, remain_qty)
        broker._get_result(self.order)
        first = broker.result_queue.get()
        second = broker.result_queue.get()
        print(' first fill:', first)
        print(' second fill:', second)
        self.assertEqual(first['filled_qty'], half_qty)
        self.assertEqual(second['filled_qty'], remain_qty)
        self.assertTrue(broker.result_queue.empty())

    def test_submit_rejects_invalid_order_dict(self):
        print('\n[TestBrokerContract] submit 拒绝非法订单')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        invalid_order = dict(self.order)
        invalid_order.pop('status')
        with self.assertRaises(ValueError) as cm:
            broker.submit(invalid_order)
        message = str(cm.exception)
        print(' error:', message)
        self.assertIn('Broker.submit.order', message)
        self.assertIn('missing required key', message)

    def test_submit_when_not_connected_raises_runtime_error(self):
        print('\n[TestBrokerContract] 未 connect 调用 submit')
        broker = MinimalBrokerForContractTest()
        with self.assertRaises(RuntimeError) as cm:
            broker.submit(self.order)
        message = str(cm.exception)
        print(' error:', message)
        self.assertIn('not connected', message)

    def test_enqueue_order_pushes_into_queue_with_validated_order(self):
        print('\n[TestBrokerContract] enqueue_order 入队并返回 order_id')
        broker = MinimalBrokerForContractTest()
        order_id = broker.enqueue_order(self.order)
        queued = broker.order_queue.get_nowait()
        broker.order_queue.task_done()
        print(' returned order_id:', order_id)
        print(' queued order keys:', sorted(queued.keys()))
        self.assertEqual(order_id, self.order['order_id'])
        self.assertEqual(queued['order_id'], self.order['order_id'])

    def test_poll_fills_when_not_connected_returns_empty_for_legacy_path(self):
        print('\n[TestBrokerContract] 未 connect 调用 poll_fills（legacy 空队列）')
        broker = MinimalBrokerForContractTest()
        fills = broker.poll_fills()
        print(' fills:', fills)
        self.assertEqual(fills, [])

    def test_poll_fills_reads_legacy_result_queue_without_connect(self):
        print('\n[TestBrokerContract] poll_fills 兼容 legacy result_queue 未 connect 路径')
        broker = MinimalBrokerForContractTest()
        raw_result = {
            'order_id': 777001,
            'filled_qty': 10.0,
            'price': 12.3,
            'transaction_fee': 0.5,
            'execution_time': '2026-05-11 09:31:00',
            'canceled_qty': 0.0,
            'delivery_amount': 0.0,
            'delivery_status': 'ND',
        }
        broker.result_queue.put(raw_result)
        fills = broker.poll_fills()
        print(' fills:', fills)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]['order_id'], raw_result['order_id'])
        self.assertTrue(broker.result_queue.empty())

    def test_poll_messages_returns_and_drains_one_message(self):
        print('\n[TestBrokerContract] poll_messages 返回并消费 broker_messages')
        broker = MinimalBrokerForContractTest()
        broker.send_message('hello from broker')
        polled_messages = broker.poll_messages()
        print(' polled_messages:', polled_messages)
        self.assertEqual(len(polled_messages), 1)
        self.assertIn('hello from broker', polled_messages[0])
        self.assertEqual(broker.poll_messages(), [])

    def test_connect_disconnect_idempotent(self):
        print('\n[TestBrokerContract] connect/disconnect 幂等')
        broker = MinimalBrokerForContractTest()
        self.assertFalse(broker.is_connected)
        broker.connect()
        self.assertTrue(broker.is_connected)
        broker.connect()
        self.assertTrue(broker.is_connected)
        broker.disconnect()
        self.assertFalse(broker.is_connected)
        broker.disconnect()
        self.assertFalse(broker.is_connected)
        broker.connect()
        self.assertTrue(broker.is_connected)
        broker_order_id = broker.submit(self.order)
        print(' broker_order_id after reconnect:', broker_order_id)
        self.assertTrue(broker_order_id.startswith('MinimalBroker:'))

    def test_cancel_unknown_broker_order_id(self):
        print('\n[TestBrokerContract] cancel unknown id 返回 False')
        broker = MinimalBrokerForContractTest()
        broker.connect()
        result = broker.cancel('no-such-id')
        print(' cancel result:', result)
        self.assertFalse(result)

    def test_get_remote_apis_stable_empty(self):
        print('\n[TestBrokerContract] remote 查询占位返回稳定类型')
        broker = MinimalBrokerForContractTest()
        orders = broker.get_remote_orders(account_id=1)
        positions = broker.get_remote_positions(account_id=1)
        cash = broker.get_remote_cash(account_id=1)
        print(' orders:', orders)
        print(' positions:', positions)
        print(' cash:', cash)
        self.assertIsInstance(orders, list)
        self.assertEqual(orders, [])
        self.assertIsInstance(positions, list)
        self.assertEqual(positions, [])
        self.assertIsNone(cash)

    def test_drain_order_queue_returns_all_fifo(self):
        print('\n[TestBrokerContract] drain_order_queue FIFO 排空')
        broker = MinimalBrokerForContractTest()
        payloads = [
            {'order_id': 1, 'symbol': '000001.SZ'},
            {'order_id': 2, 'symbol': '000002.SZ'},
            {'order_id': 3, 'symbol': '000003.SZ'},
        ]
        for item in payloads:
            broker.order_queue.put(item)
        drained = broker.drain_order_queue()
        print(' drained:', drained)
        self.assertEqual(len(drained), 3)
        self.assertEqual([it['order_id'] for it in drained], [1, 2, 3])
        self.assertTrue(broker.order_queue.empty())

    def test_drain_order_queue_idempotent_when_empty(self):
        print('\n[TestBrokerContract] drain_order_queue 空队列幂等')
        broker = MinimalBrokerForContractTest()
        first = broker.drain_order_queue()
        second = broker.drain_order_queue()
        print(' first:', first, ' second:', second)
        self.assertEqual(first, [])
        self.assertEqual(second, [])

    def test_wait_until_idle_waits_for_async_get_result(self):
        print('\n[TestBrokerContract] wait_until_idle 等待异步 _get_result 线程完成')
        broker = SlowAsyncBrokerForIdleWaitTest()
        broker.register(debug=True)
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        broker.order_queue.put(dict(self.order))

        start_ts = time.time()
        idle_ok = broker.wait_until_idle(timeout=2.0)
        elapsed = time.time() - start_ts
        print(f' wait_until_idle returned: {idle_ok}, elapsed: {elapsed:.3f}s')
        self.assertTrue(idle_ok)
        self.assertGreaterEqual(elapsed, 0.25)

        self.assertFalse(broker.result_queue.empty())
        raw_result = broker.result_queue.get()
        broker.result_queue.task_done()
        print(' raw_result:', raw_result)
        validate_raw_trade_result(raw_result, context='test.wait_until_idle')
        self.assertEqual(raw_result['order_id'], self.order['order_id'])
        self.assertEqual(raw_result['filled_qty'], self.order['qty'])
        self.assertEqual(raw_result['price'], self.order['price'])

        broker.status = 'stopped'
        broker_thread.join(timeout=1.0)


class TestBrokerRemoteReconcileContract(unittest.TestCase):
    """Broker 远端现金/持仓钩子契约（阶段 5-B L3 可选路径）。"""

    def test_minimal_broker_remote_cash_and_positions_are_unimplemented(self) -> None:
        print('\n[TestBrokerRemoteReconcileContract] default remote hooks')
        broker = MinimalBrokerForContractTest()
        cash = broker.get_remote_cash(account_id=1)
        pos = broker.get_remote_positions(account_id=1)
        print(' remote_cash:', cash, ' remote_positions:', pos)
        self.assertIsNone(cash)
        self.assertEqual(pos, [])


class TestBrokerFacadeAndRegistry(unittest.TestCase):
    """BrokerFacade 委托行为与注册表扩展回归。"""

    def setUp(self) -> None:
        self.order = {
            'order_id': 9001,
            'pos_id': 1,
            'direction': 'buy',
            'order_type': 'limit',
            'qty': 10.0,
            'price': 11.0,
            'status': 'submitted',
            'submitted_time': '2026-05-14 09:30:00',
            'symbol': '000001.SH',
            'position': 'long',
        }

    def test_broker_facade_delegates_contract_methods(self):
        print('\n[TestBrokerFacadeAndRegistry] facade 委托 submit/poll/status')
        inner = MinimalBrokerForContractTest()
        facade = BrokerFacade(inner)
        facade.connect()
        broker_order_id = facade.submit(self.order)
        fills_round_1 = facade.poll_fills()
        fills_round_2 = facade.poll_fills()
        print(' broker_order_id:', broker_order_id)
        print(' fills round1/round2:', fills_round_1, fills_round_2)
        self.assertTrue(broker_order_id.startswith('MinimalBroker:'))
        self.assertEqual(len(fills_round_1), 1)
        self.assertEqual(len(fills_round_2), 1)
        self.assertEqual(fills_round_1[0]['status'], 'partial-filled')
        self.assertEqual(fills_round_2[0]['status'], 'filled')
        facade.status = 'paused'
        print(' inner status after facade set:', inner.status)
        self.assertEqual(inner.status, 'paused')

    def test_register_broker_factory_then_get_broker(self):
        print('\n[TestBrokerFacadeAndRegistry] register_broker_factory 可扩展 get_broker')
        broker_name = 'contract_test_custom_broker'

        def _factory(**kwargs):
            _ = kwargs
            b = MinimalBrokerForContractTest()
            b.broker_name = 'ContractCustomBroker'
            return b

        print(' register broker name:', broker_name)
        register_broker_factory(broker_name, _factory)
        try:
            broker = get_broker(broker_name, params={})
            print(' broker type/name:', type(broker).__name__, broker.broker_name)
            self.assertIsInstance(broker, MinimalBrokerForContractTest)
            self.assertEqual(broker.broker_name, 'ContractCustomBroker')
        finally:
            removed = unregister_broker_factory(broker_name)
            print(' unregister removed:', removed)
            self.assertTrue(removed)

if __name__ == '__main__':
    unittest.main()
