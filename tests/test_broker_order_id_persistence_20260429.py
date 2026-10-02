# coding=utf-8
# ======================================
# File: test_broker_order_id_persistence_20260429.py
# Author: Jackie PENG
# Contact: jackie.pengzhao@gmail.com
# Created: 2026-05-21
# Desc:
# Regression for broker 5-tuple transaction results and broker_order_id
# in raw_trade_result (S0-2 / PR-0a).
# ======================================

from __future__ import annotations

import unittest

from qteasy.broker import Broker


_VALID_ORDER = {
    'order_id': 101,
    'pos_id': 6,
    'direction': 'buy',
    'order_type': 'market',
    'qty': 100.0,
    'price': 1.23,
    'status': 'submitted',
    'submitted_time': '2026-04-29 13:21:10',
    'symbol': '513100.SH',
    'position': 'long',
}


class _BrokerTestMixin:
    """避免 Broker._parse_order 访问真实 DB。"""

    def _parse_order(self, order):
        return (
            order['order_type'],
            order.get('symbol', '513100.SH'),
            float(order['qty']),
            float(order['price']),
            order['direction'],
            order.get('position', 'long'),
        )


class _FiveTupleBroker(_BrokerTestMixin, Broker):
    def __init__(self):
        super().__init__(data_source=None)
        self.broker_name = 'XtQuantBroker'
        self.register(debug=False)
        self.connect()

    def transaction(self, symbol, order_qty, order_price, direction, position='long', order_type='market'):
        yield ('filled', order_qty, order_price, 1.0, '109000001')


class _FourTupleBroker(_BrokerTestMixin, Broker):
    def __init__(self):
        super().__init__(data_source=None)
        self.broker_name = 'LegacyBroker'
        self.register(debug=False)
        self.connect()

    def transaction(self, symbol, order_qty, order_price, direction, position='long', order_type='market'):
        yield ('filled', order_qty, order_price, 1.0)


class TestBrokerOrderIdPersistence20260429(unittest.TestCase):
    def test_five_tuple_get_result_persists_broker_order_id(self):
        print('\n[TestBrokerOrderIdPersistence] _get_result 写入 broker_order_id')
        broker = _FiveTupleBroker()
        broker._get_result(dict(_VALID_ORDER))
        result = broker.result_queue.get()
        print(' raw_trade_result:', result)
        self.assertEqual(result['broker_order_id'], '109000001')
        self.assertEqual(result['filled_qty'], 100.0)

    def test_five_tuple_submit_poll_fills_persists_broker_order_id(self):
        print('\n[TestBrokerOrderIdPersistence] submit -> poll_fills broker_order_id')
        broker = _FiveTupleBroker()
        broker.submit(dict(_VALID_ORDER))
        fills = broker.poll_fills()
        print(' fills:', fills)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]['broker_order_id'], '109000001')

    def test_four_tuple_legacy_broker_still_works(self):
        print('\n[TestBrokerOrderIdPersistence] 4-tuple legacy broker 仍可用')
        broker = _FourTupleBroker()
        broker._get_result(dict(_VALID_ORDER))
        result = broker.result_queue.get()
        print(' raw_trade_result:', result)
        self.assertEqual(result['filled_qty'], 100.0)
        self.assertNotIn('broker_order_id', result)

        broker2 = _FourTupleBroker()
        returned_boid = broker2.submit(dict(_VALID_ORDER))
        fills = broker2.poll_fills()
        print(' submit returned_boid:', returned_boid, ' fills:', fills)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]['broker_order_id'], returned_boid)


if __name__ == '__main__':
    unittest.main()
