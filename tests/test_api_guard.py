# coding=utf-8
# ======================================
# File:     test_api_guard.py
# Desc:
#   api_guard 的离线测试：假接口持有一份已知的完整数据，每次请求最多返回 limit 行，模拟真实接口的截断。
#   guard 拉回的数据必须与完整数据一行不差，否则报错；不发任何真实请求。
#   见 docs/DOWNLOAD_DESIGN.md #2。
# ======================================

import random
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from qteasy import api_guard
from qteasy.api_guard import guard

FMT = '%Y%m%d'


class FakeApi:
    """假接口：data 为完整数据(date, id, ts_code)，每次请求最多返回 limit 行。

    truncate 为截断时返回哪些行：
    - 'newest'  : 区间内最新的 limit 行(tushare、FMP 价格类)
    - 'oldest'  : 区间内最旧的 limit 行
    - 'outside' : 混入区间外的行，共 limit 行(FMP 分红日历)
    """

    def __init__(self, data, limit, truncate='newest', as_list=False):
        self.data = data.sort_values('date').reset_index(drop=True)
        self.limit = limit
        self.truncate = truncate
        self.as_list = as_list
        self.requests = []

    def __call__(self, start, end, ts_code=None):
        self.requests.append((start, end, ts_code))
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        rows = self.data[(self.data['date'] >= s) & (self.data['date'] <= e)]
        if ts_code is not None:
            rows = rows[rows['ts_code'] == ts_code]
        if len(rows) > self.limit:
            if self.truncate == 'newest':
                rows = rows.tail(self.limit)
            elif self.truncate == 'oldest':
                rows = rows.head(self.limit)
            elif self.truncate == 'outside':
                outside = self.data[self.data['date'] > e].head(self.limit // 4)
                rows = pd.concat([outside, rows.tail(self.limit - len(outside))])
        if self.as_list:
            return rows.to_dict('records')
        return rows.reset_index(drop=True)


def make_data(days, rows_per_day, start='20000101', ts_code='A', seed=0):
    """生成完整数据：days 天里，每天的行数由 rows_per_day(rng, day) 决定"""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start, periods=days, freq='D')
    counts = [rows_per_day(rng, i) for i in range(days)]
    date_col = np.repeat(dates, counts)
    return pd.DataFrame({'date': date_col, 'id': np.arange(len(date_col)), 'ts_code': ts_code})


def ids_of(res):
    if isinstance(res, list):
        return [r['id'] for r in res]
    return list(res['id'])


class TestGuardDateRange(unittest.TestCase):
    """{start, end} / {ts_code, start, end}：截断时切日期，结果与完整数据一致"""

    def setUp(self):
        api_guard.reset()
        api_guard.configure_throttle(0, 0)

    def assertComplete(self, api, start, end, res, allow_extra=False):
        """res 必须含 [start, end] 内的全部行、不重复；allow_extra 时允许混入区间外的行"""
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        expected = set(api.data.loc[(api.data['date'] >= s) & (api.data['date'] <= e), 'id'])
        got = ids_of(res)
        got_in_range = [i for i in got if i in expected]
        self.assertEqual(set(got_in_range), expected, 'missing rows')
        self.assertEqual(len(got_in_range), len(set(got_in_range)), 'duplicated rows')
        if not allow_extra:
            self.assertEqual(len(got), len(got_in_range), 'rows outside range')

    def run_guard(self, api, start, end, name='t'):
        return guard(name, dict(start=start, end=end), lambda start, end: api(start, end), fmt=FMT)

    def test_no_truncation(self):
        """总行数小于 limit：一次请求，原样返回"""
        api = FakeApi(make_data(50, lambda r, i: 1), limit=1000)
        res = self.run_guard(api, '20000101', '20000219')
        self.assertComplete(api, '20000101', '20000219', res)
        self.assertEqual(len(api.requests), 1)

    def test_truncate_newest(self):
        api = FakeApi(make_data(3000, lambda r, i: 3), limit=500, truncate='newest')
        res = self.run_guard(api, '20000101', '20080401')
        self.assertComplete(api, '20000101', '20080401', res)

    def test_truncate_oldest(self):
        api = FakeApi(make_data(3000, lambda r, i: 3), limit=500, truncate='oldest')
        res = self.run_guard(api, '20000101', '20080401')
        self.assertComplete(api, '20000101', '20080401', res)

    def test_truncate_with_rows_outside_range(self):
        """截断时混入区间外的行：区间内的行必须取全；区间外的行由下载函数过滤"""
        api = FakeApi(make_data(3000, lambda r, i: 3), limit=500, truncate='outside')
        res = self.run_guard(api, '20000101', '20050101')
        self.assertComplete(api, '20000101', '20050101', res, allow_extra=True)

    def test_sparse(self):
        """数据集中在少数几天，其余为空(公告类)"""
        hot = {10, 11, 500, 501, 502, 1500}
        api = FakeApi(make_data(2000, lambda r, i: 80 if i in hot else 0), limit=200)
        res = self.run_guard(api, '20000101', '20050623')
        self.assertComplete(api, '20000101', '20050623', res)

    def test_exactly_limit_not_truncated(self):
        """行数正好等于 limit 但没有截断"""
        api = FakeApi(make_data(500, lambda r, i: 1), limit=500)
        res = self.run_guard(api, '20000101', '20010514')
        self.assertComplete(api, '20000101', '20010514', res)

    def test_one_day_exceeds_limit_raises(self):
        """单个时点就超过 limit：最小单元超限，必须报错"""
        api = FakeApi(make_data(400, lambda r, i: 300 if i == 200 else 1), limit=250)
        with self.assertRaises(RuntimeError):
            self.run_guard(api, '20000101', '20010204')

    def test_learned_limit_reused(self):
        """第一次请求学到 limit 后，后续区间仍完整"""
        api = FakeApi(make_data(4000, lambda r, i: 2), limit=600)
        for start, end in (('20000101', '20030101'), ('20030102', '20101231')):
            res = self.run_guard(api, start, end)
            self.assertComplete(api, start, end, res)

    def test_other_fields_passed_unchanged(self):
        """切分时只切日期，ts_code 原样传"""
        data = pd.concat([make_data(2000, lambda r, i: 1, ts_code='A'),
                          make_data(2000, lambda r, i: 1, ts_code='B')], ignore_index=True)
        data['id'] = np.arange(len(data))
        api = FakeApi(data, limit=300)
        res = guard('t', dict(ts_code='A', start='20000101', end='20050623'),
                    lambda ts_code, start, end: api(start, end, ts_code=ts_code), fmt=FMT)
        self.assertTrue(all(r[2] == 'A' for r in api.requests))
        self.assertEqual(set(ids_of(res)), set(data.loc[data['ts_code'] == 'A', 'id']))

    def test_list_return(self):
        """FMP 返回 list：与 DataFrame 同样处理"""
        api = FakeApi(make_data(3000, lambda r, i: 2), limit=500, as_list=True)
        res = self.run_guard(api, '20000101', '20080401')
        self.assertIsInstance(res, list)
        self.assertComplete(api, '20000101', '20080401', res)

    def test_fmt_passed_to_api(self):
        """切出的日期按 fmt 格式化"""
        api = FakeApi(make_data(1000, lambda r, i: 1), limit=300)
        guard('t', dict(start='2000-01-01', end='2002-09-26'),
              lambda start, end: api(start, end), fmt='%Y-%m-%d')
        self.assertTrue(all(len(s) == 10 and s[4] == '-' for s, _, _ in api.requests))

    def test_random(self):
        """随机数据、随机 limit、随机截断方式：反复验证完整"""
        rnd = random.Random(42)
        for k in range(200):
            api_guard.reset()
            days = rnd.randint(1, 3000)
            density = rnd.choice([0.05, 0.3, 1, 3, 10])
            limit = rnd.choice([100, 137, 500, 1000, 5000])
            truncate = rnd.choice(['newest', 'oldest'])
            data = make_data(days, lambda r, i: r.poisson(density), seed=k)
            if len(data) and data.groupby('date').size().max() > limit:
                continue  # 单时点超限另有测试
            api = FakeApi(data, limit=limit, truncate=truncate)
            end = (pd.Timestamp('20000101') + pd.Timedelta(days=days - 1)).strftime(FMT)
            with self.subTest(k=k, days=days, density=density, limit=limit, truncate=truncate):
                res = self.run_guard(api, '20000101', end)
                self.assertComplete(api, '20000101', end, res)


class TestGuardIntervalForms(unittest.TestCase):
    """没有日期范围的区间：单时点原样请求；无时间字段时只有 C 行放行"""

    def setUp(self):
        api_guard.reset()
        api_guard.configure_throttle(0, 0)

    def test_time_point_passes(self):
        for field in ('trade_date', 'ann_date', 'month', 'quarter', 'period'):
            res = guard('t_' + field, {field: '20260930'}, lambda **kw: pd.DataFrame({'x': range(150)}))
            self.assertEqual(len(res), 150)

    def test_time_point_at_proven_limit_raises(self):
        """单时点返回行数达到已证明的 limit：报错"""
        api = FakeApi(make_data(3000, lambda r, i: 3), limit=500)
        guard('t', dict(start='20000101', end='20080401'), lambda start, end: api(start, end), fmt=FMT)
        with self.assertRaises(RuntimeError):
            guard('t', dict(trade_date='20000101'), lambda trade_date: pd.DataFrame({'x': range(500)}))

    def test_no_time_field_not_c_raises(self):
        with self.assertRaises(RuntimeError):
            guard('t', dict(exchange='SSE'), lambda exchange: pd.DataFrame({'x': [1]}))
        with self.assertRaises(RuntimeError):
            guard('t', dict(ts_code='A', start=None, end=None),
                  lambda ts_code, start, end: pd.DataFrame({'x': [1]}))

    def test_no_time_field_c_row_passes(self):
        spec = type('Spec', (), {'allow_start_end': 'C', 'fill_arg_type': 'list'})()
        api_guard.set_row(spec)
        try:
            res = guard('t', dict(exchange='SSE'), lambda exchange: pd.DataFrame({'x': [1, 2]}))
        finally:
            api_guard.set_row(None)
        self.assertEqual(len(res), 2)


class TestThrottleCount(unittest.TestCase):
    """每次真实请求都计数，含二分多发的请求"""

    def test_every_request_counted(self):
        api_guard.reset()
        api = FakeApi(make_data(3000, lambda r, i: 3), limit=500)
        with patch.object(api_guard, 'count_request') as counter:
            guard('t', dict(start='20000101', end='20080401'), lambda start, end: api(start, end), fmt=FMT)
        self.assertEqual(counter.call_count, len(api.requests))


if __name__ == '__main__':
    unittest.main()
