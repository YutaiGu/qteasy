# coding=utf-8
# ======================================
# File:     api_guard.py
# Desc:
#   防静默截断(二叉树验证)，保护每一次 API 请求，见 docs/DOWNLOAD_DESIGN.md #2。
#   各数据源的每个 API 请求都写成 guard(name, 区间, lambda 区间的字段: ...)。
#   下载函数内部可能调用多个 API、再过滤组合，guard 只看每次请求的原始返回。
#   主动节流(#3)也在这里按真实请求计数；不经 guard 的数据源在请求出口调用 count_request()。
# ======================================

import threading
import time

import pandas as pd

# 任何接口的单次 limit 都不低于这么多行：返回行数小于它一定没被截断
MIN_API_LIMIT = 100

_guards = {}
_guards_lock = threading.Lock()

_throttle = {'size': 0, 'interval': 0, 'count': 0}
_throttle_lock = threading.Lock()


def configure_throttle(batch_size, interval):
    """设置主动节流：每 batch_size 次真实请求后暂停 interval 秒；任一为 0/None 则不节流"""
    with _throttle_lock:
        _throttle.update(size=batch_size or 0, interval=interval or 0, count=0)


def count_request():
    """每次真实请求前调用：计数，满 batch_size 次后暂停；暂停时持锁，所有线程一起等"""
    with _throttle_lock:
        size, interval = _throttle['size'], _throttle['interval']
        if not (size and interval):
            return
        _throttle['count'] += 1
        if _throttle['count'] > size and _throttle['count'] % size == 1:
            time.sleep(interval)


class _ApiGuard:
    """一个 API 一份：M 为该 API 见过的最大行数(M ≤ limit)，M_is_limit 为 limit 已被证明等于 M。"""

    def __init__(self, name):
        self.name = name
        self.M = 0
        self.M_is_limit = False
        self.lock = threading.Lock()

    def verify(self, call, start, end, rows, fmt) -> list:
        """返回 [start, end] 内确认完整的 rows 分块列表；最小单元超出已证明的 limit 时报错"""
        n = 0 if rows is None else len(rows)
        if n == 0:
            return []
        with self.lock:
            m, m_is_limit = self.M, self.M_is_limit
        if n < m or n < MIN_API_LIMIT:  # 小于已知下界，一定没截断
            return [rows]
        left, right = _bisect(start, end, fmt)

        if m_is_limit:  # n == limit，一定截断：这次返回丢弃，两半重新拉
            if left is None:
                raise RuntimeError(f'{self.name} [{start}, {end}] returned {n} rows = api limit, '
                                   f'but can not be split further')
            return (self.verify(call, *left, call(*left), fmt)
                    + self.verify(call, *right, call(*right), fmt))

        with self.lock:
            self.M = max(self.M, n)
        if left is None:  # 最小单元：由路由保证不超限
            return [rows]
        l_rows, r_rows = call(*left), call(*right)
        l_n = 0 if l_rows is None else len(l_rows)
        r_n = 0 if r_rows is None else len(r_rows)
        if l_n == 0 or r_n == 0 or l_n + r_n > n:
            if l_n + r_n > n:  # 两半之和 > 整段：整段被截断，n 就是 limit
                with self.lock:
                    self.M, self.M_is_limit = n, True
            return (self.verify(call, *left, l_rows, fmt)
                    + self.verify(call, *right, r_rows, fmt))
        return [rows]  # 两半非空且之和 == n：完整


def _bisect(start, end, fmt) -> tuple:
    """按自然日对半切 [start, end]，返回 ((s1, e1), (s2, e2))；单个时点(最小单元)返回 (None, None)"""
    if start == end:  # 单个时点(可能是月、期等非日期格式)
        return None, None
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    if start >= end:
        return None, None
    mid = start + pd.Timedelta(days=(end - start).days // 2)
    return ((start.strftime(fmt), mid.strftime(fmt)),
            ((mid + pd.Timedelta(days=1)).strftime(fmt), end.strftime(fmt)))


# 当前线程正在下载的 map 行(TableFetchSpec)：下载器调用下载函数前设置，guard 据此判断跳过还是报错
_row = threading.local()

def set_row(spec):
    """下载器在调用下载函数前后调用：告诉 guard 当前是 map 的哪一行；spec 为 None 表示不在下载任务中"""
    _row.spec = spec


# 区间里表示单个时点的字段：一天 / 一月 / 一季 / 一期，不可再切
_TIME_POINT_FIELDS = ('trade_date', 'ann_date', 'month', 'quarter', 'period')


def guard(name: str, interval: dict, call, fmt: str = '%Y%m%d'):
    """区间维护器：把区间传给一个 API、验证返回的数据，返回完整数据(list 或 DataFrame，与 call 的返回类型相同)。

    所有 API 请求都经这里。interval 是这次请求的区间(见 docs/DOWNLOAD_DESIGN.md 的区间形态)，call 以区间的字段为
    参数，只负责把字段对应到接口自己的参数名。值为空的字段视为没有。按区间的形态处理：
    1. 有 start 和 end：请求整段，发现截断时对半切日期后再请求(切出的日期按 fmt 格式化)，其他字段原样传；
    2. 有单个时点(_TIME_POINT_FIELDS)：原样请求一次，不切；返回行数达到已证明的 limit 时报错；
    3. 没有时间字段，当前行第 6 列为 C：下载函数按该接口的规则自己取全(翻页，或接口一次返回全部)，原样请求一次；
    4. 其余：没有任何东西保证取全，是 map 的设计错误，报错。
    """
    def counted(**changed):  # 每次真实请求都计入节流
        count_request()
        return call(**{**interval, **changed})

    with _guards_lock:
        state = _guards.setdefault(name, _ApiGuard(name))

    start, end = interval.get('start'), interval.get('end')
    if start and end:
        first = counted()
        parts = state.verify(lambda s, e: counted(start=s, end=e), start, end, first, fmt)
        if not parts:
            return first  # 空返回原样交回
        if isinstance(parts[0], pd.DataFrame):
            return parts[0] if len(parts) == 1 else pd.concat(parts, ignore_index=True)
        return [row for part in parts for row in part]

    if any(interval.get(k) for k in _TIME_POINT_FIELDS):
        rows = counted()
        n = 0 if rows is None else len(rows)
        with state.lock:
            if state.M_is_limit and n >= state.M:
                raise RuntimeError(f'{name} {interval} returned {n} rows = api limit, but can not be split')
        return rows

    spec = getattr(_row, 'spec', None)
    if spec is not None and spec.allow_start_end.upper() == 'C':
        return counted()
    raise RuntimeError(f'{name} {interval}: no time field and not a C row, completeness can not be verified')


def reset():
    """每次下载任务开始时重置：limit 可能变小，旧的 M 不能沿用"""
    with _guards_lock:
        _guards.clear()
