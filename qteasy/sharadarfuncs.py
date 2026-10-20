# coding=utf-8
# ======================================
# File:     sharadarfuncs.py
# Author:   Jackie PENG
# Contact:  jackie.pengzhao@gmail.com
# Created:  2026-10-20
# Desc:
#   Interfaces to Sharadar direct api,
#   api.sharadar.com/v1.0 (not Nasdaq Data Link).
# ======================================

import io
import os
import time
import warnings

import requests
import pandas as pd

from qteasy.__init__ import logger_core, QT_CONFIG
from qteasy.utilfuncs import retry, regulate_date_format, to_timezone_datetime
from qteasy.api_guard import guard

ERRORS_TO_CHECK_ON_RETRY = Exception

EXTRA_RETRY_API = [

]

_SHR_BASE = 'https://api.sharadar.com/v1.0'
_US_TZ = 'America/New_York'   # 美股表的时区：时间按表所属市场换算，不按数据源
_RATE_LIMIT_TRIES = 30        # 撞限(429)后按 Retry-After 或窗口重置等待再试同一请求，最多这么多次


# sharadarfuncs interface function, call this function to extract data
def acquire_data(api_name, **kwargs):
    """ DataSource模块的接口函数，根据根据table的内容调用相应的 Sharadar API下载数据，并以DataFrame的形式返回数据"""
    data_download_retry_count = QT_CONFIG.hist_dnld_retry_cnt
    data_download_retry_wait = QT_CONFIG.hist_dnld_retry_wait
    data_download_retry_backoff = QT_CONFIG.hist_dnld_backoff

    if api_name in EXTRA_RETRY_API:
        data_download_retry_count += 3

    retry_decorator = retry(
            exception_to_check=ERRORS_TO_CHECK_ON_RETRY,
            mute=True,
            tries=data_download_retry_count,
            delay=data_download_retry_wait,
            backoff=data_download_retry_backoff,
            logger=logger_core,
    )
    func = globals()[api_name]
    decorated_func = retry_decorator(func)
    res = decorated_func(**kwargs)
    return res


def _get_key() -> str:
    key = QT_CONFIG.get('sharadar_api_key', '') or os.environ.get('SHARADAR_API_KEY', '')
    if not key:
        raise ValueError('sharadar_api_key is not configured (qteasy.cfg or env SHARADAR_API_KEY)')
    return key


class SharadarError(RuntimeError):
    """Sharadar 请求失败，kind 区分原因：rate_limit 撞限(429) / auth 密钥无效(401) /
    plan 套餐不含该表或按免费层处理(403 'Exceeds free tier') / server 服务器出错(5xx) / other。"""

    def __init__(self, table: str, status: int, text: str):
        if status == 429:
            kind = 'rate_limit'
        elif status == 401:
            kind = 'auth'
        elif status == 403:
            kind = 'plan'
        elif status >= 500:
            kind = 'server'
        else:
            kind = 'other'
        self.kind = kind
        super().__init__(f'sharadar {table} failed ({kind}): HTTP {status}, {text[:300]}')


def _check(table: str, resp: requests.Response) -> pd.DataFrame:
    """所有请求的状态判定：200 → 表；没有数据(周末、无此代码、次要类别)时 200 只给表头，是空表不是错误；其余抛 SharadarError。"""
    if resp.status_code != 200:
        raise SharadarError(table, resp.status_code, resp.text)
    return pd.read_csv(io.StringIO(resp.text)) if resp.text.strip() else pd.DataFrame()


def _shr_get(table: str, **params) -> pd.DataFrame:
    """向 Sharadar 发起单次 GET 请求(csv)，返回表。调用方一律经 guard 调用。

    限流(文档未写，实测)，两种都返回 429：
    1. 同一账户最多同时 5 个请求，多的返回 "Too many requests in progress" 并带 Retry-After(5 秒)；
    2. 每 15 分钟 5000 次请求的固定窗口，从窗口里第一个请求起算；另有加权额度 25000/窗口，每次扣 X-RateLimit-Cost
       (按代码、日期取数据实测都是 1)。
    撞限时有 Retry-After 按它等，没有就等到 X-RateLimit-Reset，再试同一请求，等完仍撞限则报错；主动节流由 refill 的
    process_count(≤ 5)和 download_batch_size/interval 负责。密钥放请求头，报错不带密钥。
    """
    for attempt in range(_RATE_LIMIT_TRIES + 1):
        with requests.get(f'{_SHR_BASE}/data/{table}', params={**params, 'format': 'csv'},
                          headers={'x-api-key': _get_key()}, timeout=120) as resp:
            try:
                return _check(table, resp)
            except SharadarError as error:
                if error.kind != 'rate_limit' or attempt == _RATE_LIMIT_TRIES:
                    raise
                retry_after = resp.headers.get('Retry-After')
                wait = (float(retry_after) if retry_after else
                        int(resp.headers.get('X-RateLimit-Reset', time.time() + 60)) - time.time() + 1)
        time.sleep(max(wait, 1))


def _frame(raw: pd.DataFrame, fields: dict) -> pd.DataFrame:
    """Sharadar 返回值的共性，统一在这里处理，各下载函数之后按列名直接取：
    1. 没有数据时只有表头(或空表) —— 按 fields 补成空列；
    2. 日期是不带时区的日历日 —— 按美股表的时区解释(to_timezone_datetime)后取日期，不截字符串；
    3. 金额单位是百万美元(descriptions 表 unittype 'USD millions') —— 'musd' 列乘 1e6 换成美元。
    fields: 接口字段 → 'date' / 'num' / 'musd' / 'str'。
    """
    raw = raw.reindex(columns=list(fields))
    for col, kind in fields.items():
        if kind == 'date':
            raw[col] = to_timezone_datetime(raw[col], _US_TZ).dt.normalize()
        elif kind in ('num', 'musd'):
            raw[col] = pd.to_numeric(raw[col], errors='coerce') * (1e6 if kind == 'musd' else 1)
    return raw


def _to_shr_ticker(ts_code: str) -> str:
    """qteasy(FMP)代码 → Sharadar 代码：类别分隔符 '-' 换成 '.'(BRK-B → BRK.B)。"""
    return ts_code.replace('-', '.')


def us_stock_indicator(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """美股每日估值指标(Sharadar daily)，[start, end] 左右闭；总股本 = 总市值 / 本地 us_stock_daily 当日收盘价(不复权)。

    市值口径：拆股当天调整；财报股数在 10-Q/10-K 申报日切换(无未来函数)；多类股按全部类别折算合计，一家公司只在主类别
    代码下有行(GOOG、BF-A 这类次要类别返回空)；分拆不往回除；ADR 按 ADS 份数(与 ADR 股价同口径)。
    pe、ps 用最近 4 个季度合计(AAPL 实测)，存 pe_ttm、ps_ttm(同港股 hk_indicators)；ev = 市值 + 债务 − 现金。
    套餐只有滚动 5 年，更早的不返回。区间里没有交易日、Sharadar 没有这个代码、次要类别时，接口返回空表，函数返回空。
    流通股本、换手率没有，留空。
    """
    if ts_code is None:
        return pd.DataFrame()
    fields = {'date': 'date', 'marketcap': 'musd', 'ev': 'musd', 'pe': 'num', 'pb': 'num', 'ps': 'num',
              'evebit': 'num', 'evebitda': 'num'}   # 除 ticker、lastupdated(Sharadar 自己的维护日期，不是这只股票的数据)外的全部字段
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    raw = guard('daily', dict(ts_code=_to_shr_ticker(ts_code), start=start, end=end),
                lambda ts_code, start, end:
                _shr_get('daily', ticker=ts_code, **{'from': start, 'to': end}),
                fmt='%Y-%m-%d')
    raw = _frame(raw, fields)
    if raw['date'].isna().any():
        warnings.warn(f'sharadar daily {ts_code}: {raw["date"].isna().sum()} rows without date dropped')
        raw = raw.dropna(subset=['date'])
    if raw.empty:
        return pd.DataFrame()
    res = pd.DataFrame({
        'ts_code':    ts_code,
        'trade_date': raw['date'].values,
        'total_mv':   raw['marketcap'].values,
        'ev':         raw['ev'].values,
        'pe_ttm':     raw['pe'].values,
        'pb':         raw['pb'].values,
        'ps_ttm':     raw['ps'].values,
        'ev_ebit':    raw['evebit'].values,
        'ev_ebitda':  raw['evebitda'].values,
    })
    from qteasy import QT_DATA_SOURCE
    daily = QT_DATA_SOURCE.read_table_data('us_stock_daily', shares=ts_code, start=start, end=end,
                                           primary_key_in_index=False)
    if not daily.empty:
        close = daily.assign(trade_date=pd.to_datetime(daily['trade_date'])).set_index('trade_date')['close']
        res['total_share'] = res['total_mv'] / res['trade_date'].map(close).astype('float64')
    return res
