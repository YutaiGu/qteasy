# coding=utf-8
# ======================================
# File:     fmpfuncs.py
# Desc:
#   FMP (Financial Modeling Prep) data acquisition.
#   Pattern mirrors tsfuncs.py / akfuncs.py:
#     acquire_data(api_name, **kwargs) is
#     the single entry point called by
#     data_channels._fetch_table_data_from_fmp.
#
#   API key is read from qteasy.cfg:
#     fmp_api_key = YOUR_KEY
# ======================================

import requests
import pandas as pd
from financetoolkit import Toolkit

from qteasy._arg_validators import QT_CONFIG
from qteasy.utilfuncs import regulate_date_format

_FMP_BASE = 'https://financialmodelingprep.com/stable'

def _get_api_key() -> str:
    return QT_CONFIG.get('fmp_api_key', '')


def _fmp_get(endpoint: str, **params) -> list:
    """向 FMP stable API 发起 GET 请求，返回 JSON list。"""
    params['apikey'] = _get_api_key()
    resp = requests.get(f'{_FMP_BASE}/{endpoint}', params=params)
    resp.raise_for_status()
    return resp.json()


_QTR_END = {'Q1': '0331', 'Q2': '0630', 'Q3': '0930', 'Q4': '1231'}

_METRIC_MAP = {
    'Estimated EPS Average': 'eps',
    'Estimated Revenue Average': 'revenue',
    'Estimated Net Income Average': 'net_profit',
    'Number of Analysts': 'num_analysts',
}

_US_EST_COLS = [
    'ts_code', 'trade_date', 'target_period',
    'eps', 'revenue', 'net_profit', 'target_price', 'num_analysts',
]


def acquire_data(api_name, **kwargs):
    """data_channels 的统一入口，按 api_name 分发到本模块的具体函数。"""
    func = globals()[api_name]
    return func(**kwargs)


def us_trade_calendar(start: str = None,
                      end: str = None,
                      is_open: int = None):
    """以 SPY 历史行情的交易日期作为 NYSE/NASDAQ 交易日历。"""
    params = {'symbol': 'SPY'}
    if start:
        params['from'] = regulate_date_format(start, force_format='date')
    if end:
        params['to'] = regulate_date_format(end, force_format='date')

    data = _fmp_get('historical-price-eod/dividend-adjusted', **params)
    if not data:
        if is_open is None:
            return pd.DataFrame(columns=['cal_date', 'is_open', 'pretrade_date'])
        return []

    trading_dates = pd.to_datetime(
        pd.DataFrame(data).sort_values('date')['date']
    ).dt.normalize()

    trading_set = set(trading_dates)
    range_start = pd.to_datetime(start) if start else trading_dates.iloc[0]
    range_end = pd.to_datetime(end) if end else trading_dates.iloc[-1]
    all_dates = pd.date_range(start=range_start, end=range_end, freq='D')

    if is_open is None:
        prev_trade = None
        rows = []
        for d in all_dates:
            open_flag = 1 if d in trading_set else 0
            rows.append({
                'exchange': 'NYSE',
                'cal_date': d.strftime('%Y%m%d'),
                'is_open': open_flag,
                'pretrade_date': prev_trade,
            })
            if open_flag:
                prev_trade = d.strftime('%Y%m%d')
        return pd.DataFrame(rows)
    else:
        return list(trading_dates[::-1])


def us_stock_daily_adj(ts_code: str = None,
                   trade_date: str = None,
                   start: str = None,
                   end: str = None) -> pd.DataFrame:
    """从 FMP Dividend-Adjusted Price Chart API 下载美股日线行情。

    Parameters
    ----------
    ts_code : str, optional
        股票代码，如 'AAPL'
    trade_date : str, optional
        单日查询，格式 'YYYYMMDD'
    start : str, optional
        格式 'YYYYMMDD' 或 'YYYY-MM-DD'
    end : str, optional
    """
    if ts_code is None:
        return pd.DataFrame()

    params = {'symbol': ts_code}
    if trade_date:
        td = regulate_date_format(trade_date, force_format='date')
        params['from'] = td
        params['to'] = td
    else:
        if start:
            params['from'] = regulate_date_format(start, force_format='date')
        if end:
            params['to'] = regulate_date_format(end, force_format='date')

    data = _fmp_get('historical-price-eod/dividend-adjusted', **params)
    if not data:
        return pd.DataFrame()

    raw = pd.DataFrame(data).sort_values('date').reset_index(drop=True)
    close = raw['adjClose']
    pre_close = close.shift(1)
    change = (close - pre_close).round(4)
    pct_chg = (change / pre_close * 100).round(4)

    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_date': pd.to_datetime(raw['date']),
        'open':       raw['adjOpen'].values,
        'high':       raw['adjHigh'].values,
        'low':        raw['adjLow'].values,
        'close':      close.values,
        'pre_close':  pre_close.values,
        'change':     change.values,
        'pct_chg':    pct_chg.values,
        'vol':        raw['volume'].values,
        'amount':     None,
    })


def _extract_estimates(raw, ts_code: str, is_annual: bool) -> list:
    """从 financetoolkit get_analyst_estimates() 的结果中提取行列表。"""
    if raw.empty:
        return []
    sub = raw.xs(ts_code, axis=1, level=1) if isinstance(raw.columns, pd.MultiIndex) else raw
    rows = []
    for period in sub.columns:
        ps = str(period)
        if is_annual:
            if not (len(ps) == 4 and ps.isdigit()):
                continue
            target = f'{ps}Y'
        else:
            if not (len(ps) == 6 and ps[:4].isdigit() and ps[4] == 'Q' and ps[4:] in _QTR_END):
                continue
            target = ps
        vals = {c: None for c in ('eps', 'revenue', 'net_profit', 'target_price', 'num_analysts')}
        for metric, col in _METRIC_MAP.items():
            try:
                v = sub.loc[metric, period]
                if pd.notna(v):
                    vals[col] = int(v) if col == 'num_analysts' else float(v)
            except (KeyError, TypeError):
                pass
        rows.append({'target': target, 'period_str': ps, **vals})
    return rows


def us_estimates(ts_code: str, trade_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """下载单只美股分析师一致预期，映射到 trade_date 后返回 us_estimates schema 的 DataFrame。"""
    rows = []
    trade_dates = pd.DatetimeIndex(trade_dates)

    try:
        raw_a = Toolkit(tickers=ts_code, api_key=_get_api_key(), quarterly=False).get_analyst_estimates()
    except Exception:
        raw_a = pd.DataFrame()
    for item in _extract_estimates(raw_a, ts_code, is_annual=True):
        fy = int(item['target'][:4])
        for td in trade_dates[trade_dates.year == fy]:
            rows.append({
                'ts_code': ts_code, 'trade_date': td,
                'target_period': item['target'],
                'eps': item['eps'], 'revenue': item['revenue'],
                'net_profit': item['net_profit'],
                'target_price': item['target_price'],
                'num_analysts': item['num_analysts'],
            })

    try:
        raw_q = Toolkit(tickers=ts_code, api_key=_get_api_key(), quarterly=True).get_analyst_estimates()
    except Exception:
        raw_q = pd.DataFrame()
    for item in _extract_estimates(raw_q, ts_code, is_annual=False):
        ps = item['period_str']
        qtr_end = pd.Timestamp(ps[:4] + _QTR_END[ps[4:]])
        rows.append({
            'ts_code': ts_code, 'trade_date': qtr_end,
            'target_period': item['target'],
            'eps': item['eps'], 'revenue': item['revenue'],
            'net_profit': item['net_profit'],
            'target_price': item['target_price'],
            'num_analysts': item['num_analysts'],
        })

    if not rows:
        return pd.DataFrame(columns=_US_EST_COLS)
    return pd.DataFrame(rows)[_US_EST_COLS]
