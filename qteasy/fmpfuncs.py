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

import time
import requests
import pandas as pd

from qteasy._arg_validators import QT_CONFIG
from qteasy.utilfuncs import regulate_date_format

_FMP_BASE = 'https://financialmodelingprep.com/stable'

# 全局速率限制（API Calls / Min），付费版 750
_FMP_CALLS_PER_MIN = 750

# endpoint -> page_limit: 每页最大条数，None 表示该端点无需分页
_FMP_API_LIMITS = {
    'historical-price-eod/dividend-adjusted': 1000,
    'stock-list':                             None,
    'analyst-estimates':                      1000,
}


def _get_api_key() -> str:
    return QT_CONFIG.get('fmp_api_key', '')


def _fmp_get(endpoint: str, **params) -> list:
    """向 FMP stable API 发起单次 GET 请求，返回 JSON list。"""
    params['apikey'] = _get_api_key()
    resp = requests.get(f'{_FMP_BASE}/{endpoint}', params=params)
    resp.raise_for_status()
    return resp.json()


def _fmp_request(endpoint: str, **params) -> list:
    """自动翻页并限速，返回该端点完整数据。

    分页：按 _FMP_API_LIMITS[endpoint] 决定每页条数，None 则单次返回。
    限速：请求间隔 = 60 / _FMP_CALLS_PER_MIN 秒。
    """
    page_limit = _FMP_API_LIMITS.get(endpoint)
    interval = 60.0 / (_FMP_CALLS_PER_MIN * 0.8)

    if page_limit is None:
        result = _fmp_get(endpoint, **params)
        time.sleep(interval)
        return result

    results, page = [], 0
    while True:
        data = _fmp_get(endpoint, page=page, limit=page_limit, **params)
        time.sleep(interval)
        if not data:
            break
        results.extend(data)
        if len(data) < page_limit:
            break
        page += 1
    return results


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

    data = _fmp_request('historical-price-eod/dividend-adjusted', **params)
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


def us_stock_basic(exchange: str = None) -> pd.DataFrame:  # noqa: ARG001
    """从 FMP Company Symbols List API 下载美股股票基本信息。（实际 exchange 空置因为默认 NYSE）"""
    data = _fmp_request('stock-list')
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    return pd.DataFrame({
        'ts_code':     df['symbol'].str[:20],
        'name':        '',
        'enname':      df['companyName'].fillna('').str[:80],
        'classify':    '',
        'list_date':   None,
        'delist_date': None,
    })


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

    data = _fmp_request('historical-price-eod/dividend-adjusted', **params)
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


def us_estimates(ts_code: str = None,
                 start: str = None,
                 end: str = None) -> pd.DataFrame:
    """从 FMP analyst-estimates 接口下载单只美股分析师一致预期。"""
    if ts_code is None:
        return pd.DataFrame()

    import pytz
    trade_date = pd.Timestamp.now(tz=pytz.timezone('America/New_York')).normalize().tz_localize(None)

    start_ts = pd.Timestamp(regulate_date_format(start, force_format='date')) if start else None
    end_ts   = pd.Timestamp(regulate_date_format(end,   force_format='date')) if end   else None
    rows = []
    for period in ('annual', 'quarter'):
        for item in _fmp_request('analyst-estimates', symbol=ts_code, period=period):
            date_str = item.get('date', '')
            if len(date_str) < 10:
                continue
            dt = pd.Timestamp(date_str[:10])
            if start_ts and dt < start_ts:
                continue
            if end_ts and dt > end_ts:
                continue
            rows.append({
                'ts_code':              ts_code,
                'trade_date':           trade_date,
                'target_date':          dt,
                'target_period':        'Y' if period == 'annual' else 'Q',
                'eps':                  item.get('epsAvg'),
                'eps_high':             item.get('epsHigh'),
                'eps_low':              item.get('epsLow'),
                'revenue':              item.get('revenueAvg'),
                'revenue_high':         item.get('revenueHigh'),
                'revenue_low':          item.get('revenueLow'),
                'net_profit':           item.get('netIncomeAvg'),
                'net_profit_high':      item.get('netIncomeHigh'),
                'net_profit_low':       item.get('netIncomeLow'),
                'ebitda':               item.get('ebitdaAvg'),
                'ebitda_high':          item.get('ebitdaHigh'),
                'ebitda_low':           item.get('ebitdaLow'),
                'ebit':                 item.get('ebitAvg'),
                'ebit_high':            item.get('ebitHigh'),
                'ebit_low':             item.get('ebitLow'),
                'sga_expense':          item.get('sgaExpenseAvg'),
                'sga_expense_high':     item.get('sgaExpenseHigh'),
                'sga_expense_low':      item.get('sgaExpenseLow'),
                'target_price':         None,
                'num_analysts_eps':     item.get('numAnalystsEps'),
                'num_analysts_revenue': item.get('numAnalystsRevenue'),
            })

    if not rows:
        return pd.DataFrame()

    new_df = pd.DataFrame(rows)

    _VALUE_COLS = [
        'eps', 'eps_high', 'eps_low',
        'revenue', 'revenue_high', 'revenue_low',
        'net_profit', 'net_profit_high', 'net_profit_low',
        'ebitda', 'ebitda_high', 'ebitda_low',
        'ebit', 'ebit_high', 'ebit_low',
        'sga_expense', 'sga_expense_high', 'sga_expense_low',
        'target_price', 'num_analysts_eps', 'num_analysts_revenue',
    ]

    from qteasy import QT_DATA_SOURCE
    existing = QT_DATA_SOURCE.read_table_data(
        'us_estimates', shares=ts_code, primary_key_in_index=False
    )

    if existing.empty:
        return new_df

    baseline = (existing.sort_values('trade_date')
                        .groupby(['target_date', 'target_period'])[_VALUE_COLS]
                        .last())

    def _changed(row):
        key = (row['target_date'], row['target_period'])
        if key not in baseline.index:
            return True
        return not row[_VALUE_COLS].equals(baseline.loc[key])

    return new_df[new_df.apply(_changed, axis=1)]
