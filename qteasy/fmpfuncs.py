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
import threading
import requests
import pandas as pd

from qteasy._arg_validators import QT_CONFIG
from qteasy.utilfuncs import regulate_date_format

_FMP_BASE = 'https://financialmodelingprep.com/stable'

# 全局请求限流（token bucket，跨线程）：平均速率 = _FMP_BATCH_SIZE / _FMP_BATCH_INTERVAL 次/秒
# None = 不限速；下载前在 notebook 里按需设置（与 refill 的 download_batch_size/interval 同义）。
_FMP_BATCH_SIZE = None
_FMP_BATCH_INTERVAL = None
_fmp_lock = threading.Lock()
_fmp_last_call = [0.0]

# endpoint -> page_limit: 每页最大条数，None 表示该端点无需分页
_FMP_API_LIMITS = {
    'historical-price-eod/dividend-adjusted': None,  # 用 from/to 过滤，不需分页
    'stock-list':                             None,
    'analyst-estimates':                      10,    # small 10, medium 1000
    'income-statement':                       1000,
    'balance-sheet-statement':                1000,
    'cash-flow-statement':                    1000,
}


def _get_api_key() -> str:
    return QT_CONFIG.get('fmp_api_key', '')


def _get_proxy():
    """qteasy.cfg 配置 fmp_proxy(如 http://127.0.0.1:7890)时仅 FMP 请求走该代理; 不配则直连。"""
    proxy = QT_CONFIG.get('fmp_proxy', '')
    return {'http': proxy, 'https': proxy} if proxy else None


def _fmp_get(endpoint: str, **params) -> list:
    """向 FMP stable API 发起单次 GET 请求，返回 JSON list。

    全局限流：_FMP_BATCH_SIZE/_FMP_BATCH_INTERVAL 均非 None 时，跨线程把平均请求速率
    压在 _FMP_BATCH_SIZE/_FMP_BATCH_INTERVAL 次/秒以内；为 None 则不限速。
    报错不带 url（含 apikey），避免泄露密钥。
    """
    params['apikey'] = _get_api_key()
    if _FMP_BATCH_SIZE and _FMP_BATCH_INTERVAL:
        interval = _FMP_BATCH_INTERVAL / _FMP_BATCH_SIZE
        with _fmp_lock:
            wait = _fmp_last_call[0] + interval - time.time()
            if wait > 0:
                time.sleep(wait)
            _fmp_last_call[0] = time.time()
    retry_delays = (1, 5, 30, 60)
    retry_statuses = {429, 500, 502, 503, 504}
    for attempt in range(len(retry_delays) + 1):
        try:
            with requests.get(f'{_FMP_BASE}/{endpoint}', params=params, timeout=10,
                              proxies=_get_proxy()) as resp:
                if not resp.ok:
                    error = RuntimeError(
                        f'FMP {endpoint} request failed: HTTP {resp.status_code}'
                    )
                    if resp.status_code not in retry_statuses or attempt == len(retry_delays):
                        raise error
                else:
                    return resp.json()
        except requests.exceptions.RequestException:
            if attempt == len(retry_delays):
                raise RuntimeError(f'FMP {endpoint} request failed after 4 retries')
        time.sleep(retry_delays[attempt])


def set_rate_limit(batch_size, interval):
    """设置 _fmp_get 全局请求限流：平均速率 = batch_size/interval 次/秒；任一为 None 则不限速。"""
    global _FMP_BATCH_SIZE, _FMP_BATCH_INTERVAL
    _FMP_BATCH_SIZE = batch_size
    _FMP_BATCH_INTERVAL = interval


def _fmp_request(endpoint: str, **params) -> list:
    """自动翻页，返回该端点完整数据。限速由 _fmp_get 全局处理。

    分页：按 _FMP_API_LIMITS[endpoint] 决定每页条数，None 则单次返回。
    """
    page_limit = _FMP_API_LIMITS.get(endpoint)

    if page_limit is None:
        return _fmp_get(endpoint, **params)

    results, page = [], 0
    while True:
        data = _fmp_get(endpoint, page=page, limit=page_limit, **params)
        if not data:
            break
        results.extend(data)
        if len(data) < page_limit:
            break
        page += 1
    return results


def us_reported_currency(ts_code: str) -> str:
    """该股最新申报币种 (reportedCurrency)。"""
    data = _fmp_get('income-statement', symbol=ts_code, limit=1)
    return (data[0].get('reportedCurrency') or 'USD') if data else 'USD'


def us_enterprise_value(ts_code: str):
    """从 FMP enterprise-values 取企业价值(EV)并折算为 USD；该接口返回原币种。无数据返回 None。"""
    data = _fmp_get('enterprise-values', symbol=ts_code, limit=1)
    ev = data[0].get('enterpriseValue') if data else None
    if ev is None:
        return None
    return float(ev) * us_fx_rate(us_reported_currency(ts_code))


# 历史汇率曲线缓存(线程安全，每个币种存一次、跨标的/跨财季复用)。结构：
#   { currency: (covered_start: Timestamp, covered_end: Timestamp, Series{date -> close}(升序)) }
#   例: {'CAD': (Timestamp('2014-12-17'), Timestamp('2026-06-21'), Series[~2900 行])}
_fx_history_cache = {}
_fx_history_lock = threading.Lock()

# Historical Forex Full Chart API 单次最多 5000 条；按 10 年/页(≈2600 条)分页，稳在上限内。
_FX_PAGE_YEARS = 10


def _fx_history(currency: str, start, end) -> pd.Series:
    """拉取并缓存 {currency}USD 在 [start-15d, end] 的收盘价(date->close 升序)。

    前推 15 天保证 start 当日休市时 asof 仍能回退到更早的交易日；分页(≤10 年/页)绕过单次
    5000 条上限；coverage-aware：缓存已覆盖请求区间则复用，否则扩展并集区间重拉。线程安全。
    """
    start = pd.Timestamp(start).normalize() - pd.Timedelta(days=15)
    end = pd.Timestamp(end).normalize()
    with _fx_history_lock:
        cached = _fx_history_cache.get(currency)
        if cached is not None:
            cov_start, cov_end, series = cached
            if start >= cov_start and end <= cov_end:
                return series
            start, end = min(start, cov_start), max(end, cov_end)
        quotes = {}
        seg_start = start
        while seg_start <= end:
            seg_end = min(seg_start + pd.DateOffset(years=_FX_PAGE_YEARS) - pd.Timedelta(days=1), end)
            for r in _fmp_request('historical-price-eod/full', symbol=f'{currency}USD',
                                  **{'from': seg_start.strftime('%Y-%m-%d'),
                                     'to':   seg_end.strftime('%Y-%m-%d')}):
                quotes[pd.Timestamp(r['date'][:10])] = float(r['close'])
            seg_start = seg_end + pd.Timedelta(days=1)
        if not quotes:
            raise ValueError(f'no historical FX for {currency}USD in [{start.date()}, {end.date()}]')
        series = pd.Series(quotes).sort_index()
        _fx_history_cache[currency] = (start, end, series)
        return series


def us_fx_rate(currency: str, date=None) -> float:
    """currency -> USD 汇率。供 estimates 取实时汇率，以及单点历史查询。

    date=None：取实时报价(quote)；给定 date：在该币种历史曲线上 asof(<= date 的最近交易日)。
    """
    if currency == 'USD':
        return 1.0
    if date is None:
        data = _fmp_get('quote', symbol=f'{currency}USD')
        if not data:
            raise ValueError(f'no FX rate for {currency}USD')
        return float(data[0]['price'])
    d = pd.Timestamp(date).normalize()
    curve = _fx_history(currency, d - pd.DateOffset(years=1), d)
    pos = curve.index.searchsorted(d, side='right') - 1
    if pos < 0:
        raise ValueError(f'no historical FX for {currency}USD on or before {d.date()}')
    return float(curve.iloc[pos])


def _fx_to_usd(values: pd.Series, currency: str, dates) -> pd.Series:
    """把一列以 currency 计价的金额，按各行 dates 的历史汇率折算成 USD 并返回。

    只读缓存；未命中由 _fx_history 下载该币种曲线后再读。各表的下载函数自行对其金额列调用。
    """
    if currency == 'USD':
        return values
    values = values.astype('float64')
    dates = pd.to_datetime(pd.Index(dates)).normalize()
    curve = _fx_history(currency, dates.min(), dates.max())  # 命中即复用，未命中下载
    pos = curve.index.searchsorted(dates, side='right') - 1  # 各行 <= 其 date 的最近交易日
    if (pos < 0).any():
        bad = dates[pos < 0].min()
        raise ValueError(f'no historical FX for {currency}USD on or before {bad.date()}')
    return values * curve.to_numpy()[pos]


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
        return pd.DataFrame(rows)  # 交易日历无金额列，不折算
    else:
        return list(trading_dates[::-1])


def us_stock_basic(exchange: str = None) -> pd.DataFrame:  # noqa: ARG001
    """从 FMP company-screener 下载美国主板(NASDAQ/NYSE/AMEX)、市值>$10B 的普通股基本信息。"""
    columns = ['ts_code', 'name', 'exchange', 'sector', 'industry', 'country']
    field_map = {
        'symbol':            'ts_code',
        'companyName':       'name',
        'exchangeShortName': 'exchange',
        'sector':            'sector',
        'industry':          'industry',
        'country':           'country',
    }
    data = _fmp_request('company-screener',
                        exchange='NASDAQ,NYSE,AMEX',
                        isEtf='false', isFund='false', isActivelyTrading='true',
                        marketCapMoreThan=10000000000, limit=20000)
    if not data:
        raise ValueError('company-screener returned no data')
    rows = []
    for item in data:
        row = {}
        for src, col in field_map.items():
            if src not in item:
                raise KeyError(f'company-screener missing field {src!r}: {item}')
            row[col] = item[src]
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


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


def us_estimates(ts_code: str = None, **_) -> pd.DataFrame:
    """美股分析师一致预期下载(FMP 源)：拉取 analyst-estimates → 映射建表 → 按申报币种折算 USD →
    交给 UsEstimateDatabase(继承通用 EstimateDatabase) 做 change-log 过滤。fmp 专属流程都在本函数。
    """
    if ts_code is None:
        return pd.DataFrame()
    from .us_estimates_db import UsEstimateDatabase
    db = UsEstimateDatabase()

    # FMP analyst-estimates 字段 -> us_estimates 表列
    fmp_map = {
        'epsAvg': 'eps', 'epsHigh': 'eps_high', 'epsLow': 'eps_low',
        'revenueAvg': 'revenue', 'revenueHigh': 'revenue_high', 'revenueLow': 'revenue_low',
        'netIncomeAvg': 'net_profit', 'netIncomeHigh': 'net_profit_high', 'netIncomeLow': 'net_profit_low',
        'ebitdaAvg': 'ebitda', 'ebitdaHigh': 'ebitda_high', 'ebitdaLow': 'ebitda_low',
        'ebitAvg': 'ebit', 'ebitHigh': 'ebit_high', 'ebitLow': 'ebit_low',
        'sgaExpenseAvg': 'sga_expense', 'sgaExpenseHigh': 'sga_expense_high', 'sgaExpenseLow': 'sga_expense_low',
        'numAnalystsEps': 'num_analysts_eps', 'numAnalystsRevenue': 'num_analysts_revenue',
    }
    trade_date = pd.Timestamp.now(tz='America/New_York').normalize().tz_localize(None)  # 快照日(美东)
    rows = []
    # for period in ('annual', 'quarter'):
    for period in ('annual',):
        for item in _fmp_request('analyst-estimates', symbol=ts_code, period=period):
            d = item.get('date')
            if not d or len(d) < 10:
                raise ValueError(f'{ts_code} {period}: missing or invalid date field: {item}')
            td = pd.Timestamp(d[:10])
            if td < trade_date:  # 只保留未来目标期的预期
                continue
            row = dict.fromkeys(db.COLUMNS)
            row['ts_code'] = ts_code
            row['trade_date'] = trade_date
            row['target_date'] = td
            row['target_period'] = 'Y' if period == 'annual' else 'Q'
            for src, col in fmp_map.items():
                if src not in item:
                    raise KeyError(f'{ts_code} {period}: FMP missing field {src!r}: {item}')
                row[col] = item[src]
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=db.COLUMNS)

    df = pd.DataFrame(rows, columns=db.COLUMNS)
    for col, dt in db.SCHEMA.items():
        if dt == 'date':
            df[col] = pd.to_datetime(df[col], errors='raise')
        elif dt == 'double':
            df[col] = pd.to_numeric(df[col], errors='raise')
        elif dt == 'int':
            s = pd.to_numeric(df[col], errors='raise')
            if (s.dropna() % 1 != 0).any():
                raise ValueError(f'{col}: non-integer value in int column')
            df[col] = s

    currency = db._currency_of('fmp', ts_code)
    if currency != 'USD':  # 预期为外币时按快照日 trade_date 折算为 USD(与财报表共用 _fx_to_usd 缓存)
        for col, dt in db.SCHEMA.items():
            if dt == 'double':
                df[col] = _fx_to_usd(df[col], currency, df['trade_date'])
    return db.changelog(df)


def _us_financials_common(endpoint: str,
                          ts_code: str,
                          start: str,
                          end: str) -> list:
    """拉取 income/balance/cashflow 原始数据并做日期过滤，返回 item 列表。"""
    start_ts = pd.Timestamp(regulate_date_format(start, force_format='date')) if start else None
    end_ts   = pd.Timestamp(regulate_date_format(end,   force_format='date')) if end   else None

    items = []
    for period in ('annual', 'quarter'):
        for item in _fmp_request(endpoint, symbol=ts_code, period=period):
            date_str = item.get('date', '')
            if len(date_str) < 10:
                continue
            dt = pd.Timestamp(date_str[:10])
            if start_ts and dt < start_ts:
                continue
            if end_ts and dt > end_ts:
                continue
            item['_trade_date'] = dt
            item['_period'] = 'Y' if period == 'annual' else 'Q'
            items.append(item)
    return items


def us_income(ts_code: str = None,
              start: str = None,
              end: str = None) -> pd.DataFrame:
    """从 FMP income-statement 接口下载美股利润表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('income-statement', ts_code, start, end)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':         ts_code,
        'trade_date':      i['_trade_date'],
        'period':          i['_period'],
        'filing_date':     pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':     i.get('fiscalYear', ''),
        'revenue':                  i.get('revenue'),
        'cost_of_revenue':          i.get('costOfRevenue'),
        'gross_profit':             i.get('grossProfit'),
        'rd_expense':               i.get('researchAndDevelopmentExpenses'),
        'sga_expense':              i.get('sellingGeneralAndAdministrativeExpenses'),
        'operating_expense':        i.get('operatingExpenses'),
        'cost_and_expense':         i.get('costAndExpenses'),
        'interest_income':          i.get('interestIncome'),
        'interest_expense':         i.get('interestExpense'),
        'da':                       i.get('depreciationAndAmortization'),
        'ebitda':                   i.get('ebitda'),
        'ebit':                     i.get('ebit'),
        'operating_income':         i.get('operatingIncome'),
        'other_income':             i.get('totalOtherIncomeExpensesNet'),
        'income_before_tax':        i.get('incomeBeforeTax'),
        'income_tax':               i.get('incomeTaxExpense'),
        'net_income_cont':          i.get('netIncomeFromContinuingOperations'),
        'net_income':               i.get('netIncome'),
        'eps':                      i.get('eps'),
        'eps_diluted':              i.get('epsDiluted'),
        'shares_out':               i.get('weightedAverageShsOut'),
        'shares_out_dil':           i.get('weightedAverageShsOutDil'),
    } for i in items]
    df = pd.DataFrame(rows)
    currency = items[0].get('reportedCurrency') or 'USD'
    if currency != 'USD':  # 外币财报按各期 trade_date 折算为 USD
        skip = {'ts_code', 'trade_date', 'period', 'filing_date', 'fiscal_year',
                'shares_out', 'shares_out_dil'}  # 主键/元信息/股数不折算
        for col in [c for c in df.columns if c not in skip]:
            df[col] = _fx_to_usd(df[col], currency, df['trade_date'])
    return df


def us_balance(ts_code: str = None,
               start: str = None,
               end: str = None) -> pd.DataFrame:
    """从 FMP balance-sheet-statement 接口下载美股资产负债表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('balance-sheet-statement', ts_code, start, end)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':                  ts_code,
        'trade_date':               i['_trade_date'],
        'period':                   i['_period'],
        'filing_date':              pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':              i.get('fiscalYear', ''),
        'cash':                     i.get('cashAndCashEquivalents'),
        'st_investments':           i.get('shortTermInvestments'),
        'cash_and_st_inv':          i.get('cashAndShortTermInvestments'),
        'net_receivables':          i.get('netReceivables'),
        'inventory':                i.get('inventory'),
        'other_current_assets':     i.get('otherCurrentAssets'),
        'total_current_assets':     i.get('totalCurrentAssets'),
        'ppe_net':                  i.get('propertyPlantEquipmentNet'),
        'goodwill':                 i.get('goodwill'),
        'intangible_assets':        i.get('intangibleAssets'),
        'lt_investments':           i.get('longTermInvestments'),
        'tax_assets':               i.get('taxAssets'),
        'other_non_current_assets': i.get('otherNonCurrentAssets'),
        'total_non_current_assets': i.get('totalNonCurrentAssets'),
        'total_assets':             i.get('totalAssets'),
        'accounts_payable':         i.get('accountPayables'),
        'st_debt':                  i.get('shortTermDebt'),
        'deferred_revenue':         i.get('deferredRevenue'),
        'other_current_liab':       i.get('otherCurrentLiabilities'),
        'total_current_liab':       i.get('totalCurrentLiabilities'),
        'lt_debt':                  i.get('longTermDebt'),
        'other_non_current_liab':   i.get('otherNonCurrentLiabilities'),
        'total_non_current_liab':   i.get('totalNonCurrentLiabilities'),
        'total_liab':               i.get('totalLiabilities'),
        'common_stock':             i.get('commonStock'),
        'retained_earnings':        i.get('retainedEarnings'),
        'aoci':                     i.get('accumulatedOtherComprehensiveIncomeLoss'),
        'total_equity':             i.get('totalStockholdersEquity'),
        'minority_interest':        i.get('minorityInterest'),
        'total_liab_equity':        i.get('totalLiabilitiesAndTotalEquity'),
        'total_debt':               i.get('totalDebt'),
        'net_debt':                 i.get('netDebt'),
    } for i in items]
    df = pd.DataFrame(rows)
    currency = items[0].get('reportedCurrency') or 'USD'
    if currency != 'USD':  # 外币财报按各期 trade_date 折算为 USD
        skip = {'ts_code', 'trade_date', 'period', 'filing_date', 'fiscal_year'}
        for col in [c for c in df.columns if c not in skip]:
            df[col] = _fx_to_usd(df[col], currency, df['trade_date'])
    return df


def us_cashflow(ts_code: str = None,
                start: str = None,
                end: str = None) -> pd.DataFrame:
    """从 FMP cash-flow-statement 接口下载美股现金流量表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('cash-flow-statement', ts_code, start, end)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':              ts_code,
        'trade_date':           i['_trade_date'],
        'period':               i['_period'],
        'filing_date':          pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':          i.get('fiscalYear', ''),
        'net_income':           i.get('netIncome'),
        'da':                   i.get('depreciationAndAmortization'),
        'deferred_tax':         i.get('deferredIncomeTax'),
        'sbc':                  i.get('stockBasedCompensation'),
        'chg_working_capital':  i.get('changeInWorkingCapital'),
        'other_non_cash':       i.get('otherNonCashItems'),
        'cfo':                  i.get('netCashProvidedByOperatingActivities'),
        'capex':                i.get('investmentsInPropertyPlantAndEquipment'),
        'acquisitions':         i.get('acquisitionsNet'),
        'purchases_inv':        i.get('purchasesOfInvestments'),
        'sales_inv':            i.get('salesMaturitiesOfInvestments'),
        'other_investing':      i.get('otherInvestingActivities'),
        'cfi':                  i.get('netCashProvidedByInvestingActivities'),
        'net_debt_issuance':    i.get('netDebtIssuance'),
        'net_stock_issuance':   i.get('netStockIssuance'),
        'dividends_paid':       i.get('netDividendsPaid'),
        'other_financing':      i.get('otherFinancingActivities'),
        'cff':                  i.get('netCashProvidedByFinancingActivities'),
        'forex_effect':         i.get('effectOfForexChangesOnCash'),
        'net_chg_cash':         i.get('netChangeInCash'),
        'cash_end':             i.get('cashAtEndOfPeriod'),
        'ocf':                  i.get('operatingCashFlow'),
        'fcf':                  i.get('freeCashFlow'),
        'income_tax_paid':      i.get('incomeTaxesPaid'),
        'interest_paid':        i.get('interestPaid'),
    } for i in items]
    df = pd.DataFrame(rows)
    currency = items[0].get('reportedCurrency') or 'USD'
    if currency != 'USD':  # 外币财报按各期 trade_date 折算为 USD
        skip = {'ts_code', 'trade_date', 'period', 'filing_date', 'fiscal_year'}
        for col in [c for c in df.columns if c not in skip]:
            df[col] = _fx_to_usd(df[col], currency, df['trade_date'])
    return df
