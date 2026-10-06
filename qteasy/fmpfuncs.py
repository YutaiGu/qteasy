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
import warnings
import requests
import pandas as pd

from qteasy._arg_validators import QT_CONFIG
from qteasy.utilfuncs import regulate_date_format
from qteasy.api_guard import guard

_FMP_BASE = 'https://financialmodelingprep.com/stable'

# 撞限(HTTP 429)后依次等待这些秒数再重试同一请求，等完仍撞限则报错
_RATE_LIMIT_WAITS = (60, 120, 240)

def _get_api_key() -> str:
    return QT_CONFIG.get('fmp_api_key', '')


def _get_proxy():
    """qteasy.cfg 配置 fmp_proxy(如 http://127.0.0.1:7890)时仅 FMP 请求走该代理; 不配则直连。"""
    proxy = QT_CONFIG.get('fmp_proxy', '')
    return {'http': proxy, 'https': proxy} if proxy else None


def _fmp_get(endpoint: str, **params) -> list:
    """向 FMP stable API 发起单次 GET 请求，返回 JSON list。调用方一律经 guard 调用。

    限流识别：HTTP 429 依次等待 _RATE_LIMIT_WAITS 后重试同一请求，等完仍撞限则报错；
    主动节流由 refill 的 download_batch_size/interval 统一负责。
    报错不带 url（含 apikey），避免泄露密钥。
    """
    params['apikey'] = _get_api_key()
    retry_delays = (1, 5, 30, 60)
    retry_statuses = {500, 502, 503, 504}
    attempt = 0
    limit_hits = 0
    while True:
        try:
            with requests.get(f'{_FMP_BASE}/{endpoint}', params=params, timeout=10,
                              proxies=_get_proxy()) as resp:
                if resp.ok:
                    return resp.json()
                error = RuntimeError(f'FMP {endpoint} request failed: HTTP {resp.status_code}')
                if resp.status_code == 429:  # 每分钟超限：等待后重试同一请求，不计入普通重试次数
                    if limit_hits == len(_RATE_LIMIT_WAITS):
                        raise RuntimeError(f'FMP {endpoint} rate limited after waiting {_RATE_LIMIT_WAITS}s')
                    time.sleep(_RATE_LIMIT_WAITS[limit_hits])
                    limit_hits += 1
                    continue
                if resp.status_code not in retry_statuses or attempt == len(retry_delays):
                    raise error
        except requests.exceptions.RequestException:
            if attempt == len(retry_delays):
                raise RuntimeError(f'FMP {endpoint} request failed after 4 retries')
        time.sleep(retry_delays[attempt])
        attempt += 1


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

def _fx_history(currency: str, start, end) -> pd.Series:
    """拉取并缓存 {currency}USD 在 [start-15d, end] 的收盘价(date->close 升序)。

    前推 15 天保证 start 当日休市时 asof 仍能回退到更早的交易日；coverage-aware：缓存已覆盖请求区间则复用，否则扩展并集区间重拉。线程安全。
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
        symbol = f'{currency}USD'
        data = guard('historical-price-eod/full',
                     dict(ts_code=symbol, start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d')),
                     lambda ts_code, start, end:
                     _fmp_get('historical-price-eod/full', symbol=ts_code, **{'from': start, 'to': end}),
                     fmt='%Y-%m-%d')
        quotes = {pd.Timestamp(r['date'][:10]): float(r['close']) for r in data}
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
    """NYSE 交易日历：工作日减去交易所公布的全天休市日(FMP holidays-by-exchange)，提前收盘日算交易日。

    [start, end] 左右闭。休市数据一次取全(FMP 覆盖 1970 至今后数年)，区间超出覆盖范围时报错，不推断；
    全量算好再截取，区间第一天的 pretrade_date 也正确。
    """
    data = guard('holidays-by-exchange', dict(start='1900-01-01', end='2100-12-31'),
                 lambda start, end:  # 该接口 from 不含当日，前推一天
                 _fmp_get('holidays-by-exchange', exchange='NYSE',
                          **{'from': (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime('%Y-%m-%d'), 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        raise ValueError('holidays-by-exchange(NYSE) returned no data')
    holidays = pd.DataFrame(data)
    closed = set(pd.to_datetime(holidays.loc[holidays['isClosed'] == True, 'date']))  # noqa: E712，None 为提前收盘
    cover_start = pd.Timestamp(f'{holidays["date"].min()[:4]}-01-01')
    cover_end = pd.Timestamp(f'{holidays["date"].max()[:4]}-12-31')

    range_start = pd.to_datetime(start) if start else cover_start
    range_end = pd.to_datetime(end) if end else cover_end
    if range_start < cover_start or range_end > cover_end:
        raise ValueError(f'NYSE calendar requested {range_start.date()}~{range_end.date()}, '
                         f'FMP holidays only cover {cover_start.date()}~{cover_end.date()}')

    days = pd.date_range(cover_start, cover_end, freq='D')
    cal = pd.DataFrame({'exchange': 'NYSE', 'cal_date': days.strftime('%Y%m%d'),
                        'is_open': [int(d.weekday() < 5 and d not in closed) for d in days]})
    cal['pretrade_date'] = cal['cal_date'].where(cal['is_open'] == 1).ffill().shift(1)
    cal = cal[(days >= range_start) & (days <= range_end)]

    if is_open is None:
        return cal.reset_index(drop=True)  # 交易日历无金额列，不折算
    return list(pd.to_datetime(cal.loc[cal['is_open'] == 1, 'cal_date'])[::-1])


def us_stock_basic(exchange: str = 'ALL', **_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """美股股票池基本信息：(VONE 持仓 ∩ screener) ∪ 在美国上市、市值 ≥ $5B 的外国公司(含 ADR)。

    VONE(Vanguard Russell 1000 ETF)跟踪 Russell 1000(美国公司，按市值取前约 1000 只，无盈利等偏好)；持仓与
    screener(三大交易所、在交易、非 ETF/基金、含全部股份类别)交叉认证，去掉现金、CVR、托管、OTC 等非交易所股票。
    名称、交易所、行业、国家取自 screener；ISIN、CUSIP 取自 VONE 持仓(外国公司部分为空)，同一代码有多行时
    (正股与附带的 CVR/权证共用代码)取权重最大的一行，即正股本身。
    exchange 为 ALL / 空时不过滤，否则只返回该交易所的股票。
    """
    columns = ['ts_code', 'name', 'exchange', 'sector', 'industry', 'country', 'isin', 'cusip']
    foreign_min_cap = 5_000_000_000

    holdings = pd.DataFrame(guard('etf/holdings', dict(), lambda:
                                  _fmp_get('etf/holdings', symbol='VONE')))
    rows, page = [], 0
    while True:  # company-screener 按 page 翻页，每页 1000 条
        data = guard('company-screener', dict(), lambda:
                     _fmp_get('company-screener', exchange='NASDAQ,NYSE,AMEX', isEtf='false', isFund='false',
                              isActivelyTrading='true', includeAllShareClasses='true', page=page, limit=1000))
        if not data:
            break
        rows.extend(data)
        page += 1
    screener = pd.DataFrame(rows)
    if holdings.empty or screener.empty:
        raise ValueError('etf/holdings(VONE) or company-screener returned no data')

    screener = screener.drop_duplicates('symbol').set_index('symbol')
    index_codes = screener.index.intersection(holdings['asset'].dropna().unique())
    foreign = screener.index[(screener['country'].fillna('US') != 'US') & (screener['marketCap'] >= foreign_min_cap)]
    info = screener.loc[index_codes.union(foreign)]
    ids = (holdings.sort_values('weightPercentage', ascending=False)
           .drop_duplicates('asset').set_index('asset').reindex(info.index))

    res = pd.DataFrame({
        'ts_code':  info.index,
        'name':     info['companyName'].values,
        'exchange': info['exchangeShortName'].values,
        'sector':   info['sector'].values,
        'industry': info['industry'].values,
        'country':  info['country'].values,
        'isin':     ids['isin'].replace('', None).values,
        'cusip':    ids['securityCusip'].replace('', None).values,
    }, columns=columns)
    if exchange and str(exchange).upper() not in ('ALL', 'NONE', ''):
        res = res[res['exchange'] == str(exchange).upper()]
    return res


def us_stock_daily(ts_code: str = None,
                   start: str = None,
                   end: str = None) -> pd.DataFrame:
    """美股日线行情(不复权，交易所原始价格)，FMP Unadjusted Stock Price API。

    [start, end] 左右闭。接口单次有行数上限，截断由下载器的二叉树验证处理。
    """
    if ts_code is None:
        return pd.DataFrame()
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-price-eod/non-split-adjusted', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-price-eod/non-split-adjusted', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    return pd.DataFrame({  # 接口字段名带 adj，但值是未复权的原始价格
        'ts_code':    ts_code,
        'trade_date': pd.to_datetime(raw['date']),
        'open':       raw['adjOpen'],
        'high':       raw['adjHigh'],
        'low':        raw['adjLow'],
        'close':      raw['adjClose'],
        'vol':        raw['volume'],
    })


def us_stock_daily_adj(ts_code: str = None,
                   trade_date: str = None,
                   start: str = None,
                   end: str = None) -> pd.DataFrame:
    """从 FMP Dividend-Adjusted Price Chart API 下载美股股票或 ETF 的日线行情(按拆股、分红复权)。

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

    trade_date = regulate_date_format(trade_date, force_format='date') if trade_date else None
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-price-eod/dividend-adjusted',
                 dict(ts_code=ts_code, trade_date=trade_date, start=start, end=end),
                 lambda ts_code, trade_date, start, end:  # 该接口没有单日参数，单日用 from = to 传
                 _fmp_get('historical-price-eod/dividend-adjusted', symbol=ts_code,
                          **{'from': start or trade_date, 'to': end or trade_date}),
                 fmt='%Y-%m-%d')
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


def us_fund_basic(ts_code: str = None, **_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """美股 ETF 基本信息(FMP etf/info)，一次一只。只存不随时间变的字段：规模、成交量、净值、持仓数等快照
    不存；行业分布(sectorsList)是一对多的数据，也不在这张表里。"""
    if ts_code is None:
        return pd.DataFrame()
    data = guard('etf/info', dict(ts_code=ts_code), lambda ts_code:
                 _fmp_get('etf/info', symbol=ts_code))
    if not data:
        return pd.DataFrame()
    i = data[0]
    return pd.DataFrame([{
        'ts_code':        i['symbol'],
        'name':           i.get('name'),
        'fund_type':      i.get('assetClass'),
        'management':     i.get('etfCompany'),
        'found_date':     pd.to_datetime(i.get('inceptionDate') or None),
        'expense_ratio':  i.get('expenseRatio'),
        'nav_currency':   i.get('navCurrency'),
        'domicile':       i.get('domicile'),
        'isin':           i.get('isin') or None,
        'cusip':          i.get('securityCusip') or None,
        'website':        i.get('website'),
        'description':    i.get('description'),
    }])


def us_treasury(start: str = None, end: str = None) -> pd.DataFrame:
    """美国国债收益率(%，FMP treasury-rates)，[start, end] 左右闭。

    接口按自然日截断：区间超过 90 天时不报错，静默只返回最后 90 天(行数远小于 100，guard 看不出来)，所以函数自己按
    90 个自然日分段请求；早年部分期限没有数据，为空。
    """
    if not (start and end):
        raise ValueError('us_treasury needs start and end')
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    data, seg_start = [], start
    while seg_start <= end:
        seg_end = min(seg_start + pd.Timedelta(days=89), end)
        data.extend(guard('treasury-rates', dict(), lambda:
                          _fmp_get('treasury-rates', **{'from': seg_start.strftime('%Y-%m-%d'),
                                                        'to': seg_end.strftime('%Y-%m-%d')})))
        seg_start = seg_end + pd.Timedelta(days=1)
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    tenors = {'month1': '1m', 'month2': '2m', 'month3': '3m', 'month6': '6m', 'year1': '1y', 'year2': '2y',
              'year3': '3y', 'year5': '5y', 'year7': '7y', 'year10': '10y', 'year20': '20y', 'year30': '30y'}
    res = raw.rename(columns=tenors)[['date', *tenors.values()]]
    res['date'] = pd.to_datetime(res['date'])
    return res


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
        items, page = [], 0
        while True:  # 该接口按 page 翻页，每页 10 条
            data = guard('analyst-estimates', dict(ts_code=ts_code), lambda ts_code:
                         _fmp_get('analyst-estimates', symbol=ts_code, period=period, page=page, limit=10))
            if not data:
                break
            items.extend(data)
            page += 1
        for item in items:
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


def _us_financials_common(endpoint: str, ts_code: str) -> list:
    """拉取 income/balance/cashflow 全部年报、季报，返回该股全部历史的 item 列表。

    FMP 财报接口只认 symbol/limit/period，不认日期、不翻页，单次最多 1000 条：按最大值取该股全部历史，返回满 1000 条
    说明可能没取全，报错。不按日期筛选：接口的发布日(filingDate)不可靠(有的等于报告期末)，按它筛选会漏掉晚收录的
    财报；拉到一只股票就写入全部，由主键去重。缺截止日(主键)或币种(金额无法解释)的记录是数据源自己的脏数据，
    丢弃并警告。
    """
    items = []
    for period in ('annual', 'quarter'):
        data = guard(endpoint, dict(ts_code=ts_code), lambda ts_code:
                     _fmp_get(endpoint, symbol=ts_code, period=period, limit=1000))
        if len(data) >= 1000:
            raise RuntimeError(f'{endpoint} {ts_code} {period} returned {len(data)} rows = api max, may be incomplete')
        for item in data:
            if len(item.get('date') or '') < 10 or not item.get('reportedCurrency'):
                warnings.warn(f'FMP {endpoint} {ts_code} {period}: dropped a record without date or reportedCurrency '
                              f'(date={item.get("date")!r}, fiscalYear={item.get("fiscalYear")!r})')
                continue
            item['_trade_date'] = pd.Timestamp(item['date'][:10])
            item['_period'] = 'Y' if period == 'annual' else 'Q'
            items.append(item)
    return items


def _us_latest_statement_codes(start: str, end: str) -> list:
    """最近有新财报的股票(FMP latest-financial-statements)：收录日(dateAdded)在 [start, end] 内、且在股票表里的代码。

    该接口只认 page/limit，不认日期，返回全球公司：从最新一页往回翻，翻到收录日早于 start 为止；读本地的依赖表
    us_stock_basic(map 第 8 列声明，下载前已更新)只留股票表里的代码。两端各放宽 1 天(收录时间的时区未知)，多拉的
    股票只是多写已有的行。只能回看最近一段时间：start 太早或翻到最后一页仍未到 start 时报错，路由退到逐股行。
    """
    page_size, max_page = 250, 100  # 每页 250 条、最多 101 页(page 0~100)，实测约覆盖最近 52 天
    max_days = 45  # start 早于这么多天前就不必尝试，直接报错
    if not start:
        raise ValueError('latest-financial-statements needs a start date')
    start_ts = pd.Timestamp(regulate_date_format(start, force_format='date')) - pd.Timedelta(days=1)
    end_ts = (pd.Timestamp(regulate_date_format(end, force_format='date')) if end
              else pd.Timestamp.now().normalize()) + pd.Timedelta(days=1)
    if start_ts < pd.Timestamp.now().normalize() - pd.Timedelta(days=max_days):
        raise RuntimeError(f'latest-financial-statements only covers the last {max_days} days, '
                           f'start={start} is too early')
    from qteasy import QT_DATA_SOURCE
    stock_codes = set(QT_DATA_SOURCE.read_table_data('us_stock_basic').index)
    if not stock_codes:
        raise RuntimeError('latest-financial-statements is filtered by us_stock_basic, which is empty')

    codes = set()
    for page in range(max_page + 1):
        data = guard('latest-financial-statements', dict(), lambda:
                     _fmp_get('latest-financial-statements', page=page, limit=page_size))
        if not data:
            raise RuntimeError(f'latest-financial-statements page {page} is empty before reaching start={start}')
        added = pd.to_datetime([r['dateAdded'][:10] for r in data])
        codes.update(r['symbol'] for r, d in zip(data, added)
                     if start_ts <= d <= end_ts and r['symbol'] in stock_codes)
        if added.min() < start_ts:
            return sorted(codes)
    raise RuntimeError(f'latest-financial-statements reached the last page before start={start}')


def _us_latest_statements(table: str, start: str, end: str) -> pd.DataFrame:
    """对最近有新财报的每只股票，用 table 的逐股行下载并合并返回；复用下载器的并行下载"""
    from qteasy import api_guard
    from qteasy.data_channels import fetch_batched_table_data, route_table_specs
    codes = _us_latest_statement_codes(start, end)
    if not codes:
        return pd.DataFrame()
    frames = [res['data'] for res in fetch_batched_table_data(
            table=table, channel='fmp', fetch_spec=route_table_specs('fmp', table, codes)[0],
            arg_list=[{'ts_code': ts_code} for ts_code in codes],
            download_batch_size=api_guard._throttle['size'],  # 沿用本次下载任务的节流设置
            download_batch_interval=api_guard._throttle['interval'],
    ) if not res['data'].empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def us_income_latest(start: str = None, end: str = None) -> pd.DataFrame:
    """最近有新财报的美股的利润表，见 _us_latest_statement_codes"""
    return _us_latest_statements('us_income', start, end)


def us_balance_latest(start: str = None, end: str = None) -> pd.DataFrame:
    """最近有新财报的美股的资产负债表，见 _us_latest_statement_codes"""
    return _us_latest_statements('us_balance', start, end)


def us_cashflow_latest(start: str = None, end: str = None) -> pd.DataFrame:
    """最近有新财报的美股的现金流量表，见 _us_latest_statement_codes"""
    return _us_latest_statements('us_cashflow', start, end)


def _us_dividend_frame(data: list, start: str, end: str) -> pd.DataFrame:
    """把 FMP 分红记录(dividends / dividends-calendar 字段相同)整理成 us_dividend 表，只留除息日在 [start, end] 内的。

    列名与 A 股 dividend 表相同含义的保持一致；cash_div_tax 为宣告时的原始每股金额(税前)，未按之后的拆股调整。除息日或金额(均为主键)缺失的记录是数据源自己的脏数据，丢弃并警告。
    """
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    dirty = raw['date'].fillna('').eq('') | raw['dividend'].isna()
    if dirty.any():
        warnings.warn(f'FMP dividends: dropped {dirty.sum()} records without ex-date or amount: '
                      f'{sorted(set(raw.loc[dirty, "symbol"]))[:5]}')
        raw = raw[~dirty]
    res = pd.DataFrame({
        'ts_code':     raw['symbol'],
        'ex_date':     pd.to_datetime(raw['date']),
        'cash_div_tax': raw['dividend'].astype('float64'),
        'ann_date':    pd.to_datetime(raw['declarationDate'].replace('', None)),
        'record_date': pd.to_datetime(raw['recordDate'].replace('', None)),
        'pay_date':    pd.to_datetime(raw['paymentDate'].replace('', None)),
    })
    if start:
        res = res[res['ex_date'] >= pd.Timestamp(regulate_date_format(start, force_format='date'))]
    if end:
        res = res[res['ex_date'] <= pd.Timestamp(regulate_date_format(end, force_format='date'))]
    return res.reset_index(drop=True)


def us_dividend(ts_code: str = None,
                start: str = None,
                end: str = None) -> pd.DataFrame:
    """单只美股的现金分红(FMP dividends)，返回除息日落在 [start, end] 内的记录。

    该接口只有 symbol、limit 两个参数(不认日期、不能翻页)，单次最多 1000 条：按最大值取该股全部历史，取回后再筛选；
    返回满 1000 条说明可能没取全，报错。
    """
    if ts_code is None:
        return pd.DataFrame()
    data = guard('dividends', dict(ts_code=ts_code), lambda ts_code:
                 _fmp_get('dividends', symbol=ts_code, limit=1000))
    if len(data) >= 1000:
        raise RuntimeError(f'dividends {ts_code} returned {len(data)} rows = api max, may be incomplete')
    return _us_dividend_frame(data, start, end)


def us_dividend_calendar(start: str = None,
                         end: str = None) -> pd.DataFrame:
    """全球股票的现金分红(FMP dividends-calendar)，返回除息日落在 [start, end] 内的记录，[start, end] 左右闭。

    接口返回全球所有交易所的股票，这里读本地的依赖表 us_stock_basic(map 第 8 列声明，下载前已更新)，只留股票表里的
    代码；股票表为空时报错。接口单次最多 4000 条、日期范围最多 90 天
    (map 第 7 列分段)；截断时会混入区间外的记录，取全后在这里筛掉。
    """
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('dividends-calendar', dict(start=start, end=end),
                 lambda start, end:
                 _fmp_get('dividends-calendar', **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    res = _us_dividend_frame(data, start, end)
    from qteasy import QT_DATA_SOURCE
    stock_codes = QT_DATA_SOURCE.read_table_data('us_stock_basic').index
    if stock_codes.empty:
        raise RuntimeError('us_dividend_calendar is filtered by us_stock_basic, which is empty')
    if res.empty:
        return res
    return res[res['ts_code'].isin(stock_codes)].reset_index(drop=True)


def us_income(ts_code: str = None,
              start: str = None,
              end: str = None) -> pd.DataFrame:
    """从 FMP income-statement 接口下载美股利润表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('income-statement', ts_code)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':              ts_code,
        'end_date':             i['_trade_date'],
        'period':               i['_period'],
        'filing_date':          pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':          i.get('fiscalYear', ''),
        'currency':             i['reportedCurrency'],
        'revenue':              i.get('revenue'),
        'oper_cost':            i.get('costOfRevenue'),
        'gross_profit':         i.get('grossProfit'),
        'rd_exp':               i.get('researchAndDevelopmentExpenses'),
        'sga_expense':          i.get('sellingGeneralAndAdministrativeExpenses'),
        'operating_expense':    i.get('operatingExpenses'),
        'cost_and_expense':     i.get('costAndExpenses'),
        'interest_income':      i.get('interestIncome'),
        'interest_expense':     i.get('interestExpense'),
        'da':                   i.get('depreciationAndAmortization'),
        'ebitda':               i.get('ebitda'),
        'ebit':                 i.get('ebit'),
        'operating_income':     i.get('operatingIncome'),
        'other_income':         i.get('totalOtherIncomeExpensesNet'),
        'total_profit':         i.get('incomeBeforeTax'),
        'income_tax':           i.get('incomeTaxExpense'),
        'continued_net_profit': i.get('netIncomeFromContinuingOperations'),
        'n_income_attr_p':      i.get('netIncome'),
        'basic_eps':            i.get('eps'),
        'diluted_eps':          i.get('epsDiluted'),
        'shares_out':           i.get('weightedAverageShsOut'),
        'shares_out_dil':       i.get('weightedAverageShsOutDil'),
    } for i in items]
    return pd.DataFrame(rows)  # 金额为报告原币，见 currency 列


def us_balance(ts_code: str = None,
               start: str = None,
               end: str = None) -> pd.DataFrame:
    """从 FMP balance-sheet-statement 接口下载美股资产负债表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('balance-sheet-statement', ts_code)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':                    ts_code,
        'end_date':                   i['_trade_date'],
        'period':                     i['_period'],
        'filing_date':                pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':                i.get('fiscalYear', ''),
        'currency':                   i['reportedCurrency'],
        'cash':                       i.get('cashAndCashEquivalents'),
        'st_investments':             i.get('shortTermInvestments'),
        'cash_and_st_inv':            i.get('cashAndShortTermInvestments'),
        'net_receivables':            i.get('netReceivables'),
        'inventories':                i.get('inventory'),
        'oth_cur_assets':             i.get('otherCurrentAssets'),
        'total_cur_assets':           i.get('totalCurrentAssets'),
        'ppe_net':                    i.get('propertyPlantEquipmentNet'),
        'goodwill':                   i.get('goodwill'),
        'intan_assets':               i.get('intangibleAssets'),
        'lt_investments':             i.get('longTermInvestments'),
        'defer_tax_assets':           i.get('taxAssets'),
        'oth_nca':                    i.get('otherNonCurrentAssets'),
        'total_nca':                  i.get('totalNonCurrentAssets'),
        'total_assets':               i.get('totalAssets'),
        'acct_payable':               i.get('accountPayables'),
        'st_debt':                    i.get('shortTermDebt'),
        'deferred_revenue':           i.get('deferredRevenue'),
        'oth_cur_liab':               i.get('otherCurrentLiabilities'),
        'total_cur_liab':             i.get('totalCurrentLiabilities'),
        'lt_debt':                    i.get('longTermDebt'),
        'oth_ncl':                    i.get('otherNonCurrentLiabilities'),
        'total_ncl':                  i.get('totalNonCurrentLiabilities'),
        'total_liab':                 i.get('totalLiabilities'),
        'common_stock':               i.get('commonStock'),
        'retained_earnings':          i.get('retainedEarnings'),
        'oth_comp_income':            i.get('accumulatedOtherComprehensiveIncomeLoss'),
        'total_hldr_eqy_exc_min_int': i.get('totalStockholdersEquity'),
        'minority_int':               i.get('minorityInterest'),
        'total_liab_hldr_eqy':        i.get('totalLiabilitiesAndTotalEquity'),
        'total_debt':                 i.get('totalDebt'),
        'net_debt':                   i.get('netDebt'),
    } for i in items]
    return pd.DataFrame(rows)  # 金额为报告原币，见 currency 列


def us_cashflow(ts_code: str = None,
                start: str = None,
                end: str = None) -> pd.DataFrame:
    """从 FMP cash-flow-statement 接口下载美股现金流量表。"""
    if ts_code is None:
        return pd.DataFrame()

    items = _us_financials_common('cash-flow-statement', ts_code)
    if not items:
        return pd.DataFrame()

    rows = [{
        'ts_code':                ts_code,
        'end_date':               i['_trade_date'],
        'period':                 i['_period'],
        'filing_date':            pd.Timestamp(i['filingDate'][:10]) if i.get('filingDate') else None,
        'fiscal_year':            i.get('fiscalYear', ''),
        'currency':               i['reportedCurrency'],
        'net_income':             i.get('netIncome'),
        'da':                     i.get('depreciationAndAmortization'),
        'deferred_tax':           i.get('deferredIncomeTax'),
        'sbc':                    i.get('stockBasedCompensation'),
        'chg_working_capital':    i.get('changeInWorkingCapital'),
        'other_non_cash':         i.get('otherNonCashItems'),
        'n_cashflow_act':         i.get('netCashProvidedByOperatingActivities'),
        'capex':                  i.get('investmentsInPropertyPlantAndEquipment'),
        'acquisitions':           i.get('acquisitionsNet'),
        'purchases_inv':          i.get('purchasesOfInvestments'),
        'c_disp_withdrwl_invest': i.get('salesMaturitiesOfInvestments'),
        'other_investing':        i.get('otherInvestingActivities'),
        'n_cashflow_inv_act':     i.get('netCashProvidedByInvestingActivities'),
        'net_debt_issuance':      i.get('netDebtIssuance'),
        'net_stock_issuance':     i.get('netStockIssuance'),
        'dividends_paid':         i.get('netDividendsPaid'),
        'other_financing':        i.get('otherFinancingActivities'),
        'n_cash_flows_fnc_act':   i.get('netCashProvidedByFinancingActivities'),
        'eff_fx_flu_cash':        i.get('effectOfForexChangesOnCash'),
        'n_incr_cash_cash_equ':   i.get('netChangeInCash'),
        'c_cash_equ_end_period':  i.get('cashAtEndOfPeriod'),
        'ocf':                    i.get('operatingCashFlow'),
        'fcf':                    i.get('freeCashFlow'),
        'income_tax_paid':        i.get('incomeTaxesPaid'),
        'interest_paid':          i.get('interestPaid'),
    } for i in items]
    return pd.DataFrame(rows)  # 金额为报告原币，见 currency 列
