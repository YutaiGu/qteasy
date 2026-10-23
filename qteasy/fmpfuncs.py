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
import warnings
import requests
import pandas as pd

from qteasy._arg_validators import QT_CONFIG
from qteasy.__init__ import logger_core
from qteasy.utilfuncs import regulate_date_format, to_timezone_datetime
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


class APIError(RuntimeError):
    """FMP 请求失败。按 HTTP 状态码和返回内容归类到 kind(FMP 出错时有时仍返回 200，错误信息在 body 里)：
    rate_limit 撞限 / auth 密钥无效 / plan 套餐不含该接口或参数 / not_found 无此接口或代码 /
    server 服务端或网络故障(status 为 None) / bad_body 返回了非数据的内容
    """
    def __init__(self, endpoint: str, status: int = None, text: str = ''):
        if status is None or status >= 500:
            kind = 'server'
        elif status == 429 or 'Limit Reach' in text:
            kind = 'rate_limit'
        elif status == 401 or 'Invalid API KEY' in text:
            kind = 'auth'
        elif status in (402, 403) or any(k in text for k in ('Premium', 'subscription', 'Restricted', 'Upgrade')):
            kind = 'plan'
        elif status == 404:
            kind = 'not_found'
        else:
            kind = 'bad_body'
        super().__init__(f'[{kind}] FMP {endpoint} HTTP {status}: {text}')
        self.kind, self.status = kind, status


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
                body = resp.json() if resp.ok else None
                if isinstance(body, list):  # 正常数据一律是 list
                    return body
                error = APIError(endpoint, resp.status_code, str(body) if body is not None else resp.text[:300])
                if error.kind == 'rate_limit':  # 每分钟超限：等待后重试同一请求，不计入普通重试次数
                    if limit_hits == len(_RATE_LIMIT_WAITS):
                        raise error
                    time.sleep(_RATE_LIMIT_WAITS[limit_hits])
                    limit_hits += 1
                    continue
                if resp.status_code not in retry_statuses or attempt == len(retry_delays):
                    raise error
        except requests.exceptions.RequestException:
            if attempt == len(retry_delays):
                raise APIError(endpoint, None, 'request failed after 4 retries')
        time.sleep(retry_delays[attempt])
        attempt += 1


def us_index_basic(**_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """美股指数基本信息(FMP index-list)，只留 USD 计价的指数。"""
    raw = pd.DataFrame(guard('index-list', dict(), lambda: _fmp_get('index-list')))
    if raw.empty:
        raise ValueError('index-list returned no data')
    raw = raw[raw['currency'] == 'USD']
    return pd.DataFrame({
        'ts_code':  raw['symbol'].values,
        'name':     raw['name'].values,
        'exchange': raw['exchange'].values,
        'currency': raw['currency'].values,
    })


def fx_basic(**_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """外汇货币对基本信息(FMP forex-list)，只留对 USD 报价的货币对：库里的外币金额都是折成 USD 用的。"""
    raw = pd.DataFrame(guard('forex-list', dict(), lambda: _fmp_get('forex-list')))
    if raw.empty:
        raise ValueError('forex-list returned no data')
    raw = raw[raw['toCurrency'] == 'USD']
    return pd.DataFrame({
        'ts_code':       raw['symbol'].values,
        'from_currency': raw['fromCurrency'].values,
        'to_currency':   raw['toCurrency'].values,
        'from_name':     raw['fromName'].values,
        'to_name':       raw['toName'].values,
    })


def fx_daily(ts_code: str = None,
             start: str = None,
             end: str = None) -> pd.DataFrame:
    """外汇日线行情(FMP historical-price-eod/full)，[start, end] 左右闭。"""
    if ts_code is None:
        return pd.DataFrame()
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-price-eod/full', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-price-eod/full', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_date': pd.to_datetime(raw['date']),
        'open':       raw['open'],
        'high':       raw['high'],
        'low':        raw['low'],
        'close':      raw['close'],
        'vol':        raw['volume'],
    })


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
    """美股股票池基本信息：(VONE 持仓 ∩ screener) ∪ 在美国上市、市值 ≥ $2B 的外国公司(含 ADR)。

    VONE(Vanguard Russell 1000 ETF)跟踪 Russell 1000(美国公司，按市值取前约 1000 只，无盈利等偏好)；持仓与
    screener(三大交易所、在交易、非 ETF/基金、含全部股份类别)交叉认证，去掉现金、CVR、托管、OTC 等非交易所股票。
    名称、交易所、行业、国家取自 screener；ISIN、CUSIP 取自 VONE 持仓(外国公司部分为空)，同一代码有多行时
    (正股与附带的 CVR/权证共用代码)取权重最大的一行，即正股本身。
    exchange 为 ALL / 空时不过滤，否则只返回该交易所的股票。
    """
    columns = ['ts_code', 'name', 'exchange', 'sector', 'industry', 'country', 'isin', 'cusip']
    foreign_min_cap = 2_000_000_000   # 约 140 亿人民币；20261011 由 $5B 下调，外国公司 330 → 466 只

    holdings = pd.DataFrame(guard('etf/holdings', dict(), lambda:
                                  _fmp_get('etf/holdings', symbol='VONE')))
    rows, page = [], 0
    while True:  # company-screener 按 page 翻到空页，页大小由接口默认
        data = guard('company-screener', dict(), lambda:
                     _fmp_get('company-screener', exchange='NASDAQ,NYSE,AMEX', isEtf='false', isFund='false',
                              isActivelyTrading='true', includeAllShareClasses='true', page=page))
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


def hk_stock_basic(**_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """港股股票池基本信息：HKSE 在交易、非 ETF/基金的股票中，有分析师预期(未来目标期)的那些。

    池子跟着预期走：这套表是为 estimates 建的，只收有人覆盖的股票：任一未来目标期 EPS 预测人数 > 1(只有 1 人的
    不是共识，约占一半)。候选来自 screener(约 2600 只)，每只查一次 analyst-estimates；通过的再查 profile 取
    ISIN、CUSIP 和交易币种：双柜台(如 89988.HK 人民币柜台)与港币柜台
    ISIN 相同，同一 ISIN 优先留 currency 为 HKD 的那个。缺 ISIN 的丢弃并警告。列与 us_stock_basic 相同。
    一次刷新约 3600 次调用：本地表非空时不调接口、不写入，返回空；要重建先清表。
    """
    from qteasy import QT_DATA_SOURCE
    if not QT_DATA_SOURCE.read_table_data('hk_stock_basic').empty:
        logger_core.info('hk_stock_basic is not empty, skipped; clear the table to rebuild')
        return pd.DataFrame()
    columns = ['ts_code', 'name', 'exchange', 'sector', 'industry', 'country', 'isin', 'cusip']
    today = pd.Timestamp.now(tz='America/New_York').strftime('%Y-%m-%d')
    trading_currency = 'HKD'
    min_analysts = 2

    rows, page = [], 0
    while True:
        data = guard('company-screener', dict(), lambda:
                     _fmp_get('company-screener', exchange='HKSE', isEtf='false', isFund='false',
                              isActivelyTrading='true', page=page))
        if not data:
            break
        rows.extend(data)
        page += 1
    screener = pd.DataFrame(rows)
    if screener.empty:
        raise ValueError('company-screener(HKSE) returned no data')
    screener = screener.drop_duplicates('symbol').set_index('symbol')

    covered = []
    for symbol in screener.index:
        data = guard('analyst-estimates', dict(ts_code=symbol), lambda ts_code:
                     _fmp_get('analyst-estimates', symbol=ts_code, period='annual', page=0, limit=1000))
        if any((item.get('date') or '') >= today and (item.get('numAnalystsEps') or 0) >= min_analysts
               for item in data):
            covered.append(symbol)
    profiles = pd.DataFrame([item for symbol in covered for item in
                             guard('profile', dict(ts_code=symbol), lambda ts_code: _fmp_get('profile', symbol=ts_code))])
    profiles = profiles.drop_duplicates('symbol').set_index('symbol').reindex(covered)
    dirty = profiles['isin'].fillna('').eq('')
    if dirty.any():
        warnings.warn(f'FMP profile(HKSE): dropped {dirty.sum()} stocks without isin: {list(profiles.index[dirty])[:5]}')
    kept = profiles[~dirty]
    prefer = (kept.assign(_hkd=kept['currency'] == trading_currency)
              .sort_values('_hkd', ascending=False, kind='stable').drop_duplicates('isin'))
    kept = kept[kept.index.isin(prefer.index)]
    info = screener.loc[kept.index]

    return pd.DataFrame({
        'ts_code':  info.index,
        'name':     info['companyName'].values,
        'exchange': info['exchangeShortName'].values,
        'sector':   info['sector'].values,
        'industry': info['industry'].values,
        'country':  info['country'].values,
        'isin':     kept['isin'].values,
        'cusip':    kept['cusip'].replace('', None).values,
    }, columns=columns)


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


def index_5min(ts_code: str = None,
               start: str = None,
               end: str = None) -> pd.DataFrame:
    """指数 5 分钟 K 线(FMP historical-chart/5min)，[start, end] 左右闭。

    FMP 代码：.SH → .SS，.SZ 不变；入库仍用 tushare 代码。接口只返回 to 往前 10 个自然日，更早的静默丢弃，
    由 map 第 7 列按 9 天分段保证取全。时间为交易所当地时间，原样入库；成交额接口没有，留空。
    """
    if ts_code is None:
        return pd.DataFrame()
    symbol = ts_code[:-3] + '.SS' if ts_code.endswith('.SH') else ts_code
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-chart/5min', dict(ts_code=symbol, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-chart/5min', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_time': pd.to_datetime(raw['date']),
        'open':       raw['open'],
        'high':       raw['high'],
        'low':        raw['low'],
        'close':      raw['close'],
        'vol':        raw['volume'],
    })


def index_1min(ts_code: str = None,
               start: str = None,
               end: str = None) -> pd.DataFrame:
    """1 分钟 K 线(FMP historical-chart/1min)，[start, end] 左右闭；指数、ETF 同一个接口，加密货币见 crypto_1min。

    FMP 代码：.SH → .SS，.SZ 不变；入库仍用 tushare 代码。接口只返回 to 当天和之前 2 个自然日，更早的静默丢弃，
    由 map 第 7 列按 2 天分段保证取全。时间为交易所当地时间；成交额接口没有，留空。
    bar 时刻：美股按开始时刻原样入库(09:30–15:59)。A 股按国内惯例(通达信、tushare)标结束时刻：接口标开始时刻，每天 242 根，
    09:30 那根含开盘集合竞价，另有 11:30(上午收盘那几秒的成交)、15:00(收盘集合竞价)两根；每根 +1 分钟，11:31、15:01
    并回 11:30、15:00，得 09:31–11:30、13:01–15:00 共 240 根。tushare 单列的 09:30 竞价拆不出，在 09:31 那根里。
    """
    if ts_code is None:
        return pd.DataFrame()
    symbol = ts_code[:-3] + '.SS' if ts_code.endswith('.SH') else ts_code
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-chart/1min', dict(ts_code=symbol, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-chart/1min', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    res = pd.DataFrame({
        'ts_code':    ts_code,
        'trade_time': pd.to_datetime(raw['date']),
        'open':       raw['open'],
        'high':       raw['high'],
        'low':        raw['low'],
        'close':      raw['close'],
        'vol':        raw['volume'],
    })
    if not ts_code.endswith(('.SH', '.SZ')):
        return res
    res = res.sort_values('trade_time')   # 合并的两根按原始先后取 open / close
    t = res['trade_time'] + pd.Timedelta(minutes=1)
    hm = t.dt.strftime('%H:%M')
    t = t.mask(hm.isin(['11:31', '15:01']), t - pd.Timedelta(minutes=1))
    return (res.assign(trade_time=t)
            .groupby(['ts_code', 'trade_time'], as_index=False)
            .agg(open=('open', 'first'), high=('high', 'max'), low=('low', 'min'), close=('close', 'last'),
                 vol=('vol', 'sum')))


def crypto_1min(ts_code: str = None,
                start: str = None,
                end: str = None) -> pd.DataFrame:
    """加密货币 1 分钟 K 线(FMP historical-chart/1min)，[start, end] 左右闭，按 UTC 入库(加密货币行业惯例，全天交易无收盘)。

    接口给美东当地时间(不带时区)，换算到 UTC。夏令时回拨那天美东 01:xx 出现两次，接口只给第一次(夏令时，实测 00:59→01:00
    价格连续、01:59→02:00 跳空)，第二次的 60 根源头就没有；前拨那天 02:xx 不存在，接口也不给。
    start/end 按美东日期传给接口，换算后两端各差 4~5 小时，下一段会补上。
    """
    if ts_code is None:
        return pd.DataFrame()
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-chart/1min', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-chart/1min', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_time': to_timezone_datetime(raw['date'], 'UTC', naive_time_zone='America/New_York', ambiguous=True),
        'open':       raw['open'],
        'high':       raw['high'],
        'low':        raw['low'],
        'close':      raw['close'],
        'vol':        raw['volume'],
    })


def _fmp_indicator(ts_code: str, start: str, end: str, daily_table: str) -> pd.DataFrame:
    """每日市值(FMP historical-market-capitalization，交易币种)，[start, end] 左右闭；总股本 = 市值 / 本地日线表的
    当日收盘价(不复权)。FMP 的市值是股价 × 最近一期财报股本，拆股日调整、财报发布日切换，期中增发反映不出来。
    其他列(复权因子、流通股本、换手率)FMP 没有，留空。
    """
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('historical-market-capitalization', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _fmp_get('historical-market-capitalization', symbol=ts_code, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    from qteasy import QT_DATA_SOURCE
    res = pd.DataFrame({'ts_code': ts_code, 'trade_date': pd.to_datetime(pd.DataFrame(data)['date']),
                        'total_mv': pd.DataFrame(data)['marketCap'].astype('float64')})
    daily = QT_DATA_SOURCE.read_table_data(daily_table, shares=ts_code, start=start, end=end,
                                           primary_key_in_index=False)
    if not daily.empty:
        close = daily.assign(trade_date=pd.to_datetime(daily['trade_date'])).set_index('trade_date')['close']
        res['total_share'] = res['total_mv'] / res['trade_date'].map(close).astype('float64')
    return res


def us_stock_indicator(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    if ts_code is None:
        return pd.DataFrame()
    return _fmp_indicator(ts_code, start, end, 'us_stock_daily')


def hk_stock_indicator(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    if ts_code is None:
        return pd.DataFrame()
    return _fmp_indicator(ts_code, start, end, 'hk_stock_daily')


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


def us_stock_adj_factor(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """美股除权比率(与 hk_stock_adj_factor 同一张表的形式)：只有事件日有行，adj_ratio = 当天 ÷ 前一天的复权倍数，
    复权因子 = 比率从任一起点往后累乘。没有基准日，所以不必从上市第一天算起，更早的事件以后补上也不改已有的行。

    只按本地表算，不调 FMP：拆股表 us_split 的比例 r = 拆后 ÷ 拆前，分红表 us_dividend 按 前收 ÷ (前收 − 股息)，
    同一天两者相乘。FMP 把分拆也记进拆股表，且有的分拆它把"不复权"价往回除过了(没有标记、前后不一致)，这种再乘 r
    就重复调整；用 us_stock_indicator.total_share(Sharadar 真实市值 ÷ 本地 FMP 价)在事件日前后的变化分辨：
        股数 × r   → 真拆股，乘 r
        股数不变   → 分拆、价格没被调，乘 r
        股数 × 1/r → 分拆、价格已被 FMP 往回除，不乘(不写行)
    没有股数可比的(2021-10 以前、次要类别)一律乘 r。非交易日的事件归到下一个交易日；前收取不到的分红不写，下次再来。
    """
    if ts_code is None:
        return pd.DataFrame()
    from qteasy import QT_DATA_SOURCE as ds
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    raw = ds.read_table_data('us_stock_daily', shares=ts_code, primary_key_in_index=False)
    if raw.empty:
        return pd.DataFrame()
    close = raw.assign(trade_date=pd.to_datetime(raw['trade_date'])).set_index('trade_date')['close'].sort_index()
    shares = ds.read_table_data('us_stock_indicator', shares=ts_code, primary_key_in_index=False)
    shares = (shares.assign(trade_date=pd.to_datetime(shares['trade_date'])).set_index('trade_date')['total_share']
              .pipe(pd.to_numeric, errors='coerce').dropna().sort_index()) if not shares.empty \
              else pd.Series(dtype='float64', index=pd.DatetimeIndex([]))

    def on_day(dates):  # 非交易日的事件归到下一个交易日
        pos = close.index.searchsorted(pd.to_datetime(dates))
        return close.index[pos.clip(max=len(close) - 1)]
    ratio = {}
    spl = ds.read_table_data('us_split', shares=ts_code, start=start, end=end, primary_key_in_index=False)
    for t, r in zip(on_day(spl['ex_date']), spl['numerator'] / spl['denominator']) if not spl.empty else []:
        before, after = shares[shares.index < t], shares[shares.index >= t]
        if len(before) and len(after) and abs(after.iloc[0] / before.iloc[-1] * r - 1) < 0.02:
            continue  # 分拆且价格已被往回除
        ratio[t] = ratio.get(t, 1.0) * r
    div = ds.read_table_data('us_dividend', shares=ts_code, start=start, end=end, primary_key_in_index=False)
    for t, d in (div.groupby(on_day(div['ex_date']))['cash_div_tax'].sum().items() if not div.empty else []):
        i = close.index.get_loc(t)
        if i == 0 or not close.iloc[i - 1] > d > 0:
            continue
        ratio[t] = ratio.get(t, 1.0) * close.iloc[i - 1] / (close.iloc[i - 1] - d)
    if not ratio:
        return pd.DataFrame()
    res = pd.DataFrame(sorted(ratio.items()), columns=['trade_date', 'adj_ratio'])
    res.insert(0, 'ts_code', ts_code)
    return res


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
    """美国国债收益率(%，FMP treasury-rates)，[start, end] 左右闭，区间最多 90 天(文档，map 第 7 列分段)。
    早年部分期限没有数据，为空。
    """
    if not (start and end):
        raise ValueError('us_treasury needs start and end')
    start, end = pd.Timestamp(start).strftime('%Y-%m-%d'), pd.Timestamp(end).strftime('%Y-%m-%d')
    data = guard('treasury-rates', dict(start=start, end=end), lambda start, end:
                 _fmp_get('treasury-rates', **{'from': start, 'to': end}), fmt='%Y-%m-%d')
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    tenors = {'month1': '1m', 'month2': '2m', 'month3': '3m', 'month6': '6m', 'year1': '1y', 'year2': '2y',
              'year3': '3y', 'year5': '5y', 'year7': '7y', 'year10': '10y', 'year20': '20y', 'year30': '30y'}
    res = raw.rename(columns=tenors)[['date', *tenors.values()]]
    res['date'] = pd.to_datetime(res['date'])
    return res


def period_label(dates, annual_date) -> pd.Series:
    """预测期的名字(与财报 end_date 同义)：年报那一期是四季报记 FY-12-31，往前依次 09-30 / 06-30 / 03-31。

    dates 是各期的结账日，annual_date 是这只股票任一年度预测的结账日；只看结账日之间相差几个月，不查公司几月结年：
    距年报 0 个月(或 12 的倍数)是 Q4，差 3 个月是 Q3，6 是 Q2，9 是 Q1；财年 = 该期所属年报结账日所在年。
    结账日落在月初 7 天内的(52/53 周财年跨月)按上月算。
    """
    d = pd.to_datetime(pd.Series(dates)) - pd.Timedelta(days=7)
    a = pd.Timestamp(annual_date) - pd.Timedelta(days=7)
    m = d.dt.year * 12 + d.dt.month
    n = ((a.year * 12 + a.month - m) % 12 / 3).round().astype(int) % 4   # 距年报几个季：0 = Q4
    fy_end = m + n * 3                                                   # 该期所属年报的结账月
    fy, q = (fy_end - 1) // 12, 4 - n
    return pd.to_datetime(fy.astype(str) + '-' + (q * 3).astype(str).str.zfill(2) + '-01') + pd.offsets.MonthEnd(0)


def _fmp_estimates(ts_code: str, income_table: str, db) -> pd.DataFrame:
    """单只股票的分析师一致预期快照(FMP analyst-estimates)，金额为财报申报币种，经 db.changelog 只留相对库内上一版
    有实质变化的行。美股、港股共用，income_table / db 决定市场。

    只留期末日晚于最新已披露财报的期：财年结束到财报发布之间共识仍有效，不能按今天切。最新财报及其币种读本地依赖表
    income_table(map 第 8 列声明，下载前已更新)；表里没有这只股(FMP 没有它的财报)则没有币种，跳过并警告。接口按 page
    翻到空页，limit 最大 1000；quarter 不在套餐内时跳过并警告，annual 照常。
    FMP 的 date 是各期结账日(未披露时是 12-30 / 03-30 这类近似日)，target_date 存报告期的名字(period_label，按它自己的年度预测定
    第几季)，与财报 end_date 同义；没有年度预测就没法定名字，整只跳过并警告。
    FMP 对没人预测的指标填 0 而不是空：numAnalystsEps 为 0 时 eps/net_profit 置空，numAnalystsRevenue 为 0 时
    revenue 置空，两者都为 0 的整条丢弃。另有人数不为 0 但 eps 和 net_profit 同时为 0 的(如腾讯 2030 年度，25 人)，
    也是没有预测，同样置空。缺 date(主键)的记录是数据源自己的脏数据，丢弃并警告。
    数值列不信任接口回传的类型，按 SCHEMA 转换后再交 changelog 比较。
    """
    from qteasy import QT_DATA_SOURCE
    income = QT_DATA_SOURCE.read_table_data(income_table, shares=ts_code, primary_key_in_index=False)
    if income.empty:
        warnings.warn(f'FMP analyst-estimates {ts_code}: not in {income_table}, currency unknown, skipped')
        return pd.DataFrame(columns=db.COLUMNS)
    latest = income.sort_values('end_date').iloc[-1]
    reported, currency = pd.Timestamp(latest['end_date']), latest['currency']   # end_date 是报告期的名字
    fmp_map = {
        'epsAvg': 'eps', 'epsHigh': 'eps_high', 'epsLow': 'eps_low',
        'revenueAvg': 'revenue', 'revenueHigh': 'revenue_high', 'revenueLow': 'revenue_low',
        'netIncomeAvg': 'net_profit', 'netIncomeHigh': 'net_profit_high', 'netIncomeLow': 'net_profit_low',
        'ebitdaAvg': 'ebitda', 'ebitdaHigh': 'ebitda_high', 'ebitdaLow': 'ebitda_low',
        'ebitAvg': 'ebit', 'ebitHigh': 'ebit_high', 'ebitLow': 'ebit_low',
        'sgaExpenseAvg': 'sga_expense', 'sgaExpenseHigh': 'sga_expense_high', 'sgaExpenseLow': 'sga_expense_low',
        'numAnalystsEps': 'num_analysts_eps', 'numAnalystsRevenue': 'num_analysts_revenue',
    }

    frames = []
    for period in ('annual', 'quarter'):
        items, page = [], 0
        while True:
            try:
                data = guard('analyst-estimates', dict(ts_code=ts_code), lambda ts_code:
                             _fmp_get('analyst-estimates', symbol=ts_code, period=period, page=page, limit=1000))
            except APIError as e:
                if period == 'quarter' and e.kind == 'plan':
                    warnings.warn(f'FMP analyst-estimates {ts_code}: quarter not in plan, skipped ({e})')
                    items = []
                    break
                raise
            if not data:
                break
            items.extend(data)
            page += 1
        if items:
            frames.append(pd.DataFrame(items).assign(target_period='Y' if period == 'annual' else 'Q'))
    if not frames:
        return pd.DataFrame(columns=db.COLUMNS)
    raw = pd.concat([f.dropna(axis=1, how='all') for f in frames], ignore_index=True)  # 全空列不参与类型推断，后面 reindex 补回
    if (raw['target_period'] == 'Y').sum() == 0:
        warnings.warn(f'FMP analyst-estimates {ts_code}: no annual estimate, periods can not be named, skipped')
        return pd.DataFrame(columns=db.COLUMNS)
    dirty = raw['date'].fillna('').str.len() < 10
    if dirty.any():
        warnings.warn(f'FMP analyst-estimates {ts_code}: dropped {dirty.sum()} records without date')
        raw = raw[~dirty]

    res = raw.rename(columns=fmp_map).assign(
        ts_code=ts_code,
        trade_date=pd.Timestamp.now(tz='America/New_York').normalize().tz_localize(None),
        target_date=period_label(raw['date'].str[:10], raw.loc[raw['target_period'] == 'Y', 'date'].str[:10].max()).values,
        currency=currency,
    )
    res = res[res['target_date'] > reported].reindex(columns=db.COLUMNS)
    for col, dt in db.SCHEMA.items():
        if dt == 'date':
            res[col] = pd.to_datetime(res[col], errors='raise')
        elif dt == 'double':
            res[col] = pd.to_numeric(res[col], errors='raise')
        elif dt == 'int':
            res[col] = pd.to_numeric(res[col], errors='raise')
            if (res[col].dropna() % 1 != 0).any():
                raise ValueError(f'FMP analyst-estimates {ts_code}: non-integer value in {col}')
    no_eps = (res['num_analysts_eps'].fillna(0) == 0) | ((res['eps'] == 0) & (res['net_profit'] == 0))
    no_rev = res['num_analysts_revenue'].fillna(0) == 0
    res.loc[no_eps, ['eps', 'eps_high', 'eps_low', 'net_profit', 'net_profit_high', 'net_profit_low']] = None
    res.loc[no_rev, ['revenue', 'revenue_high', 'revenue_low']] = None
    res = res[~(no_eps & no_rev)]
    return db.changelog(res)


def us_estimates(ts_code: str = None, **_) -> pd.DataFrame:
    if ts_code is None:
        return pd.DataFrame()
    from .us_estimates_db import UsEstimateDatabase
    return _fmp_estimates(ts_code, 'us_income', UsEstimateDatabase())


def hk_estimates(ts_code: str = None, **_) -> pd.DataFrame:
    if ts_code is None:
        return pd.DataFrame()
    from .us_estimates_db import HkEstimateDatabase
    return _fmp_estimates(ts_code, 'hk_income', HkEstimateDatabase())


def estimates_fmp(ts_code: str = None, **_) -> pd.DataFrame:
    """A 股的 FMP 季度预期快照，只取 quarter：年度共识用 tushare report_rc 加权算的 estimates，FMP 人数少得多；
    季度 tushare 几乎没有，FMP 的作补充。写入 estimates_fmp，经 changelog 只留有实质变化的行。

    FMP 代码：.SH → .SS，.SZ 不变；入库仍用 tushare 代码。币种恒为 CNY。只留期末日晚于最新已披露财报的期，
    最新财报读本地 income(tushare 的表，先更新好)，没有这只股则跳过并警告。接口按 page 翻到空页，limit 最大 1000。
    date 取月末；没人预测的指标 FMP 填 0：按人数为 0、或 eps 与 net_profit 同时为 0 置空，两个人数都为 0 整条丢弃；
    缺 date 的记录丢弃并警告；数值列按 SCHEMA 转换后再比较。
    """
    if ts_code is None:
        return pd.DataFrame()
    from .us_estimates_db import AFmpEstimateDatabase
    from qteasy import QT_DATA_SOURCE
    db = AFmpEstimateDatabase()
    symbol = ts_code[:-3] + '.SS' if ts_code.endswith('.SH') else ts_code
    fmp_map = {
        'epsAvg': 'eps', 'epsHigh': 'eps_high', 'epsLow': 'eps_low',
        'revenueAvg': 'revenue', 'revenueHigh': 'revenue_high', 'revenueLow': 'revenue_low',
        'netIncomeAvg': 'net_profit', 'netIncomeHigh': 'net_profit_high', 'netIncomeLow': 'net_profit_low',
        'ebitdaAvg': 'ebitda', 'ebitdaHigh': 'ebitda_high', 'ebitdaLow': 'ebitda_low',
        'ebitAvg': 'ebit', 'ebitHigh': 'ebit_high', 'ebitLow': 'ebit_low',
        'sgaExpenseAvg': 'sga_expense', 'sgaExpenseHigh': 'sga_expense_high', 'sgaExpenseLow': 'sga_expense_low',
        'numAnalystsEps': 'num_analysts_eps', 'numAnalystsRevenue': 'num_analysts_revenue',
    }
    income = QT_DATA_SOURCE.read_table_data('income', shares=ts_code, primary_key_in_index=False)
    if income.empty:
        warnings.warn(f'FMP analyst-estimates {ts_code}: not in income, latest reported period unknown, skipped')
        return pd.DataFrame(columns=db.COLUMNS)
    reported = pd.to_datetime(income['end_date']).max() + pd.offsets.MonthEnd(0)

    items, page = [], 0
    while True:
        data = guard('analyst-estimates', dict(ts_code=symbol), lambda ts_code:
                     _fmp_get('analyst-estimates', symbol=ts_code, period='quarter', page=page, limit=1000))
        if not data:
            break
        items.extend(data)
        page += 1
    if not items:
        return pd.DataFrame(columns=db.COLUMNS)
    raw = pd.DataFrame(items)
    dirty = raw['date'].fillna('').str.len() < 10
    if dirty.any():
        warnings.warn(f'FMP analyst-estimates {ts_code}: dropped {dirty.sum()} records without date')
        raw = raw[~dirty]
    res = raw.rename(columns=fmp_map).assign(
        ts_code=ts_code,
        trade_date=pd.Timestamp.now(tz='America/New_York').normalize().tz_localize(None),
        target_date=pd.to_datetime(raw['date'].str[:10]) + pd.offsets.MonthEnd(0),
        target_period='Q',
        currency='CNY',
    )
    res = res[res['target_date'] > reported].reindex(columns=db.COLUMNS)
    for col, dt in db.SCHEMA.items():
        if dt == 'date':
            res[col] = pd.to_datetime(res[col], errors='raise')
        elif dt in ('double', 'int'):
            res[col] = pd.to_numeric(res[col], errors='raise')
    no_eps = (res['num_analysts_eps'].fillna(0) == 0) | ((res['eps'] == 0) & (res['net_profit'] == 0))
    no_rev = res['num_analysts_revenue'].fillna(0) == 0
    res.loc[no_eps, ['eps', 'eps_high', 'eps_low', 'net_profit', 'net_profit_high', 'net_profit_low']] = None
    res.loc[no_rev, ['revenue', 'revenue_high', 'revenue_low']] = None
    return db.changelog(res[~(no_eps & no_rev)])


def _us_financials_common(endpoint: str, ts_code: str) -> list:
    """拉取 income/balance/cashflow 全部年报、季报，返回该股全部历史的 item 列表。

    end_date 是报告期标识，与 A 股同义：财年第 N 季记为 fiscalYear 年的 N 季末(Q1 03-31 … Q4/FY 12-31)，不是实际截止日；
    实际截止日(FMP 的 date，如 AAPL 06-27)另存 period_end。缺 fiscalYear/period 的记录无法定期，丢弃并警告。
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
            q = _QUARTER_END.get(item.get('period'))
            if q is None or not str(item.get('fiscalYear', '')).isdigit():
                warnings.warn(f'FMP {endpoint} {ts_code} {period}: dropped a record without fiscalYear/period '
                              f'(date={item.get("date")!r}, fiscalYear={item.get("fiscalYear")!r}, period={item.get("period")!r})')
                continue
            item['_end_date'] = pd.Timestamp(f'{int(item["fiscalYear"])}-{q}')   # 报告期标识，与 A 股同义：财年第 N 季 → N 季末
            item['_period_end'] = pd.Timestamp(item['date'][:10])              # 实际截止日
            item['_period'] = 'Y' if period == 'annual' else 'Q'
            items.append(item)
    return items


_QUARTER_END = {'Q1': '03-31', 'Q2': '06-30', 'Q3': '09-30', 'Q4': '12-31', 'FY': '12-31'}


def _us_latest_statement_codes(start: str, end: str) -> list:
    """最近有新财报的股票(FMP latest-financial-statements)：收录日(dateAdded)在 [start, end] 内、且在股票表里的代码。

    该接口只认 page/limit，不认日期，返回全球公司：从最新一页往回翻，翻到收录日早于 start 为止；读本地的依赖表
    us_stock_basic(map 第 8 列声明，下载前已更新)只留股票表里的代码。两端各放宽 1 天(收录时间的时区未知)，多拉的
    股票只是多写已有的行。只能回看最近一段时间：翻到最后一页仍未到 start 时报错。
    """
    max_page = 100   # 文档：page 最大 100；页大小由接口默认(250，也是文档上限)
    if not start:
        raise ValueError('latest-financial-statements needs a start date')
    start_ts = pd.Timestamp(regulate_date_format(start, force_format='date')) - pd.Timedelta(days=1)
    end_ts = (pd.Timestamp(regulate_date_format(end, force_format='date')) if end
              else pd.Timestamp.now(tz='America/New_York').normalize().tz_localize(None)) + pd.Timedelta(days=1)
    from qteasy import QT_DATA_SOURCE
    stock_codes = set(QT_DATA_SOURCE.read_table_data('us_stock_basic').index)
    if not stock_codes:
        raise RuntimeError('latest-financial-statements is filtered by us_stock_basic, which is empty')

    codes = set()
    for page in range(max_page + 1):
        data = guard('latest-financial-statements', dict(), lambda:
                     _fmp_get('latest-financial-statements', page=page))
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


def _us_dividend_frame(data: list, start: str, end: str, daily_table: str = 'us_stock_daily') -> pd.DataFrame:
    """把 FMP 分红记录(dividends / dividends-calendar 字段相同)整理成 us_dividend 表，只留除息日在 [start, end] 内的。

    列名与 A 股 dividend 表相同含义的保持一致；cash_div_tax 为每股税前金额、交易币种(美股美元、港股港币)，不按之后的拆股调整：
    FMP 的 dividend 对美股外国公司是本币且无币种字段，写入时按前复权价反推成美元(见 _pick_same_day)。
    adjDividend 不存：它按下载当天为止的拆股折算，会随时间变。除息日或金额(均为主键)缺失的记录是数据源自己的脏数据，丢弃并警告。
    宣告日、登记日、派息日可以没有：FMP 用空字符串或 0001-01-01(日期类型的最小值)表示，都按空值。
    同一(代码, 除息日)多条(多数是同一笔分红按不同汇率/修正前后记了两遍，偶有真的两笔)：用 FMP 前复权价在除息日
    前后的变化反推实际金额，在"单独某条 / 全部相加"里取最接近的，只存选中的；前收从本地 daily_table 读，没有就全存。
    """
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    dirty = raw['date'].fillna('').eq('') | raw['dividend'].isna()
    if dirty.any():
        warnings.warn(f'FMP dividends: dropped {dirty.sum()} records without ex-date or amount: '
                      f'{sorted(set(raw.loc[dirty, "symbol"]))[:5]}')
        raw = raw[~dirty]
    no_date = ('', '0001-01-01')
    res = pd.DataFrame({
        'ts_code':     raw['symbol'],
        'ex_date':     pd.to_datetime(raw['date']),
        'cash_div_tax': raw['dividend'].astype('float64'),
        'ann_date':    pd.to_datetime(raw['declarationDate'].replace(no_date, None)),
        'record_date': pd.to_datetime(raw['recordDate'].replace(no_date, None)),
        'pay_date':    pd.to_datetime(raw['paymentDate'].replace(no_date, None)),
    })
    if start:
        res = res[res['ex_date'] >= pd.Timestamp(regulate_date_format(start, force_format='date'))]
    if end:
        res = res[res['ex_date'] <= pd.Timestamp(regulate_date_format(end, force_format='date'))]
    return _pick_same_day(res.reset_index(drop=True), daily_table)


def _implied_dividend(tk, t, raw, k):
    """用 FMP 前复权价 / 原始价的比值 k 在除息日 t 前后的变化反推股息(交易币种)。
    窗口 t-2 → t：前两天防 FMP 早一天调；只到当天，当天就要出因子。全量时 t+1 已有的话查一下 FMP 是否晚一天才调，是就 warning。"""
    i = raw.index.get_loc(t)
    if i < 2 or raw.index[i - 2] not in k.index or t not in k.index:
        return None
    implied = raw.iloc[i - 1] * (1 - k[raw.index[i - 2]] / k[t])
    if i + 1 < len(raw) and raw.index[i + 1] in k.index:
        n = raw.index[i + 1]
        late = k[n] / k[t] - 1
        if abs(late) > 0.01 / (k[t] * raw[t]) + 0.01 / (k[n] * raw[n]):  # 两天 adj 价各 ±0.01(FMP 两次舍入，实测到 0.0087)
            warnings.warn(f'{tk} {t.date()}: FMP 前复权价在除息日后一天又变了 {late:.4%}，可能晚一天才调')
    return implied


def _pick_same_day(res: pd.DataFrame, daily_table: str) -> pd.DataFrame:
    """分红表写入前的两件事，都靠 FMP 前复权价 / 本地原始价在除息日前后的变化反推 FMP 实际用的金额：
    1. 美股的外国公司(basic 表 country ≠ US)：FMP 给的是本币且无币种字段，金额改成反推出的美元；反推不了的不写，下次再来。
    2. 同一(代码, 除息日)多条：在"单独某条 / 全部相加"里取最接近反推值的，只存选中的；反推不了的全存。
    港股 FMP 已折成港币，只做第 2 件。"""
    if res.empty:
        return res
    from qteasy import QT_DATA_SOURCE as ds
    foreign = set()
    if daily_table == 'us_stock_daily':
        basic = ds.read_table_data('us_stock_basic', shares=','.join(sorted(set(res['ts_code']))), primary_key_in_index=False)
        if not basic.empty:
            foreign = set(basic.loc[basic['country'] != 'US', 'ts_code'])
    dup = res.duplicated(['ts_code', 'ex_date'], keep=False)
    todo = res[dup | res['ts_code'].isin(foreign)]
    if todo.empty:
        return res
    res = res.copy()
    keep = pd.Series(True, index=res.index)
    for tk, g_all in todo.groupby('ts_code'):
        lo = (g_all['ex_date'].min() - pd.Timedelta(days=10)).strftime('%Y-%m-%d')
        hi = (g_all['ex_date'].max() + pd.Timedelta(days=10)).strftime('%Y-%m-%d')
        raw = ds.read_table_data(daily_table, shares=tk, start=lo, end=hi, primary_key_in_index=False)
        data = [] if raw.empty else guard('historical-price-eod/dividend-adjusted', dict(ts_code=tk, start=lo, end=hi),
                                          lambda ts_code, start, end:
                                          _fmp_get('historical-price-eod/dividend-adjusted', symbol=ts_code, **{'from': start, 'to': end}),
                                          fmt='%Y-%m-%d')
        if not data:
            if tk in foreign:
                keep[g_all.index] = False
            continue
        raw = raw.assign(trade_date=pd.to_datetime(raw['trade_date'])).set_index('trade_date')['close'].sort_index()
        adj = pd.DataFrame(data).assign(date=lambda d: pd.to_datetime(d['date'])).set_index('date')['adjClose']
        k = (adj.astype('float64') / raw).dropna()
        for t, g in g_all.groupby('ex_date'):
            implied = _implied_dividend(tk, t, raw, k) if t in raw.index else None
            if tk in foreign:
                if implied is None or implied <= 0:
                    keep[g.index] = False
                else:
                    keep[g.index] = False
                    keep[g.index[0]] = True
                    res.loc[g.index[0], 'cash_div_tax'] = round(float(implied), 6)
            elif len(g) > 1 and implied is not None:
                amounts = g['cash_div_tax'].tolist()
                best = min(amounts + [sum(amounts)], key=lambda x: abs(x - implied))
                if best in amounts:  # 只留最接近的那一条；最接近"全部相加"则都留
                    keep[g.index] = False
                    keep[g['cash_div_tax'].sub(best).abs().idxmin()] = True
    return res[keep].reset_index(drop=True)


def _earnings_calendar(basic_table: str) -> pd.DataFrame:
    """即将发布的财报(FMP earnings-calendar)：发布日在未来 250 天内的，按本地股票表过滤，每个报告期一行。

    表只存未来，区间固定为 [今天, 今天+250](港股中报到年报约 240 天)，不用 refill 的区间；接口 ≤90 天，分段请求。
    confirmed=0 表示发布日是 FMP 预估。
    """
    today = pd.Timestamp.now(tz='America/New_York').normalize().tz_localize(None)   # 美东的今天
    data, seg_start, last = [], today, today + pd.Timedelta(days=250)
    while seg_start <= last:
        seg_end = min(seg_start + pd.Timedelta(days=89), last)
        data.extend(guard('earnings-calendar', dict(start=seg_start.strftime('%Y-%m-%d'), end=seg_end.strftime('%Y-%m-%d')),
                          lambda start, end:
                          _fmp_get('earnings-calendar', **{'from': start, 'to': end, 'includeReportTimes': 'true'}),
                          fmt='%Y-%m-%d'))
        seg_start = seg_end + pd.Timedelta(days=1)
    from qteasy import QT_DATA_SOURCE
    stock_codes = set(QT_DATA_SOURCE.read_table_data(basic_table).index)
    if not stock_codes:
        raise RuntimeError(f'earnings calendar is filtered by {basic_table}, which is empty')
    rows = [r for r in data if r.get('symbol') in stock_codes]
    if not rows:
        return pd.DataFrame()
    raw = pd.DataFrame(rows)
    dirty = raw['periodEnding'].fillna('').eq('') | raw['date'].fillna('').eq('')
    if dirty.any():
        warnings.warn(f'FMP earnings-calendar: dropped {dirty.sum()} records without period end or report date: '
                      f'{sorted(set(raw.loc[dirty, "symbol"]))[:5]}')
        raw = raw[~dirty]
    return pd.DataFrame({
        'ts_code':          raw['symbol'],
        'end_date':         pd.to_datetime(raw['periodEnding']),
        'report_date':      pd.to_datetime(raw['date']),
        'report_time':      raw['time'],
        'fiscal_period':    raw['fiscalPeriod'],
        'fiscal_year':      raw['fiscalYear'],
        'confirmed':        raw['confirmed'].astype('Int64'),
        'eps_estimate':     raw['epsEstimated'],
        'revenue_estimate': raw['revenueEstimated'],
        'updated':          pd.to_datetime(raw['lastUpdated']),
    }).sort_values('report_date').drop_duplicates(['ts_code', 'end_date'], keep='last')


def us_earnings(**_) -> pd.DataFrame:
    return _earnings_calendar('us_stock_basic')


def hk_earnings(**_) -> pd.DataFrame:
    return _earnings_calendar('hk_stock_basic')


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
    from qteasy import QT_DATA_SOURCE
    stock_codes = set(QT_DATA_SOURCE.read_table_data('us_stock_basic').index)
    if not stock_codes:
        raise RuntimeError('us_dividend_calendar is filtered by us_stock_basic, which is empty')
    return _us_dividend_frame([r for r in data if r.get('symbol') in stock_codes], start, end)  # 先过滤再解析


def _fmp_calendar(endpoint: str, basic_table: str, frame, start: str, end: str) -> pd.DataFrame:
    """拉全球日历接口，只留本地 basic_table 里的代码，再交给 frame 解析。"""
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard(endpoint, dict(start=start, end=end),
                 lambda start, end: _fmp_get(endpoint, **{'from': start, 'to': end}),
                 fmt='%Y-%m-%d')
    from qteasy import QT_DATA_SOURCE
    stock_codes = set(QT_DATA_SOURCE.read_table_data(basic_table).index)
    if not stock_codes:
        raise RuntimeError(f'{endpoint} is filtered by {basic_table}, which is empty')
    return frame([r for r in data if r.get('symbol') in stock_codes], start, end)  # 先过滤再解析


def hk_dividend(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    if ts_code is None:
        return pd.DataFrame()
    data = guard('dividends', dict(ts_code=ts_code), lambda ts_code:
                 _fmp_get('dividends', symbol=ts_code, limit=1000))
    if len(data) >= 1000:
        raise RuntimeError(f'dividends {ts_code} returned {len(data)} rows = api max, may be incomplete')
    return _us_dividend_frame(data, start, end, 'hk_stock_daily')


def hk_dividend_calendar(start: str = None, end: str = None) -> pd.DataFrame:
    return _fmp_calendar('dividends-calendar', 'hk_stock_basic', lambda d, a, b: _us_dividend_frame(d, a, b, 'hk_stock_daily'), start, end)


def _us_split_frame(data: list, start: str, end: str) -> pd.DataFrame:
    """把 FMP 拆股记录(splits / splits-calendar 字段相同)整理成 us_split 表，只留除权日在 [start, end] 内的。"""
    if not data:
        return pd.DataFrame()
    raw = pd.DataFrame(data)
    res = pd.DataFrame({
        'ts_code':     raw['symbol'],
        'ex_date':     pd.to_datetime(raw['date']),
        'numerator':   raw['numerator'].astype('float64'),
        'denominator': raw['denominator'].astype('float64'),
        'split_type':  raw['splitType'],
    })
    if start:
        res = res[res['ex_date'] >= pd.Timestamp(regulate_date_format(start, force_format='date'))]
    if end:
        res = res[res['ex_date'] <= pd.Timestamp(regulate_date_format(end, force_format='date'))]
    return res.reset_index(drop=True)


def us_split(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """单只股票的拆股/合股(FMP splits)，返回除权日落在 [start, end] 内的记录。

    该接口只有 symbol、limit 两个参数(不认日期、不能翻页)，单次最多 1000 条：按最大值取该股全部历史，取回后再筛选；
    返回满 1000 条说明可能没取全，报错。
    """
    if ts_code is None:
        return pd.DataFrame()
    data = guard('splits', dict(ts_code=ts_code), lambda ts_code:
                 _fmp_get('splits', symbol=ts_code, limit=1000))
    if len(data) >= 1000:
        raise RuntimeError(f'splits {ts_code} returned {len(data)} rows = api max, may be incomplete')
    return _us_split_frame(data, start, end)


def us_split_calendar(start: str = None, end: str = None) -> pd.DataFrame:
    """全球股票的拆股(FMP splits-calendar)，[start, end] 左右闭，只留 us_stock_basic 里的代码。"""
    return _fmp_calendar('splits-calendar', 'us_stock_basic', _us_split_frame, start, end)


def hk_split(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    return us_split(ts_code, start, end)


def hk_split_calendar(start: str = None, end: str = None) -> pd.DataFrame:
    return _fmp_calendar('splits-calendar', 'hk_stock_basic', _us_split_frame, start, end)


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
        'end_date':             i['_end_date'],
        'period':               i['_period'],
        'period_end':           i['_period_end'],
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
        'admin_exp':            i.get('generalAndAdministrativeExpenses'),
        'sell_exp':             i.get('sellingAndMarketingExpenses'),
        'other_expenses':       i.get('otherExpenses'),
        'net_interest_income':  i.get('netInterestIncome'),
        'non_oper_income_ex_int': i.get('nonOperatingIncomeExcludingInterest'),
        'discontinued_net_profit': i.get('netIncomeFromDiscontinuedOperations'),
        'other_adj_net_income': i.get('otherAdjustmentsToNetIncome'),
        'net_income_deductions': i.get('netIncomeDeductions'),
        'bottom_line_net_income': i.get('bottomLineNetIncome'),
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
        'end_date':                   i['_end_date'],
        'period':                     i['_period'],
        'period_end':                 i['_period_end'],
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
        'accounts_receiv':            i.get('accountsReceivables'),
        'oth_receiv':                 i.get('otherReceivables'),
        'prepayment':                 i.get('prepaids'),
        'goodwill_intang':            i.get('goodwillAndIntangibleAssets'),
        'total_invest':               i.get('totalInvestments'),
        'oth_assets':                 i.get('otherAssets'),
        'accrued_exp':                i.get('accruedExpenses'),
        'oth_payable':                i.get('otherPayables'),
        'total_payable':              i.get('totalPayables'),
        'taxes_payable':              i.get('taxPayables'),
        'lease_liab_cur':             i.get('capitalLeaseObligationsCurrent'),
        'lease_liab_ncur':            i.get('capitalLeaseObligationsNonCurrent'),
        'lease_liab':                 i.get('capitalLeaseObligations'),
        'defer_rev_ncur':             i.get('deferredRevenueNonCurrent'),
        'defer_tax_liab_ncur':        i.get('deferredTaxLiabilitiesNonCurrent'),
        'oth_liab':                   i.get('otherLiabilities'),
        'preferred_stock':            i.get('preferredStock'),
        'cap_rese':                   i.get('additionalPaidInCapital'),
        'treasury_stock':             i.get('treasuryStock'),
        'oth_eqt_tools':              i.get('otherTotalStockholdersEquity'),
        'total_equity':               i.get('totalEquity'),
    } for i in items]
    return pd.DataFrame(rows)  # 金额为报告原币，见 currency 列


def hk_balance_debt(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股 hk_balance 里 FMP 负责的两列：total_debt、net_debt(理杏仁没有总债务：腾讯的长期票据不在它任何字段里)。
    其余列由理杏仁通道写；这里只返回主键 + 这两列，写入用 merge_type='patch'，两边互不覆盖。"""
    return _hk_fill_rows(us_balance(ts_code, start, end), 'hk_balance', ['total_debt', 'net_debt'])


def hk_income_da(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股 hk_income 里 FMP 负责的三列：da、ebitda、ebit(理杏仁利润表没有折旧摊销)。
    其余列由理杏仁通道写；这里只返回主键 + 这三列，写入用 merge_type='patch'，两边互不覆盖。"""
    return _hk_fill_rows(us_income(ts_code, start, end), 'hk_income', ['da', 'ebitda', 'ebit'])


def _hk_fill_rows(res: pd.DataFrame, table: str, columns: list) -> pd.DataFrame:
    """FMP 给港股财报表补列：只填本地表里理杏仁已经写了的行(按主键过滤)，不自己造行——FMP 的期比理杏仁多(1987 年起、
    半年报公司的季度行)，造出来的行只有这几列、其余全空，估值读到会炸。所以理杏仁那一行要先跑。"""
    if res.empty:
        return res
    from qteasy import QT_DATA_SOURCE
    local = QT_DATA_SOURCE.read_table_data(table, shares=res['ts_code'].iloc[0], primary_key_in_index=False)
    if local.empty:
        return pd.DataFrame()
    keys = local[['ts_code', 'end_date', 'period']].assign(end_date=lambda d: pd.to_datetime(d['end_date']))
    res = res.assign(end_date=pd.to_datetime(res['end_date'])).merge(keys, on=['ts_code', 'end_date', 'period'])
    return res[['ts_code', 'end_date', 'period'] + columns]


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
        'end_date':               i['_end_date'],
        'period':                 i['_period'],
        'period_end':             i['_period_end'],
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
        'chg_receiv':             i.get('accountsReceivables'),
        'chg_inventory':          i.get('inventory'),
        'chg_payable':            i.get('accountsPayables'),
        'chg_oth_working_cap':    i.get('otherWorkingCapital'),
        'capex_total':            i.get('capitalExpenditure'),
        'st_debt_issuance_net':   i.get('shortTermNetDebtIssuance'),
        'lt_debt_issuance_net':   i.get('longTermNetDebtIssuance'),
        'stock_issuance':         i.get('commonStockIssuance'),
        'stock_repurchase':       i.get('commonStockRepurchased'),
        'stock_issuance_net':     i.get('netCommonStockIssuance'),
        'pref_stock_issuance_net': i.get('netPreferredStockIssuance'),
        'div_paid_common':        i.get('commonDividendsPaid'),
        'div_paid_pref':          i.get('preferredDividendsPaid'),
        'cash_begin':             i.get('cashAtBeginningOfPeriod'),
    } for i in items]
    return pd.DataFrame(rows)  # 金额为报告原币，见 currency 列
