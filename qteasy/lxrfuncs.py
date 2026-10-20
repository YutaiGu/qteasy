# coding=utf-8
# ======================================
# File:     lxrfuncs.py
# Author:   Jackie PENG
# Contact:  jackie.pengzhao@gmail.com
# Created:  2026-10-19
# Desc:
#   Interfaces to lixingren (理杏仁) open api,
#   open.lixinger.com.
# ======================================

import os
import warnings

import requests
import pandas as pd

from qteasy.__init__ import logger_core, QT_CONFIG
from qteasy.utilfuncs import retry, regulate_date_format
from qteasy.api_guard import guard

ERRORS_TO_CHECK_ON_RETRY = Exception

EXTRA_RETRY_API = [

]

_LXR_BASE = 'https://open.lixinger.com/api'


# lxrfuncs interface function, call this function to extract data
def acquire_data(api_name, **kwargs):
    """ DataSource模块的接口函数，根据根据table的内容调用相应的理杏仁 API下载数据，并以DataFrame的形式返回数据"""
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


def _get_token() -> str:
    token = QT_CONFIG.get('lixingren_token', '') or os.environ.get('LIXINGREN_TOKEN', '')
    if not token:
        raise ValueError('lixingren_token is not configured (qteasy.cfg or env LIXINGREN_TOKEN)')
    return token


class LxrServerError(RuntimeError):
    """理杏仁服务器自己出错(HTTP 5xx，如营收构成接口对个别股票报 TypeError)：是数据源对这只股票的问题，不是请求错误。"""


def _lxr_post(path: str, **body) -> list:
    """向理杏仁 API 发起单次 POST 请求，返回 data list。调用方一律经 guard 调用。
    理杏仁按请求次数计额度，一次请求可带多个代码。报错不带 token。
    """
    with requests.post(f'{_LXR_BASE}/{path}', json={'token': _get_token(), **body}, timeout=60) as resp:
        payload = resp.json() if resp.ok else None
    if payload is None or payload.get('code') != 1:
        error = LxrServerError if resp.status_code >= 500 else RuntimeError
        raise error(f'lixingren {path} failed: HTTP {resp.status_code}, '
                    f'{payload.get("message") if payload else resp.text[:300]}')
    return payload.get('data') or []


def _frame(data: list, fields: dict) -> pd.DataFrame:
    """理杏仁返回值的共性，统一在这里处理，各下载函数之后按列名直接取：
    1. 没有值的字段整个键不给，不是给 null —— 按 fields 补成空列；
    2. 数值、文本字段偶有 '' 空串(如快手的 capitalizationH) —— 当空值；
    3. 日期有两种格式：多数接口 'YYYY-MM-DDT00:00:00+08:00'，营收构成是 UTC 'YYYY-MM-DDT16:00:00.000Z'(即香港次日零点)
       —— 一律按时区转成香港时间再取日期，不能直接截前 10 位。
    fields: 接口字段 → 'date' / 'num' / 'str' / 'obj'(嵌套结构原样保留)。
    """
    raw = pd.DataFrame(data).reindex(columns=list(fields))
    for col, kind in fields.items():
        s = raw[col].astype(object).where(raw[col].notna() & (raw[col].astype(object) != ''), None)
        if kind == 'date':
            raw[col] = pd.to_datetime(s, utc=True).dt.tz_convert('Asia/Hong_Kong').dt.tz_localize(None).dt.normalize()
        elif kind == 'num':
            raw[col] = pd.to_numeric(s, errors='coerce')
        else:
            raw[col] = s
    return raw


def _to_ts_code(code: str, area: str, market: str) -> str:
    """理杏仁 stockCode + areaCode/market → qteasy 代码。A 股 6 位按前缀定交易所(同 akfuncs)；港股 4 位 + .HK；美股原样。"""
    code = str(code).strip()
    if area == 'cn' and market == 'a':
        code = code.zfill(6)
        suffix = 'BJ' if code.startswith(('4', '8')) else 'SH' if code.startswith('6') else 'SZ'
        return f'{code}.{suffix}'
    if area == 'hk':
        return f'{int(code):04d}.HK'
    return code


def _to_lxr_code(ts_code: str) -> str:
    """qteasy 代码 → 理杏仁 stockCode(请求参数)：去后缀，港股补足 5 位。"""
    symbol, _, suffix = ts_code.partition('.')
    return symbol.zfill(5) if suffix == 'HK' else symbol


def _normalize_stock_basic(constituents: list, companies: list, industries: dict = None) -> pd.DataFrame:
    """理杏仁成分列表(stockCode, stockName{cmn_hans_cn, cmn_hant_hk, en}, areaCode, market) + hk/company 的公司信息
    → hk_stock_basic 结构。name 取英文名，cnname 取简体中文名(繁体与简体只差字形，不存；hk/company 的 name 与之重复)；
    exchange、ipoDate、sharesPerLot、fsTableType、mutualMarkets(list，逗号连接)、stockCodeA(转成 A 股 ts_code)来自 hk/company；
    industries 是 hk/company/industries 的结果(代码 → 恒生行业分类三级列表)：sector/industry/sub_industry 取一二三级名，
    industry_code 取三级代码；areaCode 是上市地不是国家，country 与 isin/cusip 留空；listingStatus、mutualMarketFlag 不存。
    """
    columns = ['ts_code', 'name', 'cnname', 'exchange', 'sector', 'industry', 'sub_industry', 'industry_code', 'country', 'isin', 'cusip',
               'list_date', 'shares_per_lot', 'fs_type', 'mutual_markets', 'a_code']
    out = pd.DataFrame(columns=columns)
    if not constituents:
        return out
    info = {_to_ts_code(c['stockCode'], c.get('areaCode'), c.get('market')): c for c in companies}
    out['ts_code'] = [_to_ts_code(i['stockCode'], i.get('areaCode'), i.get('market')) for i in constituents]
    out['name'] = [(i.get('stockName') or {}).get('en') for i in constituents]
    out['cnname'] = [(i.get('stockName') or {}).get('cmn_hans_cn') for i in constituents]
    rows = [info.get(c, {}) for c in out['ts_code']]
    out['exchange'] = [r.get('exchange') for r in rows]
    out['list_date'] = [pd.Timestamp(r['ipoDate'][:10]) if r.get('ipoDate') else None for r in rows]
    out['shares_per_lot'] = [r.get('sharesPerLot') for r in rows]
    out['fs_type'] = [r.get('fsTableType') for r in rows]
    out['mutual_markets'] = [','.join(r['mutualMarkets']) if r.get('mutualMarkets') else None for r in rows]
    out['a_code'] = [_to_ts_code(r['stockCodeA'], 'cn', 'a') if r.get('stockCodeA') else None for r in rows]
    levels = [sorted(((industries or {}).get(c) or []), key=lambda x: len(x.get('stockCode', ''))) for c in out['ts_code']]
    out['sector'] = [lv[0]['name'] if len(lv) > 0 else None for lv in levels]
    out['industry'] = [lv[1]['name'] if len(lv) > 1 else None for lv in levels]
    out['sub_industry'] = [lv[-1]['name'] if len(lv) > 2 else None for lv in levels]
    out['industry_code'] = [lv[-1]['stockCode'] if lv else None for lv in levels]
    return out.drop_duplicates('ts_code').reset_index(drop=True)


def hk_stock_basic(**_) -> pd.DataFrame:  # C 行附带 start/end，用不上
    """港股股票池基本信息：恒生综合指数当前成分 = 理杏仁 hk/index/constituents 的大型(HSLI)、中型(HSMI)、小型(HSSI)
    三个分指数之和，三个代码放在同一个请求里(文档：stockCodes ≤100，date=latest)；再按成分查 hk/company 补公司信息
    (文档：stockCodes ≤100，按 100 只一批；按 pageIndex 翻到空页)，逐只查 hk/company/industries 补恒生行业分类(一次返回全部层级)。
    经 _normalize_stock_basic 转成表结构。表按主键更新、不删行。

    自保：理杏仁按请求次数计额度，成分每季检讨一次，只在每月 1/7/13/19/25 日(香港时间)取；其他日子不调任何接口，
    返回空。本地表为空时不受日期限制，取一次初始化。
    """
    from qteasy import QT_DATA_SOURCE
    index_codes = ['HSLI', 'HSMI', 'HSSI']   # 恒生综合 = 大型 + 中型 + 小型
    refresh_days = (1, 7, 13, 19, 25)
    batch = 100                              # hk/company 文档：stockCodes ≤100
    today = pd.Timestamp.now(tz='Asia/Hong_Kong')

    if not QT_DATA_SOURCE.read_table_data('hk_stock_basic').empty and today.day not in refresh_days:
        logger_core.info(f'hk_stock_basic refreshes only on days {refresh_days}, skipped')
        return pd.DataFrame()

    data = guard('lixingren-hk-index-constituents', dict(), lambda:
                 _lxr_post('hk/index/constituents', stockCodes=index_codes, date='latest'))
    got = [item.get('stockCode') for item in data]
    if sorted(got) != sorted(index_codes) or any(not item.get('constituents') for item in data):
        raise RuntimeError(f'lixingren hk/index/constituents: expected {index_codes} with constituents, got {got}')
    constituents = [c for item in data for c in item['constituents']]
    codes = sorted({c['stockCode'] for c in constituents})
    companies = []
    for i in range(0, len(codes), batch):
        page = 0
        while True:   # 按 pageIndex 翻到空页(越界返回空 data)，页大小由接口默认
            rows = guard('lixingren-hk-company', dict(), lambda:
                         _lxr_post('hk/company', stockCodes=codes[i:i + batch], pageIndex=page))
            if not rows:
                break
            companies.extend(rows)
            page += 1
    industries = {f'{int(c):04d}.HK': guard('lixingren-hk-company-industries', dict(ts_code=c), lambda ts_code:
                                           _lxr_post('hk/company/industries', stockCode=ts_code)) for c in codes}
    res = _normalize_stock_basic(constituents, companies, industries)
    logger_core.info(f'downloaded {len(res)} rows of data from lixingren table hk_stock_basic ({"+".join(index_codes)})')
    return res


def hk_stock_adj_factor(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股除权比率(理杏仁 hk/company/candlestick，后复权)：[start, end] 两端包含，跨度 ≤10 年(文档)。
    接口每天一行，只有除权日的 backwardComplexFactor 有值(当天 ÷ 前一天的复权倍数，含拆股、分红、实物分派)，
    只保留这些行。后复权基准传 start(比率与基准无关，只影响价格列，价格不存)。
    """
    if ts_code is None:
        return pd.DataFrame()
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('lixingren-hk-candlestick', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _lxr_post('hk/company/candlestick', stockCode=_to_lxr_code(ts_code), type='bc_rights',
                           startDate=start, endDate=end, adjustBackwardDate=start),
                 fmt='%Y-%m-%d')
    raw = _frame(data, {'date': 'date', 'backwardComplexFactor': 'num'})
    raw = raw[raw['backwardComplexFactor'].notna()]
    if raw.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_date': raw['date'],
        'adj_ratio':  raw['backwardComplexFactor'],
    })


def hk_stock_daily(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股日线行情(理杏仁 hk/company/candlestick，不复权 ex_rights)：[start, end] 两端包含，跨度 ≤10 年(文档)。
    存开高低收、成交量、成交额；change(涨跌幅，可由收盘算出)不存，to_r(换手率)属于 indicator 表，这里不存。
    """
    if ts_code is None:
        return pd.DataFrame()
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard('lixingren-hk-candlestick', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _lxr_post('hk/company/candlestick', stockCode=_to_lxr_code(ts_code), type='ex_rights',
                           startDate=start, endDate=end),
                 fmt='%Y-%m-%d')
    raw = _frame(data, {'date': 'date', 'open': 'num', 'high': 'num', 'low': 'num', 'close': 'num',
                        'volume': 'num', 'amount': 'num'})
    if raw.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        'ts_code':    ts_code,
        'trade_date': raw['date'],
        'open':       raw['open'],
        'high':       raw['high'],
        'low':        raw['low'],
        'close':      raw['close'],
        'vol':        raw['volume'],
        'amount':     raw['amount'],
    })


def _back(start: str, days: int) -> str:
    """接口区间按事件日过滤、记录晚于事件日才出现的表：start 往前倒 days 天，日更才拉得到事件日在区间之前、公告在区间之内的记录。"""
    return (pd.Timestamp(start) - pd.Timedelta(days=days)).strftime('%Y-%m-%d') if start else None


def _lxr_dated(path: str, ts_code: str, start: str, end: str, fields: dict, back: int = 0) -> pd.DataFrame:
    """按股票 + [start, end](两端包含，跨度 ≤10 年)取理杏仁的逐条事件表，经 _frame(fields) 规整；back 见 _back。"""
    start = _back(regulate_date_format(start, force_format='date'), back) if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard(f'lixingren-{path}', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _lxr_post(path, stockCode=_to_lxr_code(ts_code), startDate=start, endDate=end),
                 fmt='%Y-%m-%d')
    return _frame(data, fields)


def hk_equity_change(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股股本变动(理杏仁 hk/company/equity-change)：每次变动一行，带公告日；变动原因存接口原文(繁体)。
    接口区间按变动日(date)过滤，记录公告后才出现，公告比变动日晚 0~7 天(实测)，录入再晚几天：start 倒 30 天。"""
    if ts_code is None:
        return pd.DataFrame()
    raw = _lxr_dated('hk/company/equity-change', ts_code, start, end, back=30,
                     fields={'date': 'date', 'declarationDate': 'date', 'changeReason': 'str',
                             'capitalization': 'num', 'capitalizationH': 'num'})
    if raw.empty:
        return pd.DataFrame()
    dirty = raw['declarationDate'].isna()
    if dirty.any():   # 公告日是主键，缺了是数据源的脏记录(实测只有 REIT)：丢弃并警告
        warnings.warn(f'lixingren equity-change {ts_code}: dropped {int(dirty.sum())} records without declarationDate '
                      f'(dates {sorted(raw.loc[dirty, "date"].dt.strftime("%Y-%m-%d"))[:5]})')
        raw = raw[~dirty]
        if raw.empty:
            return pd.DataFrame()
    return pd.DataFrame({
        'ts_code':       ts_code,
        'change_date':   raw['date'],
        'ann_date':      raw['declarationDate'],
        'change_reason': raw['changeReason'],
        'total_share':   raw['capitalization'],
        'h_share':       raw['capitalizationH'],
    })


def hk_repurchase(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股回购(理杏仁 hk/company/repurchase)：每笔一行。_id、company、lastUpdated 不存，stockCode 即 ts_code。
    接口区间按回购日(date)过滤，申报(formDate)晚 1~3 个交易日，录入再晚几天：start 倒 30 天。"""
    if ts_code is None:
        return pd.DataFrame()
    raw = _lxr_dated('hk/company/repurchase', ts_code, start, end, back=30,
                     fields={'date': 'date', 'securityType': 'str', 'methodOfPurchase': 'str', 'num': 'num',
                             'highestPrice': 'num', 'lowestPrice': 'num', 'avgPrice': 'num', 'totalPaid': 'num',
                             'currency': 'str', 'numPurchasedInYearSinceResolution': 'num',
                             'ratioPurchasedSinceResolution': 'num', 'formDate': 'date', 'boardType': 'str'})
    if raw.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        'ts_code':       ts_code,
        'trade_date':    raw['date'],
        'security_type': raw['securityType'],
        'method':        raw['methodOfPurchase'],
        'vol':           raw['num'],
        'high':          raw['highestPrice'],
        'low':           raw['lowestPrice'],
        'avg_price':     raw['avgPrice'],
        'amount':        raw['totalPaid'],
        'currency':      raw['currency'],
        'vol_ytd':       raw['numPurchasedInYearSinceResolution'],
        'ratio_ytd':     raw['ratioPurchasedSinceResolution'],
        'form_date':     raw['formDate'],
        'board_type':    raw['boardType'],
    })


def hk_mainbz(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股营收构成(理杏仁 operation-revenue-constitution)：每期按业务、按地区两棵树，子项带父项名；合計行没有父项，父项记它自己；
    只作标题的行(没有数值)不存。接口的 date 是期末(UTC 格式，转成香港日期)，存入 period_end；end_date 是报告期的名字(period_label，
    与财报 end_date 同义)，第几季按本地依赖表 hk_income 这只股年报的结账日定；hk_income 里没有年报的股票定不了名字，返回空。
    接口区间按期末过滤，公告晚 1~4 个月：start 倒 366 天。
    个别股票接口服务器报 500(它自己的 TypeError)：是数据源对这只股票的问题，跳过这只并警告。
    """
    if ts_code is None:
        return pd.DataFrame()
    from qteasy import QT_DATA_SOURCE
    from qteasy.fmpfuncs import period_label
    try:
        raw = _lxr_dated('hk/company/operation-revenue-constitution', ts_code, start, end, back=366,
                         fields={'date': 'date', 'declarationDate': 'date', 'currency': 'str', 'dataList': 'obj'})
    except LxrServerError as e:
        warnings.warn(f'lixingren operation-revenue-constitution {ts_code} [{start}, {end}]: server error, skipped ({e})')
        return pd.DataFrame()
    if raw.empty:
        return pd.DataFrame()
    income = QT_DATA_SOURCE.read_table_data('hk_income', shares=ts_code, primary_key_in_index=False)
    annual = income.loc[income['period'] == 'Y', 'period_end'] if not income.empty else pd.Series(dtype=object)
    if annual.empty:
        logger_core.info(f'hk_mainbz {ts_code}: no annual report in hk_income, period name undefined, skipped')
        return pd.DataFrame()
    rows = []
    for _, r in raw.iterrows():
        for item in r['dataList'] or []:
            if item.get('revenue') is None and item.get('costs') is None:
                continue
            rows.append((r['date'], r['declarationDate'], r['currency'],
                         item.get('parentItemName') or item['itemName'], item['itemName'],
                         item.get('revenue'), item.get('costs'), item.get('grossProfitMargin')))
    res = pd.DataFrame(rows, columns=['period_end', 'ann_date', 'curr_type', 'bz_parent', 'bz_item',
                                      'bz_sales', 'bz_cost', 'gross_margin'])
    res['ts_code'] = ts_code
    res['period_end'] = pd.to_datetime(res['period_end'])
    res['ann_date'] = pd.to_datetime(res['ann_date'])
    res['end_date'] = period_label(res['period_end'].values, pd.to_datetime(annual).max()).values
    return res[['ts_code', 'end_date', 'bz_parent', 'bz_item', 'period_end', 'ann_date',
                'bz_sales', 'bz_cost', 'gross_margin', 'curr_type']]


# ── 财报：理杏仁 hk/company/fs/<fsTableType>，按 hk_stock_basic.fs_type 路由；列对现有 hk_income / hk_balance 结构 ──

_FS_INCOME_COMMON = {   # hk_income 列 → 理杏仁利润表字段，六类财报都有的科目
    'total_profit': 'ps.tp', 'income_tax': 'ps.ite', 'bottom_line_net_income': 'ps.np',
    'n_income_attr_p': 'ps.npatoshopc', 'basic_eps': 'ps.beps', 'diluted_eps': 'ps.deps',
}
_FS_INCOME = {   # 按 fs_type 的上半部分。只对有明确对应的科目；FMP 的 da/ebitda/ebit/operating_income 等没有对应，留空
    'non_financial': {'revenue': 'ps.toi', 'oper_cost': 'ps.oc', 'gross_profit': 'ps.gp', 'rd_exp': 'ps.rade',
                      'sell_exp': 'ps.se', 'admin_exp': 'ps.ae', 'interest_income': 'ps.iiife', 'interest_expense': 'ps.fc'},
    'bank':          {'revenue': 'ps.oi', 'interest_income': 'ps.ii', 'interest_expense': 'ps.ie', 'net_interest_income': 'ps.nii',
                      'operating_expense': 'ps.oe', 'admin_exp': 'ps.baae'},
    'security':      {'revenue': 'ps.oi', 'interest_income': 'ps.ii', 'interest_expense': 'ps.ie', 'net_interest_income': 'ps.nii',
                      'operating_expense': 'ps.oe', 'admin_exp': 'ps.baae'},
    'insurance':     {'revenue': 'ps.oi', 'operating_expense': 'ps.oe', 'admin_exp': 'ps.baae'},
    'other_financial': {'revenue': 'ps.oi', 'net_interest_income': 'ps.nii', 'operating_expense': 'ps.oe', 'admin_exp': 'ps.baae'},
    'reit':          {'revenue': 'ps.oi', 'admin_exp': 'ps.baae', 'interest_income': 'ps.iiife', 'interest_expense': 'ps.fc'},
}
_FS_BALANCE_COMMON = {  # hk_balance 列 → 理杏仁资产负债表字段，六类都有的科目
    'total_assets': 'bs.ta', 'total_liab': 'bs.tl', 'total_equity': 'bs.toe', 'common_stock': 'bs.sc',
    'cap_rese': 'bs.capr', 'treasury_stock': 'bs.is', 'retained_earnings': 'bs.rtp', 'preferred_stock': 'bs.psioei',
    'oth_eqt_tools': 'bs.oei', 'total_hldr_eqy_exc_min_int': 'bs.tetshaoehopc', 'minority_int': 'bs.etmsh',
    'goodwill': 'bs.gw', 'intan_assets': 'bs.ia', 'goodwill_intang': 'bs.gwaia', 'ppe_net': 'bs.ppe',
    'defer_tax_liab_ncur': 'bs.ditl', 'taxes_payable': 'bs.tp',
}
_DITA = {'defer_tax_assets': 'bs.dita'}   # REIT 的资产负债表没有递延所得税资产
_FS_BALANCE_CURRENT = {  # 有流动/非流动划分的类型(非金融、REIT)
    'cash': 'bs.cabb', 'net_receivables': 'bs.nraar', 'accounts_receiv': 'bs.ar', 'inventories': 'bs.i',
    'total_cur_assets': 'bs.tca', 'total_nca': 'bs.tnca', 'acct_payable': 'bs.ap', 'total_payable': 'bs.npaap',
    'st_debt': 'bs.stl', 'lease_liab_cur': 'bs.stll', 'total_cur_liab': 'bs.tcl', 'lt_debt': 'bs.ltl',
    'lease_liab_ncur': 'bs.ltll', 'total_ncl': 'bs.tncl',
}
_FS_BALANCE = {
    'non_financial': {**_FS_BALANCE_CURRENT, **_DITA, 'st_investments': 'bs.sti', 'lt_investments': 'bs.olti',
                      'deferred_revenue': 'bs.stcl', 'defer_rev_ncur': 'bs.ltcl'},
    'reit':          _FS_BALANCE_CURRENT,
    'bank':          {**_DITA, 'cash': 'bs.cabwcb'},
    'security':      {**_DITA, 'cash': 'bs.cabb', 'accounts_receiv': 'bs.ar', 'acct_payable': 'bs.ap', 'st_debt': 'bs.stl',
                      'lt_debt': 'bs.ltl', 'lease_liab': 'bs.ll', 'deferred_revenue': 'bs.cl'},
    'insurance':     {**_DITA, 'cash': 'bs.cabb', 'accounts_receiv': 'bs.ar', 'acct_payable': 'bs.ap', 'st_debt': 'bs.stl', 'lt_debt': 'bs.ltl'},
    'other_financial': {**_DITA, 'cash': 'bs.cabb', 'total_debt': 'bs.l'},
}
_FS_FIELDS = {   # 理杏仁六类财报各自认的字段(文档)，利润表 ps / 资产负债表 bs；取数时整类全取，对不上现有列的存成 ps_代码 / bs_代码
    'ps': {
        'non_financial': ["toi", "oi", "ii", "faci", "or", "oc", "ie", "face", "gp", "se", "ae", "fe", "fc", "iiife", "nfceg", "rade", "sbcc", "iifaajv", "iifa", "iifjv", "ivi", "ciofv", "fvcoip", "ailor", "cilor", "ooi", "ooe", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "adddicwrornci", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "tciatmsh", "tciatrnci", "natooci", "da", "da_om", "ra"],
        'bank': ["oi", "nii", "ii", "ie", "nfaci", "faci", "face", "ep", "iifaajv", "iifa", "iifjv", "ivi", "ooi", "oe", "tas", "baae", "ailor", "cilor", "oail", "ooe", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "tciatmsh", "natooci", "da", "da_om", "ra"],
        'security': ["oi", "nii", "ii", "ie", "nfaci", "nfifb", "nfifib", "nfifam", "iifaajv", "iifa", "iifjv", "ivi", "oic", "ciofv", "ei", "adi", "ooi", "oe", "tas", "baae", "ailor", "cilor", "oail", "ooe", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "adddicwrornci", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "tciatmsh", "tciatrnci", "natooci", "da", "da_om", "ra"],
        'insurance': ["oi", "ep", "ir", "iifb", "nbii", "nifaci", "iifaajv", "iifa", "iifjv", "ivi", "oic", "ciofv", "ei", "adi", "ooi", "oe", "s", "ce", "crcfri", "iiicr", "irrcfri", "phdrfpip", "rie", "faceoio", "ise", "aoripp", "arfri", "iifefici", "nrifefrich", "iefb", "nbie", "niface", "tas", "baae", "ercfri", "ailor", "cilor", "oail", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "adddicwrornci", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "tciatmsh", "tciatrnci", "natooci", "da", "da_om", "ra"],
        'reit': ["oi", "poe", "mpf", "npi", "baae", "fe", "fc", "iiife", "nfceg", "fvcoip", "iifaajv", "iifa", "iifjv", "ivi", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "adddicwrornci", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "twuh", "tciatmsh", "tciatrnci", "natooci", "da", "da_om", "ra", "tda", "dtuh"],
        'other_financial': ["oi", "nii", "nfaci", "ep", "iifaajv", "iifa", "iifjv", "ivi", "ooi", "oe", "tas", "baae", "ailor", "cilor", "oail", "ooe", "tp", "ite", "np", "npatshaoehopc", "npatoshopc", "npatmsh", "adddicwrornci", "nonifrs_op", "nonifrs_opatoehopc", "beps", "deps", "tci", "tciatshaoehopc", "tciatoshopc", "tciatmsh", "tciatrnci", "natooci", "da", "da_om", "ra"],
    },
    'bs': {
        'non_financial': ["ta", "tca", "cabb", "claatc", "lcabbapbd", "lcabb", "cpbd", "i", "nraar", "nr", "ar", "rf", "tr", "cca", "clr", "cahfs", "cdfa", "std", "sti", "osti", "cfiaac", "cfiafvtpol", "cfiafvtoci", "cpdoraoa", "tnca", "ltd", "nclaatc", "ncpbd", "ppe", "lur", "roua", "ba", "oaga", "cip", "pi", "iiaajv", "iia", "iijv", "gwaia", "gw", "ia", "olti", "dita", "ncca", "nclr", "ncahfs", "ncdfa", "ncfiaac", "ncfiafvtpol", "ncfiafvtoci", "ltar", "ncpdoraoa", "tl", "tcl", "stl", "stpl", "stbp", "tp", "otp", "npaap", "np", "ap", "didwioy", "stlcofcb", "stcl", "stll", "stdfl", "stsn", "stflafvtpol", "stafcrdaoraol", "tncl", "ltl", "ltpl", "ditl", "ltdi", "sn", "bp", "lcorcps", "ltlcofcb", "ltcl", "ltll", "ltdfl", "ltflafvtpol", "ltafcrdaoraol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "ep_stn", "tsc"],
        'bank': ["ta", "cabwcb", "bwbaofi", "pm", "pwbaofi", "fahursa", "laatc", "hkgcoi", "dfa", "dfiafvtpol", "fiafvtpol", "fiaac", "diafvtoci", "deiafvtoci", "ppe", "roua", "iiaajv", "iia", "iijv", "gwaia", "gw", "ia", "tr", "dita", "oa", "tl", "bfcb", "dfbaofi", "pfbaofi", "cd", "hkcnic", "fasurpa", "dfl", "flafvtpol", "dflafvtpol", "dsi", "tp", "ditl", "ol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "ep_stn"],
        'security': ["ta", "cabb", "choboc", "sr", "dhoboc", "ma", "dfa", "fahursa", "rfd", "ar", "fiafvtpol", "fiaac", "diafvtoci", "deiafvtoci", "ltei", "rei", "ppe", "cip", "roua", "gwaia", "gw", "ia", "dita", "ahfs", "oa", "tl", "stl", "stfip", "pfbaofi", "tfl", "dfl", "fasurpa", "stoa", "ssoa", "sawp", "tp", "ap", "intp", "cl", "al", "ltl", "bp", "ll", "ditl", "ol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "ep_stn", "tsc"],
        'insurance': ["ta", "cabb", "sr", "pwbaofi", "fahursa", "pr", "ar", "ir", "rir", "uprrfri", "crrfri", "lirrfri", "lthirrfri", "niipl", "dfa", "crorir", "phpl", "ltar", "laatc", "td", "dfiafvtpol", "fiafvtpol", "fiaac", "diafvtoci", "deiafvtoci", "htmi", "ltei", "sd", "rei", "ppe", "gwaia", "gw", "ia", "roua", "dita", "oa", "saa", "tl", "stl", "bfcb", "dfbaofi", "pfbaofi", "tfl", "dfl", "fasurpa", "cd", "stoa", "ap", "afc", "pria", "facp", "rip", "sawp", "tp", "intp", "cp", "phdp", "phd", "icr", "uepr", "cr", "lir", "lthir", "ltl", "bp", "ditl", "sal", "ncal", "ol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "ev", "evolahib", "evolib", "evohib", "ep_stn", "tsc"],
        'reit': ["ta", "tca", "cabb", "lcabb", "i", "nraar", "nr", "ar", "tr", "cdfa", "cpdoraoa", "tnca", "ppe", "pi", "rei", "iiaajv", "iia", "iijv", "gwaia", "gw", "ia", "ncdfa", "ncpdoraoa", "tl", "tcl", "stl", "tp", "npaap", "np", "ap", "stll", "stdfl", "stafcrdaoraol", "tncl", "ltl", "ltll", "ditl", "ltdfl", "ltafcrdaoraol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "uii", "ep_stn", "tsc"],
        'other_financial': ["ta", "cabb", "laatc", "dfa", "fiafvtpol", "fiafvtoci", "ppe", "roua", "rei", "iiaajv", "iia", "iijv", "gwaia", "gw", "ia", "dita", "oa", "tl", "l", "dfl", "flafvtpol", "tp", "ditl", "ol", "toe", "sc", "spa", "oei", "psioei", "pcsioei", "capr", "is", "r", "rr", "ors", "surr", "rtp", "shfsas", "ecocb", "tetshaoehopc", "tetoshopc", "etmsh", "rnci", "ep_stn", "tsc"],
    },
}
_FS_ENDPOINT = {t: f'hk/company/fs/{t}' for t in
                ('non_financial', 'bank', 'security', 'insurance', 'reit', 'other_financial')}
_FS_PERIOD = {'first_quarterly_report': 'Q', 'interim_report': 'Q', 'third_quarterly_report': 'Q', 'annual_report': 'Q'}


def _lxr_fs(ts_code: str, start: str, end: str, kind: str, common: dict, by_type: dict, flow: bool) -> pd.DataFrame:
    """单只股票的理杏仁财报：[start, end] 两端包含，跨度 ≤10 年(文档)，单只股票一次最多 128 个指标(文档)，超过分批请求。
    接口区间按财报日过滤，公告晚 1~4 个月：start 倒 366 天(_back)。
    财报类型读本地依赖表 hk_stock_basic 的 fs_type，六类各认一套字段(_FS_FIELDS[kind][fs_type])，整类全取：
    能对上现有列的(common + by_type[fs_type])填进那一列，其余存成 kind_代码 列；没有 fs_type 的返回空。
    flow=True 是利润表：Q 行取单期原始值(季报公司 q.c_o 单季；只出半年报的公司没有单季值，用 hy.c_o 半年)，Y 行取年报的
    y.t_o；flow=False 是资产负债表时点值 q.t_o，Q 行每份财报一行，年报再写一行 Y。
    end_date 是报告期的名字(period_label，按年报的财报日定第几季)，period_end 是财报日，filing_date 是 reportDate(公告日)，
    利润表另存 auditOpinionType。
    """
    from qteasy import QT_DATA_SOURCE
    from qteasy.fmpfuncs import period_label
    max_metrics = 128
    basic = QT_DATA_SOURCE.read_table_data('hk_stock_basic', shares=ts_code)
    fs_type = basic['fs_type'].iloc[0] if not basic.empty else None
    if fs_type not in _FS_ENDPOINT:
        logger_core.info(f'lixingren fs {ts_code}: fs_type={fs_type} unknown, skipped')
        return pd.DataFrame()
    named = {m.split('.')[1]: col for col, m in {**common, **by_type[fs_type]}.items()}   # 代码 → 现有列
    fields = {code: named.get(code, f'{kind}_{code}') for code in _FS_FIELDS[kind][fs_type]}  # 代码 → 落到哪一列
    start = _back(regulate_date_format(start, force_format='date'), 366) if start else None   # 区间按财报日过滤，公告晚 1~4 个月
    end = regulate_date_format(end, force_format='date') if end else None
    metrics = sorted({f'q.{kind}.{c}.{"c_o" if flow else "t_o"}' for c in fields}
                     | ({f'hy.{kind}.{c}.c_o' for c in fields} | {f'y.{kind}.{c}.t_o' for c in fields} if flow else set()))
    data = {}
    for i in range(0, len(metrics), max_metrics):
        chunk = metrics[i:i + max_metrics]
        part = guard(f'lixingren-{_FS_ENDPOINT[fs_type]}', dict(ts_code=ts_code, start=start, end=end),
                     lambda ts_code, start, end:
                     _lxr_post(_FS_ENDPOINT[fs_type], stockCodes=[_to_lxr_code(ts_code)],
                               startDate=start, endDate=end, metricsList=chunk),
                     fmt='%Y-%m-%d')
        for item in part:                                   # 同一份财报的各批合并(按财报日)
            got = data.setdefault(item['date'], item)
            for g in ('q', 'hy', 'y'):
                if g in item:
                    got.setdefault(g, {}).setdefault(kind, {}).update(item[g].get(kind, {}))
    if not data:
        return pd.DataFrame()

    def pick(item, gran, code, ctype):
        node = item.get(gran, {}).get(kind, {}).get(code, {})
        return node.get(ctype) if isinstance(node, dict) else None

    rows = []
    for item in data.values():
        base = dict(ts_code=ts_code, period_end=pd.Timestamp(item['date'][:10]),
                    filing_date=pd.Timestamp(item['reportDate'][:10]) if item.get('reportDate') else None,
                    currency=item.get('currency'), report_type=item.get('reportType'))
        if flow:
            base['audit_opinion'] = item.get('auditOpinionType')
            q = {col: pick(item, 'q', c, 'c_o') for c, col in fields.items()}
            if all(v is None for v in q.values()):          # 没有单季值：半年报公司
                q = {col: pick(item, 'hy', c, 'c_o') for c, col in fields.items()}
            rows.append({**base, 'period': 'Q', **q})
            if item.get('reportType') == 'annual_report':
                rows.append({**base, 'period': 'Y', **{col: pick(item, 'y', c, 't_o') for c, col in fields.items()}})
        else:
            q = {col: pick(item, 'q', c, 't_o') for c, col in fields.items()}
            rows.append({**base, 'period': 'Q', **q})
            if item.get('reportType') == 'annual_report':
                rows.append({**base, 'period': 'Y', **q})
    res = pd.DataFrame(rows)
    annual = res.loc[res['report_type'] == 'annual_report', 'period_end']
    if annual.empty:
        logger_core.info(f'lixingren fs {ts_code}: no annual report in range, period name undefined, skipped')
        return pd.DataFrame()
    res['end_date'] = period_label(res['period_end'].values, annual.max()).values
    res['fiscal_year'] = res['end_date'].dt.year.astype(str)
    return res.drop(columns='report_type')


def hk_income(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股利润表(理杏仁，原始值)：该类全部 ps 字段，对上现有列的见 _FS_INCOME_COMMON + _FS_INCOME[fs_type]，其余 ps_代码。"""
    if ts_code is None:
        return pd.DataFrame()
    return _lxr_fs(ts_code, start, end, 'ps', _FS_INCOME_COMMON, _FS_INCOME, flow=True)


def hk_balance(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股资产负债表(理杏仁，原始值)：该类全部 bs 字段，对上现有列的见 _FS_BALANCE_COMMON + _FS_BALANCE[fs_type]，其余 bs_代码。"""
    if ts_code is None:
        return pd.DataFrame()
    return _lxr_fs(ts_code, start, end, 'bs', _FS_BALANCE_COMMON, _FS_BALANCE, flow=False)


_INDICATOR = {   # hk_stock_indicator 列 → 理杏仁 fundamental 指标。sp/spc/spa/tv/ta 与日线表重复、sharesPerLot 在股票表、估值统计是派生值，不存
    'turnover_rate': 'to_r', 'pe_ttm': 'pe_ttm', 'pb': 'pb', 'ps_ttm': 'ps_ttm', 'pcf_ttm': 'pcf_ttm', 'dv_ratio': 'dyr',
    'total_mv': 'mc', 'h_mv': 'mc_om', 'sc_hold': 'ah_sh', 'sc_hold_mv': 'ah_shm', 'sc_net_buy': 'mm_nba',
}


def hk_stock_indicator(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股每日估值指标(理杏仁 hk/company/fundamental/<fs_type>，按本地 hk_stock_basic 的 fs_type 路由)：
    [start, end] 两端包含，跨度 ≤10 年(文档)；列见 _INDICATOR。
    """
    if ts_code is None:
        return pd.DataFrame()
    from qteasy import QT_DATA_SOURCE
    basic = QT_DATA_SOURCE.read_table_data('hk_stock_basic', shares=ts_code)
    fs_type = basic['fs_type'].iloc[0] if not basic.empty else None
    if fs_type not in _FS_ENDPOINT:
        logger_core.info(f'lixingren fundamental {ts_code}: fs_type={fs_type} unknown, skipped')
        return pd.DataFrame()
    path = f'hk/company/fundamental/{fs_type}'
    start = regulate_date_format(start, force_format='date') if start else None
    end = regulate_date_format(end, force_format='date') if end else None
    data = guard(f'lixingren-{path}', dict(ts_code=ts_code, start=start, end=end),
                 lambda ts_code, start, end:
                 _lxr_post(path, stockCodes=[_to_lxr_code(ts_code)], startDate=start, endDate=end,
                           metricsList=list(_INDICATOR.values())),
                 fmt='%Y-%m-%d')
    raw = _frame(data, {'date': 'date', **{m: 'num' for m in _INDICATOR.values()}})
    if raw.empty:
        return pd.DataFrame()
    res = pd.DataFrame({'ts_code': ts_code, 'trade_date': raw['date']})
    for col, m in _INDICATOR.items():
        res[col] = raw[m]
    return res


def hk_dividend(ts_code: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """港股分红(理杏仁 hk/company/dividend)：每条公告一行，"无股息"的公告也存(cash_div 为 0)；同一天两条(季息 + 特别息)靠 content 区分。
    接口区间按公告日过滤，理杏仁录入晚于公告 8 天以上(实测中电 10-12 公告、10-20 未录)：start 倒 366 天。"""
    if ts_code is None:
        return pd.DataFrame()
    raw = _lxr_dated('hk/company/dividend', ts_code, start, end, back=366,
                     fields={'date': 'date', 'content': 'str', 'fsEndDate': 'date', 'status': 'str', 'dividend': 'num',
                             'currency': 'str', 'dividendAmount': 'num', 'marketDividendAmount': 'num',
                             'annualNetProfit': 'num', 'annualNetProfitDividendRatio': 'num',
                             'bonusSharesFromProfit': 'num', 'bonusSharesFromCapitalReserve': 'num',
                             'registerDate': 'date', 'exDate': 'date', 'paymentDate': 'date'})
    if raw.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        'ts_code':              ts_code,
        'ann_date':             raw['date'],
        'content':              raw['content'],
        'fs_end_date':          raw['fsEndDate'],
        'div_proc':             raw['status'],
        'cash_div':             raw['dividend'],
        'currency':             raw['currency'],
        'div_amount':           raw['dividendAmount'],
        'h_div_amount':         raw['marketDividendAmount'],
        'annual_np':            raw['annualNetProfit'],
        'payout_ratio':         raw['annualNetProfitDividendRatio'],
        'bonus_shares_profit':  raw['bonusSharesFromProfit'],
        'bonus_shares_reserve': raw['bonusSharesFromCapitalReserve'],
        'record_date':          raw['registerDate'],
        'ex_date':              raw['exDate'],
        'pay_date':             raw['paymentDate'],
    })
