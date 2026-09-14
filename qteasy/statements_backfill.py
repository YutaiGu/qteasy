# coding=utf-8
"""三大财报(income/balance/cashflow)全量回补，不走 refill_data_source。

按公告日区间逐年拉全市场(_vip_bisect 二分防 vip 静默截断)，每年拉完立即写库。现在回头拉历史，
截至今天发布过的所有版本(快报、正式报告、日后修订)都沿用原始公告日，会随所在年份一起拉下来。
日更走 refill → tsfuncs 逐天按实际发布日拉，见 tsfuncs._by_publish_day。
"""

import time

import pandas as pd
import tushare as ts

from qteasy.__init__ import logger_core, QT_CONFIG
from qteasy.datatables import get_built_in_table_schema
from qteasy.tsfuncs import ERRORS_TO_CHECK_ON_RETRY, STATEMENT_CAPS, _vip_bisect
from qteasy.utilfuncs import retry

VIP_APIS = {'income': 'income_vip', 'balance': 'balancesheet_vip', 'cashflow': 'cashflow_vip'}


def _log(message):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} [backfill] {message}', flush=True)
    logger_core.info(f'[backfill] {message}')


def backfill_statements(tables=('income', 'balance', 'cashflow'), start_year=2010, end_year=None,
                        data_source=None) -> dict:
    """逐表逐年回补，返回 {表名: 写库行数}。

    Parameters
    ----------
    tables: str or iterable
        'income,balance,cashflow' 或列表
    start_year / end_year: int
        公告日所在年份区间，end_year 缺省为今年。中途断了，从断掉的那年用 start_year 接着跑
    data_source: DataSource
        缺省为 QT_DATA_SOURCE

    返回字段取表定义的列，与 tsfuncs 默认字段一致；写库行数是数据库返回值(内容没变的行记 0)。
    """
    if data_source is None:
        from qteasy import QT_DATA_SOURCE
        data_source = QT_DATA_SOURCE
    if isinstance(tables, str):
        tables = [table.strip() for table in tables.split(',')]
    end_year = end_year or pd.Timestamp.today().year
    with_retry = retry(ERRORS_TO_CHECK_ON_RETRY, tries=QT_CONFIG.hist_dnld_retry_cnt,
                       delay=QT_CONFIG.hist_dnld_retry_wait, backoff=QT_CONFIG.hist_dnld_backoff,
                       mute=True, logger=logger_core)
    pro = ts.pro_api()

    written = {}
    for table in tables:
        api = with_retry(getattr(pro, VIP_APIS[table]))
        fields = ','.join(get_built_in_table_schema(table)[0])
        written[table] = 0
        for year in range(start_year, end_year + 1):
            began = time.time()
            rows = _vip_bisect(api, f'{year}0101', f'{year}1231', cap=STATEMENT_CAPS[table], fields=fields)
            count = data_source.update_table_data(table, rows, merge_type='update') if not rows.empty else 0
            written[table] += count
            _log(f'{table} {year}: 下载 {len(rows)} 行，写库 {count} 行，{time.time() - began:.0f}s')
        _log(f'{table} 完成：写库 {written[table]} 行')
    return written
