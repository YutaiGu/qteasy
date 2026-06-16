# coding=utf-8
"""us_estimates 表的领域对象：schema(与 datatables.py 镜像) + 多源下载 + 写入。"""

import pandas as pd
import pytz


class EstimateDatabase:
    TABLE = 'us_estimates'

    # schema —— 必须与 datatables.py 的 us_estimates 定义保持一致(两边手动同步)，供本类各方法使用
    COLUMNS = [
        # 列名                     类型    示例           含义
        'ts_code',               # str,   'AAPL'         股票代码
        'trade_date',            # date,  '2026-06-16'   快照日期(美东时区, 即拉取这条预期的日期)
        'target_date',           # date,  '2026-09-28'   预测目标期截止日(财报期末)
        'target_period',         # str,   'Q'            目标期类型: 'Q'=季度 / 'Y'=年度
        'eps',                   # float, 3.14           预测 EPS 均值 (美元/股)
        'eps_high',              # float, 3.40           预测 EPS 上限 (美元/股)
        'eps_low',               # float, 2.90           预测 EPS 下限 (美元/股)
        'revenue',               # float, 9.5e10         预测营业收入均值 (美元)
        'revenue_high',          # float, 1.0e11         预测营业收入上限 (美元)
        'revenue_low',           # float, 9.0e10         预测营业收入下限 (美元)
        'net_profit',            # float, 2.5e10         预测净利润均值 (美元)
        'net_profit_high',       # float, 2.7e10         预测净利润上限 (美元)
        'net_profit_low',        # float, 2.3e10         预测净利润下限 (美元)
        'ebitda',                # float, 3.0e10         预测 EBITDA 均值 (美元)
        'ebitda_high',           # float, 3.2e10         预测 EBITDA 上限 (美元)
        'ebitda_low',            # float, 2.8e10         预测 EBITDA 下限 (美元)
        'ebit',                  # float, 2.8e10         预测 EBIT 均值 (美元)
        'ebit_high',             # float, 3.0e10         预测 EBIT 上限 (美元)
        'ebit_low',              # float, 2.6e10         预测 EBIT 下限 (美元)
        'sga_expense',           # float, 6.5e9          预测 SG&A 费用均值 (美元)
        'sga_expense_high',      # float, 7.0e9          预测 SG&A 费用上限 (美元)
        'sga_expense_low',       # float, 6.0e9          预测 SG&A 费用下限 (美元)
        'target_price',          # float, None           目标价 (美元/股); FMP analyst-estimates 不提供, 暂为空
        'num_analysts_eps',      # int,   25             参与 EPS 预测的分析师数量
        'num_analysts_revenue',  # int,   24             参与营收预测的分析师数量
    ]
    PRIMARY_KEYS = ['ts_code', 'trade_date', 'target_date', 'target_period']

    # FMP analyst-estimates 字段 -> 表列
    FMP_MAP = {
        'epsAvg':               'eps', 
        'epsHigh':              'eps_high', 
        'epsLow':               'eps_low',
        'revenueAvg':           'revenue', 
        'revenueHigh':          'revenue_high', 
        'revenueLow':           'revenue_low',
        'netIncomeAvg':         'net_profit', 
        'netIncomeHigh':        'net_profit_high', 
        'netIncomeLow':         'net_profit_low',
        'ebitdaAvg':            'ebitda', 
        'ebitdaHigh':           'ebitda_high', 
        'ebitdaLow':            'ebitda_low',
        'ebitAvg':              'ebit', 
        'ebitHigh':             'ebit_high', 
        'ebitLow':              'ebit_low',
        'sgaExpenseAvg':        'sga_expense', 
        'sgaExpenseHigh':       'sga_expense_high', 
        'sgaExpenseLow':        'sga_expense_low',
        'numAnalystsEps':       'num_analysts_eps', 
        'numAnalystsRevenue':   'num_analysts_revenue',
    }

    def download_from_fmp(self, ts_code: str) -> pd.DataFrame:
        from .fmpfuncs import _fmp_request
        trade_date = pd.Timestamp.now(tz=pytz.timezone('America/New_York')).normalize().tz_localize(None)
        rows = []
        for period in ('annual', 'quarter'):
            for item in _fmp_request('analyst-estimates', symbol=ts_code, period=period):
                d = item.get('date', '')
                if len(d) < 10:
                    continue
                row = dict.fromkeys(self.COLUMNS)
                row['ts_code'] = ts_code
                row['trade_date'] = trade_date
                row['target_date'] = pd.Timestamp(d[:10])
                row['target_period'] = 'Y' if period == 'annual' else 'Q'
                for src, col in self.FMP_MAP.items():
                    row[col] = item.get(src)
                rows.append(row)
        return pd.DataFrame(rows, columns=self.COLUMNS)

    def download_from_source2(self, ts_code: str) -> pd.DataFrame:
        raise NotImplementedError

    def insert(self, df: pd.DataFrame, data_source=None) -> int:
        if df.empty:
            return 0
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        return data_source.update_table_data(self.TABLE, df, merge_type='update')
