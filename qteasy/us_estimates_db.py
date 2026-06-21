# coding=utf-8
"""us_estimates 表的领域对象：schema(与 datatables.py 镜像) + 多源下载 + 写入。"""

import pandas as pd


class EstimateDatabase:
    TABLE = 'us_estimates'

    # {列名: 类型}  类型与 datatables.py 的 us_estimates 定义保持一致(两边手动同步)；示例/含义见行尾
    SCHEMA = {
        'ts_code':              'varchar',  # 'AAPL'        股票代码
        'trade_date':           'date',     # '2026-06-16'  快照日期(美东时区, 即拉取这条预期的日期)
        'target_date':          'date',     # '2026-09-28'  预测目标期截止日(财报期末)
        'target_period':        'varchar',  # 'Q'           目标期类型: 'Q'=季度 / 'Y'=年度
        'eps':                  'double',   # 3.14          预测 EPS 均值 (美元/股)
        'eps_high':             'double',   # 3.40          预测 EPS 上限 (美元/股)
        'eps_low':              'double',   # 2.90          预测 EPS 下限 (美元/股)
        'revenue':              'double',   # 9.5e10        预测营业收入均值 (美元)
        'revenue_high':         'double',   # 1.0e11        预测营业收入上限 (美元)
        'revenue_low':          'double',   # 9.0e10        预测营业收入下限 (美元)
        'net_profit':           'double',   # 2.5e10        预测净利润均值 (美元)
        'net_profit_high':      'double',   # 2.7e10        预测净利润上限 (美元)
        'net_profit_low':       'double',   # 2.3e10        预测净利润下限 (美元)
        'ebitda':               'double',   # 3.0e10        预测 EBITDA 均值 (美元)
        'ebitda_high':          'double',   # 3.2e10        预测 EBITDA 上限 (美元)
        'ebitda_low':           'double',   # 2.8e10        预测 EBITDA 下限 (美元)
        'ebit':                 'double',   # 2.8e10        预测 EBIT 均值 (美元)
        'ebit_high':            'double',   # 3.0e10        预测 EBIT 上限 (美元)
        'ebit_low':             'double',   # 2.6e10        预测 EBIT 下限 (美元)
        'sga_expense':          'double',   # 6.5e9         预测 SG&A 费用均值 (美元)
        'sga_expense_high':     'double',   # 7.0e9         预测 SG&A 费用上限 (美元)
        'sga_expense_low':      'double',   # 6.0e9         预测 SG&A 费用下限 (美元)
        'target_price':         'double',   # None          目标价 (美元/股); FMP analyst-estimates 不提供, 暂为空
        'num_analysts_eps':     'int',      # 25            参与 EPS 预测的分析师数量
        'num_analysts_revenue': 'int',      # 24            参与营收预测的分析师数量
    }
    COLUMNS = list(SCHEMA)
    PRIMARY_KEYS = ['ts_code', 'trade_date', 'target_date', 'target_period']

    # 币种缓存：唯一一个，按 source 区分（持久化到 us_stock_currency 的 source 列）
    _CURRENCY_CACHE = {}     # {(source, ts_code): currency}
    _CURRENCY_LOADED = False

    # ── 币种：通用调度（缓存+写表），按 source 调本源查询(封装在 source 模块) ──
    def _currency_of(self, source: str, ts_code: str) -> str:
        cls = EstimateDatabase
        if not cls._CURRENCY_LOADED:
            from qteasy import QT_DATA_SOURCE
            tbl = QT_DATA_SOURCE.read_table_data('us_stock_currency', primary_key_in_index=False)
            if not tbl.empty:
                cls._CURRENCY_CACHE = {(r['source'], r['ts_code']): r['currency']
                                       for _, r in tbl.iterrows()}
            cls._CURRENCY_LOADED = True
        key = (source, ts_code)
        if key in cls._CURRENCY_CACHE:
            return cls._CURRENCY_CACHE[key]
        if source == 'fmp':
            from .fmpfuncs import us_reported_currency
            cur = us_reported_currency(ts_code)
        else:
            raise NotImplementedError(f'currency source: {source}')
        cls._CURRENCY_CACHE[key] = cur
        from qteasy import QT_DATA_SOURCE
        QT_DATA_SOURCE.update_table_data(
            'us_stock_currency',
            pd.DataFrame([{'ts_code': ts_code, 'source': source, 'currency': cur}]),
            merge_type='update')
        return cur

    def download_from_source2(self, ts_code: str) -> pd.DataFrame:
        raise NotImplementedError

    COMPARE_KEYS = ['eps', 'revenue', 'net_profit', 'ebitda', 'ebit']
    COMPARE_TOL = 0.01  # 共识相对变化 >1% 才记一条；吸收汇率抖动与单分析师噪声

    def changelog(self, df: pd.DataFrame, data_source=None) -> pd.DataFrame:
        if df.empty:
            return df
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        ts_code = df['ts_code'].iloc[0]
        existing = data_source.read_table_data(self.TABLE, shares=ts_code, primary_key_in_index=False)
        if existing.empty:
            return df
        existing['target_date'] = pd.to_datetime(existing['target_date'])
        baseline = (existing.sort_values('trade_date')
                    .groupby(['target_date', 'target_period']).last())
        keep = []
        for _, row in df.iterrows():
            key = (row['target_date'], row['target_period'])
            if key not in baseline.index:
                keep.append(True)
                continue
            base = baseline.loc[key]
            changed = False
            for c in self.COMPARE_KEYS:
                if pd.isna(row[c]):
                    continue
                a, b = float(row[c]), float(base[c])
                if pd.isna(b) or abs(a - b) > abs(b) * self.COMPARE_TOL:
                    changed = True  # 原来缺失、或相对变化超阈值 → 实质变化
                    break
            keep.append(changed)
        return df[keep]

    def insert(self, df: pd.DataFrame, data_source=None) -> int:
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        df = self.changelog(df, data_source)
        if df.empty:
            return 0
        return data_source.update_table_data(self.TABLE, df, merge_type='update')
