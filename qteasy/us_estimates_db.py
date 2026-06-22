# coding=utf-8
"""us_estimates 表的领域对象：schema(与 datatables.py 镜像) + 多源下载 + 写入。"""

import pandas as pd


class EstimateDatabase:
    """一致预期领域基类（source/市场无关）：统一的 changelog 去重 + insert 写入。

    各市场做子类，声明自己的 TABLE / SCHEMA / PRIMARY_KEYS 及源专属逻辑：
      UsEstimateDatabase   美股(FMP 源)    -> us_estimates
      AEstimateDatabase    A股(tushare 源)  -> estimates
    去重/写库逻辑只写一遍，靠 self.TABLE / self.COMPARE_KEYS 多态适配各自的表。
    """
    COMPARE_KEYS = ['eps', 'revenue', 'net_profit', 'ebitda', 'ebit']
    COMPARE_TOL = 0.01  # 共识相对变化 >1% 才记一条；吸收汇率抖动与单分析师噪声

    def _changed(self, row, base) -> bool:
        """row 相对基线 base 是否实质变化:任一比较键相对变动 > 容差(或基线缺失)。base=None 视为新增。"""
        if base is None:
            return True
        for c in self.COMPARE_KEYS:
            if pd.isna(row[c]):
                continue
            a, b = float(row[c]), float(base[c])
            if pd.isna(b) or abs(a - b) > abs(b) * self.COMPARE_TOL:
                return True  # 原来缺失、或相对变化超阈值 → 实质变化
        return False

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
            base = baseline.loc[key] if key in baseline.index else None
            keep.append(self._changed(row, base))
        return df[keep]

    def insert(self, df: pd.DataFrame, data_source=None) -> int:
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        df = self.changelog(df, data_source)
        if df.empty:
            return 0
        return data_source.update_table_data(self.TABLE, df, merge_type='update')


class UsEstimateDatabase(EstimateDatabase):
    """美股一致预期(FMP 源)：写入 us_estimates；build 在 fmpfuncs.us_estimates。"""
    TABLE = 'us_estimates'

    # {列名: 类型}  与 datatables.py 的 us_estimates 定义对齐
    SCHEMA = {
        'ts_code':              'varchar',  # 'AAPL'
        'trade_date':           'date',     # 快照日期(美东时区)
        'target_date':          'date',     # 预测目标期截止日(财报期末)
        'target_period':        'varchar',  # 'Q' / 'Y'
        'eps':                  'double',
        'eps_high':             'double',
        'eps_low':              'double',
        'revenue':              'double',
        'revenue_high':         'double',
        'revenue_low':          'double',
        'net_profit':           'double',
        'net_profit_high':      'double',
        'net_profit_low':       'double',
        'ebitda':               'double',
        'ebitda_high':          'double',
        'ebitda_low':           'double',
        'ebit':                 'double',
        'ebit_high':            'double',
        'ebit_low':             'double',
        'sga_expense':          'double',
        'sga_expense_high':     'double',
        'sga_expense_low':      'double',
        'target_price':         'double',   # FMP analyst-estimates 不提供, 暂为空
        'num_analysts_eps':     'int',
        'num_analysts_revenue': 'int',
    }
    COLUMNS = list(SCHEMA)
    PRIMARY_KEYS = ['ts_code', 'trade_date', 'target_date', 'target_period']

    # 币种缓存(美股折算用)：按 source 区分（持久化到 us_stock_currency）
    _CURRENCY_CACHE = {}     # {(source, ts_code): currency}
    _CURRENCY_LOADED = False

    def _currency_of(self, source: str, ts_code: str) -> str:
        """该股申报币种(给金额折算用)，缓存并持久化到 us_stock_currency；按 source 调本源查询。"""
        cls = UsEstimateDatabase
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


class AEstimateDatabase(EstimateDatabase):
    """A股券商一致预期(来源 tushare 的 report_rc)：结构 = us_estimates + dividend，写入表 estimates。

    去重/写入逻辑全继承 EstimateDatabase；本类只声明自己的 schema(datatables 与之对齐)。
    report_rc → estimates 的 build 在源模块 tsfuncs(对称于 fmpfuncs.us_estimates)。
    """
    TABLE = 'estimates'
    SCHEMA = {
        'ts_code':              'varchar',  # '600111.SH'
        'trade_date':           'date',     # 观测日(研报日)
        'target_date':          'date',     # 预测目标期截止日(财报期末)
        'target_period':        'varchar',  # 期型: Y / Q1 / H1 / Q3
        'eps':                  'double',
        'eps_high':             'double',
        'eps_low':              'double',
        'revenue':              'double',
        'revenue_high':         'double',
        'revenue_low':          'double',
        'net_profit':           'double',
        'net_profit_high':      'double',
        'net_profit_low':       'double',
        'ebitda':               'double',   # report_rc 无, 留空(结构对齐用)
        'ebitda_high':          'double',
        'ebitda_low':           'double',
        'ebit':                 'double',   # report_rc 无, 留空
        'ebit_high':            'double',
        'ebit_low':             'double',
        'sga_expense':          'double',   # report_rc 无, 留空
        'sga_expense_high':     'double',
        'sga_expense_low':      'double',
        'target_price':         'double',
        'dividend':             'double',   # A股独有: 股息率
        'num_analysts_eps':     'int',
        'num_analysts_revenue': 'int',
    }
    COLUMNS = list(SCHEMA)
    PRIMARY_KEYS = ['ts_code', 'trade_date', 'target_date', 'target_period']

    # report_rc.quarter 类型 → 标准期型(A股披露口径: Q1 / H1中报 / Q3 / Y年报) 及期末日
    _QUARTER_MAP = {'Q1': 'Q1', 'Q2': 'H1', 'Q3': 'Q3', 'Q4': 'Y', 'H1': 'H1', 'H2': 'Y', 'Y': 'Y'}
    _PERIOD_END  = {'Q1': '0331', 'H1': '0630', 'Q3': '0930', 'Y': '1231'}
    # 各期型法定披露截止日(年报次年4/30,余当年);未来期没真实披露日时拿它兜底
    _DEADLINE_MD = {'Q1': (0, '0430'), 'H1': (0, '0831'), 'Q3': (0, '1031'), 'Y': (1, '0430')}
    _share_cache = {}   # (ts_code, trade_date) -> total_share 万股

    @classmethod
    def _legal_deadline(cls, target_date, target_period):
        """该期法定披露截止日;真实 ann_date 缺(未来期)时作右端兜底。"""
        off, md = cls._DEADLINE_MD[target_period]
        return pd.Timestamp(f'{target_date.year + off}{md}')

    @staticmethod
    def _disclosure_map(ts_code, data_source):
        """该股各期末日 -> 真实披露日(income.ann_date 取最早)。report_rc 有效区间的右端来源。"""
        inc = data_source.read_table_data('income', shares=ts_code, primary_key_in_index=False)
        if inc.empty or 'ann_date' not in inc.columns:
            return {}
        inc = inc[['end_date', 'ann_date']].dropna()
        inc['end_date'] = pd.to_datetime(inc['end_date'])
        inc['ann_date'] = pd.to_datetime(inc['ann_date'])
        return inc.groupby('end_date')['ann_date'].min().to_dict()

    # ── tushare 源转换：report_rc(各分析师单条预测) → 快照 → 继承的 changelog/insert ──
    def build(self, ts_code, data_source=None, start_date=None, end_date=None, stale_days=365) -> pd.DataFrame:
        """report_rc → 该股 estimates 稀疏 change-log,返回去重后的 df(不写库)。
        在 [start_date, end_date] 内每个研报日产一个共识快照,以"库内已有 + 本次累积"为基线,
        只保留相对上一版变化>1%的行(吸收汇率/单分析师噪声)。区间留空=全历史回放。
        返回 df 交由上层 channel 走正常管线写入(进度条按标的可见),不再内部偷偷写。
        """
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        rc = data_source.read_table_data('report_rc', shares=ts_code, primary_key_in_index=False)
        if rc.empty:
            return pd.DataFrame(columns=self.COLUMNS)
        rc = self._prepare(rc, ts_code)
        if rc.empty:
            return pd.DataFrame(columns=self.COLUMNS)
        disc = self._disclosure_map(ts_code, data_source)  # 期末 -> 真实披露日(右端切用)
        obs = rc['report_date_dt'].dropna()
        if start_date:
            obs = obs[obs >= pd.Timestamp(start_date)]
        if end_date:
            obs = obs[obs <= pd.Timestamp(end_date)]
        # 基线 = 库内已有(增量去重) + 本次累积(同股内逐观测日去重)
        baseline = {}
        existing = data_source.read_table_data(self.TABLE, shares=ts_code, primary_key_in_index=False)
        if not existing.empty:
            existing['target_date'] = pd.to_datetime(existing['target_date'])
            for _, r in existing.sort_values('trade_date').iterrows():
                baseline[(r['target_date'], r['target_period'])] = r
        kept = []
        for d in sorted(obs.unique()):
            snap = self._snapshot(rc, pd.Timestamp(d), ts_code, stale_days, disc)
            for _, row in snap.iterrows():
                key = (row['target_date'], row['target_period'])
                if self._changed(row, baseline.get(key)):
                    kept.append(row.to_dict())
                    baseline[key] = row
        return pd.DataFrame(kept).reindex(columns=self.COLUMNS) if kept else pd.DataFrame(columns=self.COLUMNS)

    def refill(self, ts_code, data_source=None, start_date=None, end_date=None, stale_days=365) -> int:
        """直接建表用:build 出去重 df 后写库(merge=update),返回写入行数。channel 路径不走这里(走 build)。"""
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        df = self.build(ts_code, data_source, start_date, end_date, stale_days)
        if df.empty:
            return 0
        return data_source.update_table_data(self.TABLE, df, merge_type='update')

    @classmethod
    def _target(cls, quarter):
        """report_rc.quarter('2026Q1') → (target_date, 期型)；无法解析返回 (None, None)。"""
        if not isinstance(quarter, str) or len(quarter) < 5 or not quarter[:4].isdigit():
            return None, None
        y, q = quarter[:4], quarter[4:]
        t = cls._QUARTER_MAP.get(q)
        return (pd.Timestamp(f'{y}{cls._PERIOD_END[t]}'), t) if t else (None, None)

    @classmethod
    def _total_share(cls, ts_code, report_date):
        """研报当日(或之前最近交易日)的总股本(万股)，调 tushare daily_basic 并按(股,日)缓存；缺则 None。
        点时态:历史预测用【当时】的股本,只往前推到最近交易日。"""
        import tushare as ts
        from .utilfuncs import nearest_market_trade_day
        td = nearest_market_trade_day(pd.Timestamp(report_date), 'SSE')  # ≤ 研报日的最近交易日
        if td is None:
            return None
        day = td.strftime('%Y%m%d')
        key = (ts_code, day)
        if key not in cls._share_cache:
            df = ts.pro_api().daily_basic(ts_code=ts_code, trade_date=day, fields='ts_code,trade_date,total_share')
            cls._share_cache[key] = (float(df['total_share'].iloc[0])
                                     if not df.empty and pd.notna(df['total_share'].iloc[0]) else None)
        return cls._share_cache[key]

    def _prepare(self, rc, ts_code):
        """report_rc 明细 → 附 target_date/target_period 及派生数值列(营收/净利 万元→元)。"""
        td_tp = rc['quarter'].apply(self._target)
        rc = rc.assign(target_date=[x[0] for x in td_tp], target_period=[x[1] for x in td_tp])
        rc = rc[rc['target_period'].notna()].copy()
        rc['report_date_dt'] = pd.to_datetime(rc['report_date'], errors='coerce')
        rc = rc[rc['report_date_dt'].notna()]

        def _eps(r):
            e = pd.to_numeric(r['eps'], errors='coerce')
            if pd.notna(e) and e != 0:
                return float(e)
            npv = pd.to_numeric(r['np'], errors='coerce')
            if pd.isna(npv) or npv == 0:
                return None
            sh = self._total_share(ts_code, r['report_date_dt'])   # 研报【当时】的股本
            return round(npv / sh, 4) if sh and sh > 0 else None    # np(万元)/total_share(万股) = 元/股

        rc['_eps']        = rc.apply(_eps, axis=1)
        rc['_revenue']    = pd.to_numeric(rc['op_rt'], errors='coerce') * 1e4
        rc['_net_profit'] = pd.to_numeric(rc['np'],    errors='coerce') * 1e4
        rc['_dividend']   = pd.to_numeric(rc['rd'],    errors='coerce')
        rc['_tp']         = rc[['max_price', 'min_price']].apply(
                                lambda c: pd.to_numeric(c, errors='coerce')).mean(axis=1)
        return rc

    @staticmethod
    def _mhl(s):
        s = pd.to_numeric(s, errors='coerce').dropna()
        return (None, None, None) if s.empty else (
            round(float(s.mean()), 4), round(float(s.max()), 4), round(float(s.min()), 4))

    def _snapshot(self, rc, obs_date, ts_code, stale_days, disc=None):
        """obs_date 这天的共识快照(每机构取≤该日最新预测) → df(列对齐 self.COLUMNS)。
        只保留 obs_date < 该期披露日(income.ann_date,缺则法定截止)的期:已披露=实际,不再算预期。"""
        disc = disc or {}
        win = rc[(rc['report_date_dt'] <= obs_date) &
                 (rc['report_date_dt'] >= obs_date - pd.Timedelta(days=stale_days))]
        if win.empty:
            return pd.DataFrame(columns=self.COLUMNS)
        latest = (win.sort_values('report_date_dt')
                     .groupby(['org_name', 'target_date', 'target_period'], as_index=False).last())
        rows = []
        for (tdate, tp), g in latest.groupby(['target_date', 'target_period']):
            expiry = disc.get(tdate) or self._legal_deadline(tdate, tp)
            if obs_date >= expiry:        # 该期实际已披露 → 共识失效,右开不含
                continue
            eps_m, eps_h, eps_l = self._mhl(g['_eps'])
            rev_m, rev_h, rev_l = self._mhl(g['_revenue'])
            np_m,  np_h,  np_l  = self._mhl(g['_net_profit'])
            tp_v = pd.to_numeric(g['_tp'],       errors='coerce').dropna()
            dv_v = pd.to_numeric(g['_dividend'], errors='coerce').dropna()
            rows.append({
                'ts_code': ts_code, 'trade_date': obs_date.normalize(),
                'target_date': tdate, 'target_period': tp,
                'eps': eps_m, 'eps_high': eps_h, 'eps_low': eps_l,
                'revenue': rev_m, 'revenue_high': rev_h, 'revenue_low': rev_l,
                'net_profit': np_m, 'net_profit_high': np_h, 'net_profit_low': np_l,
                'target_price': round(float(tp_v.mean()), 2) if not tp_v.empty else None,
                'dividend': round(float(dv_v.mean()), 4) if not dv_v.empty else None,
                'num_analysts_eps': int(pd.to_numeric(g['_eps'], errors='coerce').notna().sum()),
                'num_analysts_revenue': int(pd.to_numeric(g['_revenue'], errors='coerce').notna().sum()),
            })
        return pd.DataFrame(rows).reindex(columns=self.COLUMNS)
