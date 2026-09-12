# coding=utf-8
"""us_estimates 表的领域对象：schema(与 datatables.py 镜像) + 多源下载 + 写入。"""

import numpy as np
import pandas as pd

ERROR_FLOOR = 1e-3      # 误差下限占实际值的比例，避免 log(0)
MAX_HERDING = 0.95      # 抱团程度上限，避免权重塌缩到单个人


class EstimateDatabase:
    """一致预期领域基类（source/市场无关）：统一的 changelog 去重 + insert 写入。

    各市场做子类，声明自己的 TABLE / SCHEMA / PRIMARY_KEYS 及源专属逻辑：
      UsEstimateDatabase   美股(FMP 源)    -> us_estimates
      AEstimateDatabase    A股(tushare 源)  -> estimates
    去重/写库逻辑只写一遍，靠 self.TABLE / self.COMPARE_KEYS 多态适配各自的表。

    另附一组共识加权的纯计算方法(score/skill/herding/weights/consensus)：按分析师历史准确度
    定权重，代替简单平均。只有拿得到单个分析师预测的市场用得上(A股 report_rc)；FMP 只给共识，
    美股用不到这组方法。所有取值都由数据算出，没有可调参数。
    """
    COMPARE_KEYS = ['eps', 'revenue', 'net_profit', 'ebitda', 'ebit']
    COMPARE_TOL = 0.01  # 共识相对变化 >1% 才记一条；吸收汇率抖动与单分析师噪声

    # ── 共识加权：打分 → 能力 → 权重 ──
    @staticmethod
    def score(forecast: pd.Series, actual: float) -> pd.Series:
        """同期各条预测的得分：log(本条误差 ÷ 同期误差中位数)。0=与同行持平，负=更准。

        除以同期中位数而不是实际值，抵消了"预测期越远越难"，不同时点的分数才可比；
        用中位数不用均值，避免被个别离谱值带偏。同期不足 2 条无从比较，返回空。
        """
        forecast = pd.to_numeric(forecast, errors='coerce').dropna()
        if len(forecast) < 2 or not actual or pd.isna(actual):
            return pd.Series(dtype=float)
        error = (forecast - actual).abs().clip(lower=abs(actual) * ERROR_FLOOR)
        middle = error.median()
        return np.log(error / middle) if middle > 0 else pd.Series(dtype=float)

    @staticmethod
    def skill(scores: pd.DataFrame, by: str = 'analyst') -> pd.Series:
        """各分析师的能力分 = 平均分 × 可信度。负数表示比同行准。

        可信度 = n / (n + 噪声方差/能力方差)：记录越多、或人与人差距越明显，越采信他自己的
        平均分，否则拉回平均水平。两个方差都从分数本身估计。差距全是噪声时返回全 0(等权)。
        """
        grouped = scores.groupby(by)['score']
        mean, count, var = grouped.mean(), grouped.size(), grouped.var()
        repeated = count > 1
        if not repeated.any():
            return mean * 0.0
        noise = ((count - 1) * var)[repeated].sum() / (count - 1)[repeated].sum()
        spread = mean.var() - (noise / count).mean()        # 总离散度扣掉噪声 = 真实差距
        if not noise or pd.isna(noise) or pd.isna(spread) or spread <= 0:
            return mean * 0.0
        return mean * count / (count + noise / spread)

    @staticmethod
    def herding(scores: pd.DataFrame, by: str = 'period') -> float:
        """抱团程度 0~1：同一期里"大家一起偏"的部分，占全部偏离的比例。

        接近 1 表示步调一致，多一个人不带来新信息，权重应当集中；接近 0 表示各想各的，
        人多能互相抵消误差，权重应当分散。scores 需含带符号的偏离列 bias。
        """
        grouped = scores.groupby(by)['bias'].agg(['mean', 'var', 'size'])
        grouped = grouped[grouped['size'] >= 3].dropna()
        if len(grouped) < 3:
            return 0.0
        common, individual = grouped['mean'].var(), grouped['var'].mean()
        total = common + individual
        return float(np.clip(common / total, 0.0, MAX_HERDING)) if total > 0 else 0.0

    @staticmethod
    def weights(skill: pd.Series, herding: float = 0.0) -> pd.Series:
        """能力分 → 归一化权重。

        误差互不相关时，最优权重是精度的平方(即误差平方的倒数)；抱团越重，重复的信息被扣得
        越多，跟不上的人权重归零，权重自然向少数人集中。退化时返回等权。
        """
        precision = np.exp(-skill.astype(float))
        if precision.empty:
            return precision
        overlap = (herding * precision.sum() / (1 + (len(precision) - 1) * herding)
                   if herding > 0 else 0.0)
        raw = (precision * (precision - overlap)).clip(lower=0)
        if raw.sum() <= 0:
            return pd.Series(1.0 / len(precision), index=precision.index)
        return raw / raw.sum()

    @staticmethod
    def consensus(forecast: pd.Series, weight: pd.Series) -> tuple:
        """加权共识 (均值, 最高, 最低)。高低值只在有权重的人里取，与均值同口径。"""
        forecast = pd.to_numeric(forecast, errors='coerce')
        valid = forecast.notna() & (weight.reindex(forecast.index).fillna(0) > 0)
        if not valid.any():
            return None, None, None
        value, share = forecast[valid], weight.reindex(forecast.index)[valid]
        return (round(float((value * share).sum() / share.sum()), 4),
                round(float(value.max()), 4), round(float(value.min()), 4))

    def _changed(self, row, base) -> bool:
        """row 相对基线 base 是否实质变化:任一比较键相对变动 > 容差(或基线缺失)。base=None 视为新增。"""
        if base is None:
            return True
        for c in self.COMPARE_KEYS:
            if pd.isna(row[c]) or pd.isna(base[c]):   # 任一侧缺值 → 当作相同，跳过该列
                continue
            a, b = float(row[c]), float(base[c])
            if abs(a - b) > abs(b) * self.COMPARE_TOL:
                return True  # 相对变化超阈值 → 实质变化
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
    # 金额单位: USD
    # 精度: 无代码层截断, 精度由各数据源保证
    SCHEMA = {
        'ts_code':              'varchar',  # 美股代码, 无后缀, 例: 'AAPL'
        'trade_date':           'date',     # 快照日(美东时区), 例: '2025-07-25'
        'target_date':          'date',     # 预测目标期末日(财报截止日), 例: '2025-12-31'
        'target_period':        'varchar',  # 期型: 'Y'年报 / 'Q'季报
        'eps':                  'double',   # USD/股, 均值, 例: 7.25
        'eps_high':             'double',   # USD/股, 最高预测
        'eps_low':              'double',   # USD/股, 最低预测
        'revenue':              'double',   # USD 总额, 营业收入均值, 例: 3.95e11
        'revenue_high':         'double',   # USD 总额, 最高预测
        'revenue_low':          'double',   # USD 总额, 最低预测
        'net_profit':           'double',   # USD 总额, 净利润均值, 例: 9.7e10
        'net_profit_high':      'double',   # USD 总额, 最高预测
        'net_profit_low':       'double',   # USD 总额, 最低预测
        'ebitda':               'double',   # USD 总额, EBITDA均值
        'ebitda_high':          'double',   # USD 总额, 最高预测
        'ebitda_low':           'double',   # USD 总额, 最低预测
        'ebit':                 'double',   # USD 总额, EBIT均值
        'ebit_high':            'double',   # USD 总额, 最高预测
        'ebit_low':             'double',   # USD 总额, 最低预测
        'sga_expense':          'double',   # USD 总额, 销售管理费用均值
        'sga_expense_high':     'double',   # USD 总额, 最高预测
        'sga_expense_low':      'double',   # USD 总额, 最低预测
        'target_price':         'double',   # USD/股, 目标价均值, 预留, 当前无数据
        'num_analysts_eps':     'int',      # EPS 预测分析师数, 例: 15
        'num_analysts_revenue': 'int',      # 营收预测分析师数, 例: 12
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
    # 金额单位: CNY(元)
    SCHEMA = {
        'ts_code':              'varchar',  # A股代码, .SH/.SZ/.BJ 后缀, 例: '600111.SH'
        'trade_date':           'date',     # 观测日(研报日), 例: '2025-07-25'
        'target_date':          'date',     # 预测目标期末日(财报截止日), 例: '2025-12-31'
        'target_period':        'varchar',  # 期型: 'Y'年报 / 'Q'季报(与 us_estimates 统一)
        'eps':                  'double',   # 元/股, 均值, round(4), 例: 1.2345
        'eps_high':             'double',   # 元/股, 最高预测, round(4)
        'eps_low':              'double',   # 元/股, 最低预测, round(4)
        'revenue':              'double',   # 元 总额, 营业收入均值, round(4), 例: 1.23e10
        'revenue_high':         'double',   # 元 总额, 最高预测, round(4)
        'revenue_low':          'double',   # 元 总额, 最低预测, round(4)
        'net_profit':           'double',   # 元 总额, 净利润均值, round(4), 例: 2.34e9
        'net_profit_high':      'double',   # 元 总额, 最高预测, round(4)
        'net_profit_low':       'double',   # 元 总额, 最低预测, round(4)
        'ebitda':               'double',   # 预留, 永远 NULL(结构对齐)
        'ebitda_high':          'double',   # 预留, 永远 NULL(结构对齐)
        'ebitda_low':           'double',   # 预留, 永远 NULL(结构对齐)
        'ebit':                 'double',   # 预留, 永远 NULL(结构对齐)
        'ebit_high':            'double',   # 预留, 永远 NULL(结构对齐)
        'ebit_low':             'double',   # 预留, 永远 NULL(结构对齐)
        'sga_expense':          'double',   # 预留, 永远 NULL(结构对齐)
        'sga_expense_high':     'double',   # 预留, 永远 NULL(结构对齐)
        'sga_expense_low':      'double',   # 预留, 永远 NULL(结构对齐)
        'target_price':         'double',   # 元/股, 目标价均值, round(2), 例: 45.50
        'dividend':             'double',   # %, 股息率均值, round(4), 例: 1.2345
        'num_analysts_eps':     'int',      # EPS 预测机构数, 例: 8
        'num_analysts_revenue': 'int',      # 营收预测机构数, 例: 6
    }
    COLUMNS = list(SCHEMA)
    PRIMARY_KEYS = ['ts_code', 'trade_date', 'target_date', 'target_period']

    # report_rc.quarter 类型 → 标准期型('Q'季报 / 'Y'年报) 及期末日
    _QUARTER_MAP = {'Q1': 'Q', 'Q2': 'Q', 'Q3': 'Q', 'Q4': 'Y', 'H1': 'Q', 'H2': 'Y', 'Y': 'Y'}
    _PERIOD_END  = {'Q1': '0331', 'Q2': '0630', 'Q3': '0930', 'Q4': '1231'}
    # 法定披露截止日,按 target_date.month 区分;未来期没真实披露日时拿它兜底
    _DEADLINE_MD = {3: (0, '0430'), 6: (0, '0831'), 9: (0, '1031'), 12: (1, '0430')}
    _share_cache = {}   # ts_code -> DataFrame(trade_date, total_share 万股)

    @classmethod
    def _legal_deadline(cls, target_date, target_period):
        """该期法定披露截止日;真实 ann_date 缺(未来期)时作右端兜底。"""
        off, md = cls._DEADLINE_MD[target_date.month]
        return pd.Timestamp(f'{target_date.year + off}{md}')

    @staticmethod
    def _disclosure_map(ts_code, data_source):
        """该股各期末日 -> 最早披露日(业绩预告/快报/正式财报三者取最早)。

        这一天起该期的数字已经公开，之后发的预测不再是预测，共识到此为止；同时它也是打分
        窗口的边界：同一窗口内的研报掌握同样的信息，彼此才可比。
        """
        dates = []
        for table in ('forecast', 'express', 'income'):
            try:
                df = data_source.read_table_data(table, shares=ts_code, primary_key_in_index=False)
            except Exception:
                continue
            if df.empty or 'ann_date' not in df.columns or 'end_date' not in df.columns:
                continue
            part = df[['end_date', 'ann_date']].dropna()
            part['end_date'] = pd.to_datetime(part['end_date'])
            part['ann_date'] = pd.to_datetime(part['ann_date'])
            dates.append(part)
        if not dates:
            return {}
        return pd.concat(dates).groupby('end_date')['ann_date'].min().to_dict()

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
        rc = self._prepare(rc, ts_code, data_source)
        if rc.empty:
            return pd.DataFrame(columns=self.COLUMNS)
        rc['_authors'] = rc['author_name'].map(self._authors) if 'author_name' in rc else [[]] * len(rc)
        disc = self._disclosure_map(ts_code, data_source)  # 期末 -> 最早披露日(右端切用)
        graded = self._graded(rc, ts_code, data_source)    # 已揭晓的分数，按人展开
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
        tables = {}                                        # 最近一次揭晓日 -> (能力分, 抱团度)
        for d in sorted(obs.unique()):
            day = pd.Timestamp(d)
            known = graded[graded['reveal'] < day] if not graded.empty else graded
            key = known['reveal'].max() if not known.empty else None
            if key not in tables:
                tables[key] = self._skill_tables(known)    # 能力只在有新结果揭晓时才变，按此缓存
            skill_of, herd = tables[key]
            snap = self._snapshot(rc, day, ts_code, stale_days, disc, skill_of, herd)
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
        """report_rc.quarter('2026Q2') → (target_date, 期型); 日期用原始quarter查, 标签用映射后。"""
        if not isinstance(quarter, str) or len(quarter) < 5 or not quarter[:4].isdigit():
            return None, None
        y, q = quarter[:4], quarter[4:]
        d = cls._PERIOD_END.get(q)
        t = cls._QUARTER_MAP.get(q)
        return (pd.Timestamp(f'{y}{d}'), t) if d and t else (None, None)

    @classmethod
    def _share_history(cls, ts_code, data_source):
        """该股历年总股本(万股)，取自本地 stock_indicator，按交易日升序；缺表则空。

        点时态:历史预测要用【当时】的股本换算，送转股前后口径才不会错位。早先逐条调
        tushare daily_basic，全量重建时会打爆接口，改读本地表后零外部调用。
        """
        if ts_code in cls._share_cache:
            return cls._share_cache[ts_code]
        try:
            ind = data_source.read_table_data('stock_indicator', shares=ts_code,
                                              primary_key_in_index=False)
        except Exception:
            ind = pd.DataFrame()
        if ind.empty or 'total_share' not in ind.columns:
            history = pd.DataFrame(columns=['trade_date', 'total_share'])
        else:
            history = ind[['trade_date', 'total_share']].copy()
            history['trade_date'] = pd.to_datetime(history['trade_date'], errors='coerce')
            history['total_share'] = pd.to_numeric(history['total_share'], errors='coerce')
            history = history.dropna().drop_duplicates('trade_date', keep='last').sort_values('trade_date')
        cls._share_cache[ts_code] = history
        return history

    def _prepare(self, rc, ts_code, data_source=None):
        """report_rc 明细 → 附 target_date/target_period 及派生数值列(营收/净利 万元→元)。

        季报一律转成【单季】(本期累计 − 上期累计)，与 us_estimates 口径统一；年报(Y)保持全年。
        缺相邻期时该条留空，不猜。eps 由净利润除以研报当时的股本派生，不用源数据的 eps，
        避开各家股本口径不一致的问题。
        """
        td_tp = rc['quarter'].apply(self._target)
        rc = rc.assign(target_date=[x[0] for x in td_tp], target_period=[x[1] for x in td_tp])
        rc = rc[rc['target_period'].notna()].copy()
        rc['report_date_dt'] = pd.to_datetime(rc['report_date'], errors='coerce')
        rc = rc[rc['report_date_dt'].notna()]
        if rc.empty:
            return rc
        rc['_row'] = rc.index                                  # 原始行号，merge_asof 会重排索引

        rc['_revenue']    = pd.to_numeric(rc['op_rt'], errors='coerce') * 1e4
        rc['_net_profit'] = pd.to_numeric(rc['np'],    errors='coerce') * 1e4
        rc = self._to_single_quarter(rc)
        rc['_dividend']   = pd.to_numeric(rc['rd'], errors='coerce')
        rc['_tp']         = rc[['max_price', 'min_price']].apply(
                                lambda c: pd.to_numeric(c, errors='coerce')).mean(axis=1)

        shares = self._share_history(ts_code, data_source) if data_source is not None else None
        if shares is None or shares.empty:
            rc['_eps'] = None
            return rc
        merged = pd.merge_asof(rc.sort_values('report_date_dt'), shares,
                               left_on='report_date_dt', right_on='trade_date', direction='backward')
        merged['_eps'] = (merged['_net_profit'] / (merged['total_share'] * 1e4)).round(4)
        return merged.drop(columns=['trade_date', 'total_share'])

    @staticmethod
    def _to_single_quarter(rc):
        """季报的累计值转单季：同一份研报里，本期累计 − 同年上一期累计。年报(Y)不动。

        A股财报按年初至今累计披露，券商预测沿用同一口径；美股本就是单季。统一成单季后
        各期之间才能直接比较，也才能和 us_estimates 对齐。
        """
        rc = rc.copy()
        quarterly = rc['target_period'] == 'Q'
        if not quarterly.any():
            return rc
        rc['_year'] = rc['target_date'].dt.year
        keys = ['org_name', 'report_date_dt', '_year']
        for column in ('_revenue', '_net_profit'):
            ordered = rc.sort_values(keys + ['target_date'])
            previous = ordered.groupby(keys)[column].shift(1)      # 同一研报、同一年的上一期累计
            gap = ordered['target_date'].dt.month - ordered.groupby(keys)['target_date'].shift(1).dt.month
            single = ordered[column] - previous
            # 只有相邻期(相差一个季度)才能相减；上期缺失或不相邻则留空
            usable = ordered['target_period'].eq('Q') & gap.eq(3) & previous.notna()
            first_quarter = ordered['target_period'].eq('Q') & ordered['target_date'].dt.month.eq(3)
            rc.loc[ordered.index[usable], column] = single[usable]
            drop = ordered['target_period'].eq('Q') & ~usable & ~first_quarter
            rc.loc[ordered.index[drop], column] = None              # 一季度本身即单季，无需相减
        return rc.drop(columns=['_year'])

    @staticmethod
    def _mhl(s):
        s = pd.to_numeric(s, errors='coerce').dropna()
        return (None, None, None) if s.empty else (
            round(float(s.mean()), 4), round(float(s.max()), 4), round(float(s.min()), 4))

    # ── 打分：把每条研报的准确度写回 report_rc，供 build 加权时取用 ──
    METRIC_SCORES = {'_net_profit': 'score_np', '_revenue': 'score_op_rt'}

    @staticmethod
    def _actuals(ts_code, data_source):
        """各期实际值与揭晓日：index=期末日，列 _net_profit/_revenue/reveal。

        实际值取正式财报第一次公布的版本(不用事后调整版)，揭晓日即该次公告日——分数只有过了
        这一天才允许使用，否则历史回放会用到当时还不知道的信息。
        """
        inc = data_source.read_table_data('income', shares=ts_code, primary_key_in_index=False)
        if inc.empty:
            return pd.DataFrame(columns=['_net_profit', '_revenue', 'reveal'])
        inc = inc.copy()
        inc['end_date'] = pd.to_datetime(inc['end_date'], errors='coerce')
        inc['ann_date'] = pd.to_datetime(inc['ann_date'], errors='coerce')
        if 'report_type' in inc.columns:
            merged = inc[inc['report_type'].astype(str) == '1']       # 1 = 合并报表
            inc = merged if not merged.empty else inc
        inc = inc.dropna(subset=['end_date', 'ann_date']).sort_values('ann_date')
        first = inc.groupby('end_date').first()
        actual = pd.DataFrame({
            '_net_profit': pd.to_numeric(first.get('n_income_attr_p'), errors='coerce'),
            '_revenue': pd.to_numeric(first.get('revenue'), errors='coerce'),
            'reveal': first['ann_date'],
        })
        return actual[actual['reveal'].notna()]

    def rescore(self, ts_code, data_source=None) -> int:
        """给该股所有研报打分并写回 report_rc 的 score_* 列，返回打分条数。

        分数只给"该期尚未披露任何信息"时发出的预测：预告或快报一出，后续预测等于抄答案。
        同一披露窗口内每家机构取最新一条参与比较，窗口内不足 2 条则整窗不打分。
        """
        if data_source is None:
            from qteasy import QT_DATA_SOURCE
            data_source = QT_DATA_SOURCE
        raw = data_source.read_table_data('report_rc', shares=ts_code, primary_key_in_index=False)
        if raw.empty:
            return 0
        rc = self._prepare(raw.copy(), ts_code, data_source)
        actual = self._actuals(ts_code, data_source)
        disc = self._disclosure_map(ts_code, data_source)
        if rc.empty or actual.empty or not disc:
            return 0

        bounds = pd.DatetimeIndex(sorted(set(disc.values())))
        rc['_window'] = bounds[bounds.searchsorted(rc['report_date_dt'], side='right') - 1].where(
            rc['report_date_dt'] >= bounds[0])
        rc['_cutoff'] = rc['target_date'].map(disc)                   # 该期最早披露日
        live = rc[rc['_cutoff'].notna() & (rc['report_date_dt'] < rc['_cutoff'])
                  & rc['_window'].notna()]
        usable = live[live['target_date'].isin(actual.index)]
        scored = 0
        for column, score_column in self.METRIC_SCORES.items():
            raw[score_column] = np.nan
            for _, group in usable.groupby(['target_date', 'target_period', '_window']):
                target = group['target_date'].iloc[0]
                latest = group.sort_values('report_date_dt').groupby('org_name').last()
                value = self.score(latest.set_index('_row')[column], actual.loc[target, column])
                if value.empty:
                    continue
                raw.loc[value.index, score_column] = value.values   # _row 即 raw 的行号
                scored += len(value)
        if not scored:
            return 0
        return data_source.update_table_data('report_rc', raw, merge_type='update')

    def _graded(self, rc, ts_code, data_source):
        """已打分的研报 → 按作者展开的长表：analyst/score/bias/period/reveal/metric。

        bias 是带符号的偏离(log 预测/实际)，用来量化抱团；reveal 是该期正式财报的公告日，
        分数过了这天才可用。report_rc 尚未打分(score 列全空)时返回空表，共识退化为等权。
        """
        columns = ['analyst', 'score', 'bias', 'period', 'reveal', 'metric']
        actual = self._actuals(ts_code, data_source)
        disc = self._disclosure_map(ts_code, data_source)
        if actual.empty or not disc:
            return pd.DataFrame(columns=columns)
        bounds = pd.DatetimeIndex(sorted(set(disc.values())))
        window = pd.Series(bounds[bounds.searchsorted(rc['report_date_dt'], side='right') - 1],
                           index=rc.index).where(rc['report_date_dt'] >= bounds[0])
        parts = []
        for column, score_column in self.METRIC_SCORES.items():
            if score_column not in rc.columns:
                continue
            scored = rc[pd.to_numeric(rc[score_column], errors='coerce').notna()
                        & rc['target_date'].isin(actual.index)]
            if scored.empty:
                continue
            truth = actual.loc[scored['target_date'], column].values
            forecast = pd.to_numeric(scored[column], errors='coerce').values
            with np.errstate(divide='ignore', invalid='ignore'):
                bias = np.log(np.where((forecast > 0) & (truth > 0), forecast / truth, np.nan))
            part = pd.DataFrame({
                'analyst': scored['_authors'].values,
                'score': pd.to_numeric(scored[score_column], errors='coerce').values,
                'bias': bias,
                # 抱团度要在同一批信息条件下比较，所以按"目标期 + 所在披露窗口"分组
                'period': scored['target_date'].astype(str).values + '|'
                          + window[scored.index].astype(str).values,
                'reveal': actual.loc[scored['target_date'], 'reveal'].values,
                'metric': score_column,
            })
            parts.append(part.explode('analyst').dropna(subset=['analyst']))
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)

    def _skill_tables(self, graded):
        """已揭晓的分数 → ({指标: 各分析师能力分}, {指标: 抱团度})。空表则都为空，等权。"""
        skill_of, herd = {}, {}
        if graded.empty:
            return skill_of, herd
        for metric, part in graded.groupby('metric'):
            skill_of[metric] = self.skill(part)
            herd[metric] = self.herding(part.dropna(subset=['bias']))
        return skill_of, herd

    @staticmethod
    def _authors(name):
        """作者栏 → 作者列表。一份研报常由多人署名，分数与权重按人计。"""
        import re
        if not isinstance(name, str) or not name.strip():
            return []
        return [a for a in re.split(r'[、,，;；/\\|]+|\s{1,}', name.strip()) if a]

    def _metric_weights(self, group, score_column, skill_of, herd):
        """某指标在该组内的权重：机构 → 其作者能力分的均值 → 基类权重公式。

        score_column 只用来区分指标(净利润/营收各有各的分数与权重)，能力分已按该指标算好。
        """
        if score_column not in skill_of or skill_of[score_column].empty:
            return pd.Series(1.0 / len(group), index=group.index)
        table = skill_of[score_column]
        skill = pd.Series(
            [float(np.mean([table.get(a, 0.0) for a in authors])) if authors else 0.0
             for authors in group['_authors']], index=group.index)
        return self.weights(skill, herd.get(score_column, 0.0))

    def _snapshot(self, rc, obs_date, ts_code, stale_days, disc=None, skill_of=None, herd=None):
        """obs_date 这天的共识快照(每机构取≤该日最新预测) → df(列对齐 self.COLUMNS)。

        只保留 obs_date < 该期披露日(预告/快报/财报最早者,缺则法定截止)的期:已披露=实际,不再算预期。
        共识按分析师历史准确度加权；skill_of/herd 由 build 按"当日已揭晓"的分数算好传入，
        不传则退化为等权(即简单平均)。
        """
        skill_of, herd = skill_of or {}, herd or {}
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
            np_w = self._metric_weights(g, 'score_np', skill_of, herd)
            rev_w = self._metric_weights(g, 'score_op_rt', skill_of, herd)
            np_m,  np_h,  np_l  = self.consensus(g['_net_profit'], np_w)
            rev_m, rev_h, rev_l = self.consensus(g['_revenue'], rev_w)
            eps_m, eps_h, eps_l = self.consensus(g['_eps'], np_w)     # eps 派生自净利润，跟随其权重
            if np_m is None and rev_m is None:                        # 该期无可用预测(如单季缺相邻期)
                continue
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
