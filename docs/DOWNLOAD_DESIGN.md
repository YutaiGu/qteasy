# 下载逻辑设计

标准：**绝不静默丢数据**。要么拿到完整数据，要么报错。

- 下载任务默认是适当的，失败不续拉，整个任务重跑：准确优先于效率。

## 1. 路由（API_MAP → 参数清单）

决定调哪个接口、按什么迭代（逐日 / 逐股 / 按日期范围），产出参数清单交给下载器。

map 每行 7 列：

| 列 | 名称 | 含义 | 取值 |
|---|---|---|---|
| 1 | `api` | 下载函数名（数据源模块里的同名函数） | 如 `daily`、`us_income` |
| 2 | `arg_name` | 迭代参数传给下载函数时的参数名 | 如 `ts_code`、`trade_date`、`exchange`、`ann_date`；不迭代填 `none` |
| 3 | `arg_type` | 按什么生成清单 | `table_index` 逐只代码；`trade_date` / `us_trade_date` 逐交易日（A 股 / 美股日历）；`datetime` 逐自然日；`month` / `quarter` 逐月 / 逐季；`list` 逐个列表值；`none` 不迭代 |
| 4 | `arg_rng` | 取值范围，随第 3 列 | `table_index`：基础表名（即依赖表）；日期类：最早日期；`list`：逗号分隔的值；`none`：不用 |
| 5 | `allowed_code_suffix` | `table_index` 时只保留这些后缀的代码 | 如 `SH,SZ`；空 = 不限 |
| 6 | `allow_start_end` | 区间是否附加日期范围 | `Y` 附加 start / end；`C` 同样附加，且为翻页接口，函数内取全，下载器不验证；空 = 不附加 |
| 7 | `start_end_chunk_size` | 第 6 列为 `Y` 时按多少天分段 | 只用于接口有时间窗口限制的情况；空 = 整段一个区间 |

依赖表：交易日类的行依赖 `trade_calendar`，`table_index` 行依赖第 4 列的基础表，下载前先更新。

map 是路由器，负责准确、高效。一张表可写多行，每行是为一种请求设计的最高效下载方式，由第 3 列 `arg_type` 区分：

- 未传 symbols（全市场）→ 非 `table_index` 行，如 stock_daily 按交易日一次拉全市场
- 传了 symbols（特例，一律逐股）→ `table_index` 行，单股一次拉完整段日期

同类的多行按优先级排列（如 vip 接口在前、普通接口在后）：

```
路由(表, 参数):
    适用的行 = 传了 symbols ? table_index 行 : (非 table_index 行 or table_index 行)
    if 没有适用的行: 报错                    # map 设计错误：能逐股就该写 ts_code 行，不能逐股就不该传 symbols
    for 行 in 适用的行:                     # 顺序即优先级
        try:   用这一行出清单并下载
        except 失败: continue               # 无权限等任何失败：整张清单换同类的下一行重新生成
        else:  return
    报错
```

不传时自动补：

| 参数 | 补成 |
|---|---|
| start / end | 表的最早日期（第 4 列）到今天 |
| symbols（`table_index` 行） | 第 4 列基础表的全部代码，按第 5 列后缀过滤。基础表在本地，下载前必须先更新依赖表，不能静默用旧表 |
| list_arg_filter（`list` 行） | 第 4 列的全部值 |

清单的每个区间（一次请求的参数，如 `{ts_code, start, end}`）有 6 种形态：

| 形态 | 一个区间 | 下载器怎么切 |
|---|---|---|
| ① 只有日期范围 | `{start, end}` | 切日期 |
| ② 代码 + 日期范围 | `{ts_code, start, end}` | 切日期 |
| ③ 单个日期 | `{trade_date}` | 不切 |
| ④ 单个期 | `{month}` / `{quarter}` / `{period}` | 不切 |
| ⑤ 列表值 | `{exchange}` 等 | 不切，标 `C` 或一次取全 |
| ⑥ 只有代码 | `{ts_code}` | 补上日期范围后切日期 |

必须保证：

1. **只有按行数截断**：翻页、默认分页在下载函数内部处理；按时间窗口截断（如 FMP 5 分钟线最多 10 天）用第 7 列分段。
2. **日期范围两端包含**：下载函数对外的 start、end 当天都返回；接口本身不是的，由下载函数换算。否则二分会漏掉切点那一天。
3. **最小单元不超限**：下载器只切日期，最多拆到单时点（一天 / 一期）。单时点可能超限的：接口能翻页就标 `C`，否则 map 写逐股的行。下载器不切股票（接口一般不接受一次传大量代码）。

无特殊限制的接口，每个区间尽可能大，切分交给下载器。

没有可切维度、只能翻页的接口（如 `fund_basic`、FMP 报表类）：第 6 列填 `C`，翻页由下载函数自己维护（含该接口越界时是否返回空页）；`C` 行同样附加日期范围，下载函数用不上可以忽略。翻页的完整性依赖接口本身，是已知的例外，接口须实测翻页稳定后才能标 `C`。

## 2. 防静默截断（guard，二叉树验证）

guard 是区间维护器，保护的是**每一次 API 请求**，不是下载函数：下载函数内部可能调用多个 API、再过滤组合。下载函数里的每个 API 请求都经 guard，把区间交给它，它把区间传给接口、验证返回的数据；lambda 只负责把区间的字段对应到接口自己的参数名：

```
res = guard('daily', dict(ts_code=ts_code, trade_date=trade_date, start=start, end=end),
            lambda ts_code, trade_date, start, end:
            pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start, end_date=end))
```

没有时间字段、所在行又不是 `C` 的请求，没有任何东西保证取全，guard 报错。

```
K = 100            # 默认任何 api 的单次 limit 都不低于 100 行
M = 0              # 该 api 见过的最大行数（每个 api 一份），M ≤ limit
M_is_limit = False # M、M_is_limit 每次下载任务开始时重置

二分(区间):
    if 日期跨不止一个时点:                    # 按自然日对半，[start, mid] 和 [mid+1, end]
        return 日期前半, 日期后半             # 首尾相接，不漏非交易日（周末公告等）
    return None, None                       # 单时点：最小单元

guard(区间, rows):                           # rows = 这个区间请求到的数据；"交回"指交回给下载函数
    n = len(rows)
    if n == 0: return
    if 区间没有日期范围: 交回(rows); return   # 单时点：最小单元；C 行无时间字段：函数内自己取全
    if n < M or n < K: 交回(rows); return    # 小于已知下界，一定没截断
    左, 右 = 二分(区间)

    if M_is_limit:                          # n == limit，一定截断
        if 左 == None: 报错; return          # 最小单元超限：路由保证被打破
        guard(左, 请求(左))
        guard(右, 请求(右))
        return

    M = n
    if 左 == None: 交回(rows); return        # 最小单元：由路由保证不超限
    l, r = 请求(左), 请求(右)
    if len(l) == 0 or len(r) == 0 or len(l) + len(r) > n:
        if len(l) + len(r) > n:             # 两半之和 > 整段：整段被截断，n 就是 limit
            M_is_limit = True
        guard(左, l)
        guard(右, r)
    else:
        交回(rows)                          # 两半非空且之和 == n：完整
```

- 被截断的那次返回丢弃，由它拆出的两半重新请求；各段确认完整后合并交回下载函数。

## 3. 限流

**计数**：在 guard 里按真实请求次数计（含二分多发的请求、翻页的每一页），各线程共用。调用时传 `download_batch_size` / `download_batch_interval`，每调用这么多次暂停这么多秒。按限额主动节流只为少撞限，**正确性靠撞限后等待重试**。

**限流识别**由每个数据源自己维护：tushare 认报错文本"每分钟最多访问该接口"，FMP 认 HTTP 429。

```
拉取(区间):
    for 等待 in (60, 120, 240 秒, 无):       # 各数据源自己维护这串等待
        try:
            return 调用接口(区间)
        except 每分钟超限:
            if 等待 == 无: 报错                # 连撞 4 次：并行或 batch 设大了，人工调
            等待后重试同一个区间                 # 偶尔撞一次不会断
        except 其他额度（每天 / 每小时）或无权限:
            报错                               # 无权限时路由会换同类的下一行
        except 其他错误:
            退避重试几次，仍失败则报错
```

- 撞限只重试同一个区间，不跳过：识别错了只会变慢或报错，不会丢数据。

## 4. 依赖表（第 8 列）

map 每行可加第 8 列 `dependent_tables`（可省略）：额外的依赖表，多张用逗号分隔，下载前先更新。

第 3、4 列写的是迭代逻辑，由它衍生的依赖表不用另写（交易日类的行依赖 `trade_calendar`，`table_index` 行依赖第 4 列的基础表）。其他依赖写第 8 列，如 FMP 分红日历返回全球股票，下载函数读 `us_stock_basic` 自己过滤。

## 5. 全市场行失败退到逐股行

未传 symbols 时，全市场行都失败后换逐股行，下载依赖表的全部代码，数据范围相同。

## 6. 数据源自己的脏数据

主键等关键字段缺失的记录是数据源的问题，不是没取全：丢弃后打警告，不报错。

## 附录：待落实

- **依赖表不能跨通道**：一次 refill 只有一个 channel，依赖表也用它下载；依赖表不在该通道时 core 只打印一句"can't be fetched"就跳过，主表照常下载，依赖表没更新也不报错。现在 `estimates_fmp`（fmp 通道）依赖 tushare 的 `income`，只能不写第 8 列、靠日更顺序保证。要改：第 8 列支持写 `tushare:income`，下载器按指定通道拉；依赖表拉不到应报错而不是跳过。
- **非主键文本超长被静默截断**：上游 `write_table_data` 的 `_clip_df_to_column_dtypes` 把超过 varchar 定义的字段直接截断写入，不报错不警告。主键不受影响（`_drop_oversized_primary_keys` 先整行丢弃并警告）。至少应改为截断时警告。

