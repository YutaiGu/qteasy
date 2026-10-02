# coding=utf-8
# ======================================
# File:     test_trader_cli.py
# Author:   Jackie PENG
# Contact:  jackie.pengzhao@gmail.com
# Created:  2024-03-04
# Desc:
#   Unittest for trader CLI.
#   使用专用测试 DataSource（非 QT_DATA_SOURCE），
#   测试结束后清理表数据。
#   Build 1 CLI 覆盖矩阵（PR 附件）见 TestTraderCLIBuild1Coverage。
# ======================================

import json
import logging
import os
from typing import Optional
import shutil
import tempfile
import unittest
import time
import io
from contextlib import redirect_stdout
from unittest.mock import patch, MagicMock
import pandas as pd
from rich.text import Text

import qteasy as qt
from qteasy import DataSource, Operator
from qteasy.trader import Trader
from qteasy.trader import (
    TraderMessage,
    coerce_trader_message,
    _live_logger_name,
    reset_live_logger_handlers,
    dataframe_log_preview,
    group_sys_log_physical_lines,
)
from qteasy.trader_cli import (
    TraderShell,
    DEBUG_RUN_TASK_CHOICES,
    CLI_COMMAND_ALIASES,
    BROKER_SUBCOMMANDS,
    ModeMenuExitRequest,
    _filter_sys_log_lines,
    _drain_message_queue,
    _accept_mode_menu_char,
    _parse_mode_menu_choice,
    _parse_error_recovery_choice,
    _read_line_with_timeout,
    _read_line_with_timeout_unix,
    _normalize_task_status_filter,
    format_trader_tasks_table,
)
from qteasy.trading_util import (
    process_account_delivery,
    process_trade_result,
    submit_order,
    update_position,
    list_live_trade_artifacts,
    sys_log_file_path_name,
    trade_log_file_path_name,
    break_point_file_path_name,
    risk_log_file_path_name,
)
from qteasy.trade_recording import new_account, read_trade_order_detail, save_parsed_trade_orders
from qteasy.trade_recording import get_or_create_position, get_position_by_id, get_account
from qteasy.trade_recording import query_trade_orders
from qteasy.broker import SimulatorBroker
from qteasy.risk import MaxOrderQtyRule, RiskManager


def _detach_live_logger_handlers(account_id: Optional[int] = None) -> None:
    """拆掉 live 相关 Logger 的 Handler，防止跨用例重复挂载导致单行双写。"""
    from qteasy.trader import reset_live_logger_handlers

    reset_live_logger_handlers(account_id)


def _get_cli_test_data_dir():
    """CLI 测试专用数据目录，不使用默认 QT_DATA_SOURCE。"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_test_trader_cli')


def _clear_cli_test_tables(datasource):
    """清理 CLI 测试用到的表数据，测试完成后调用。"""
    for table in ['sys_op_live_accounts', 'sys_op_positions', 'sys_op_trade_orders', 'sys_op_trade_results']:
        if datasource.table_data_exists(table):
            datasource.drop_table_data(table)


class TestTraderCLI(unittest.TestCase):

    def setUp(self):

        config = {
            'mode':                  0,
            'time_zone':             'local',
            'market_open_time_am':   '09:30:00',
            'market_close_time_pm':  '15:30:00',
            'market_open_time_pm':   '13:00:00',
            'market_close_time_am':  '11:30:00',
            'exchange':              'SSE',
            'cash_delivery_period':  0,
            'stock_delivery_period': 0,
            'asset_pool':            '000001.SZ, 000002.SZ, 000004.SZ, 000005.SZ, 000006.SZ, 000007.SZ',
            'asset_type':            'E',
            'trade_batch_size':      100,
            'sell_batch_size':       100,
            'PT_buy_threshold':      0.05,
            'PT_sell_threshold':     0.05,
            'allow_sell_short':      False,
            'invest_start':          '2018-01-01',
            'opti_start':            '2018-01-01',
        }
        trader_kwargs = {
            'time_zone':             'local',
            'market_open_time_am':   '09:30:00',
            'market_close_time_pm':  '15:30:00',
            'market_open_time_pm':   '13:00:00',
            'market_close_time_am':  '11:30:00',
            'exchange':              'SSE',
            'cash_delivery_period':  0,
            'stock_delivery_period': 0,
            'asset_pool':            '000001.SZ, 000002.SZ, 000004.SZ, 000005.SZ, 000006.SZ, 000007.SZ',
            'asset_type':            'E',
            'trade_batch_size':      100,
            'sell_batch_size':       100,
            'pt_buy_threshold':      0.05,
            'pt_sell_threshold':     0.05,
            'allow_sell_short':      False,
        }
        # 使用专用测试数据源（非 QT_DATA_SOURCE），测试完成后清理
        data_test_dir = _get_cli_test_data_dir()
        os.makedirs(data_test_dir, exist_ok=True)
        test_ds = DataSource(
                'file',
                file_type='csv',
                file_loc=data_test_dir,
                allow_drop_table=True,
        )

        # 创建一个操作员
        operator = Operator(strategies=['macd', 'dma'], op_type='step')
        # 创建一个经纪商
        broker = SimulatorBroker(reject_submit_probability=0.0)

        test_ds.reconnect()

        # 创建测试datasource需要的股票基础数据等数据
        stock_basic = {
            'ts_code': ['000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ', '000007.SZ'],
            'symbol': ['000001', '000002', '000004', '000005', '000006', '000007'],
            'name': ['平安银行', '万科A', '国农科技', '世纪星源', '深振业A', '全新好'],
            'area': ['深圳', '深圳', '深圳', '深圳', '深圳', '深圳'],
            'industry': ['银行', '全国地产', '生物制药', '环境保护', '区域地产', '食品'],
            'full_name': ['平安银行股份有限公司', '万科企业股份有限公司', '国农科技股份有限公司', '世纪星源股份有限公司',
                          '深圳市振业(集团)股份有限公司', '深圳市全新好股份有限公司'],
            'enname': ['Ping An Bank Co., Ltd.', 'China Vanke Co., Ltd.',
                       'China National Agricultural Technology Co., Ltd.', 'Shijixingyuan Co., Ltd.',
                       'Shenzhen Zhenye(Group) Co., Ltd.', 'Shenzhen Quanxin Hao Co., Ltd.'],
            'cnspell': ['PAYH', 'WK', 'GNKJ', 'SJXY', 'SZYA', 'SXH'],
            'market': ['主板', '主板', '主板', '主板', '主板', '主板'],
            'exchange': ['SZSE', 'SZSE', 'SZSE', 'SZSE', 'SZSE', 'SZSE'],
            'curr_type': ['CNY', 'CNY', 'CNY', 'CNY', 'CNY', 'CNY'],
            'list_status': ['L', 'L', 'L', 'L', 'L', 'L'],
            'list_date': ['19910403', '19910129', '19951027', '19990602', '19921202', '19910809'],
            'delist_date': ['', '', '', '', '', ''],
            'is_hs': ['', '', '', '', '', ''],
        }
        stock_basic_df = pd.DataFrame(stock_basic)
        # 创建测试数据源的股票基础数据
        if not test_ds.table_data_exists('stock_basic'):
            test_ds.write_table_data(stock_basic_df, 'stock_basic')

        # 清空测试数据源中的所有相关表格数据
        for table in ['sys_op_live_accounts', 'sys_op_positions', 'sys_op_trade_orders', 'sys_op_trade_results']:
            if test_ds.table_data_exists(table):
                test_ds.drop_table_data(table)

        # 创建一个ID=1的账户
        new_account(user_name='test_user1', cash_amount=100000, data_source=test_ds)
        # 添加初始持仓
        get_or_create_position(account_id=1, symbol='000001.SZ', position_type='long', data_source=test_ds)
        get_or_create_position(account_id=1, symbol='000002.SZ', position_type='long', data_source=test_ds)
        get_or_create_position(account_id=1, symbol='000004.SZ', position_type='long', data_source=test_ds)
        get_or_create_position(account_id=1, symbol='000005.SZ', position_type='long', data_source=test_ds)
        update_position(position_id=1, data_source=test_ds, qty_change=200, available_qty_change=200, cost=10)
        update_position(position_id=2, data_source=test_ds, qty_change=200, available_qty_change=200, cost=10)
        update_position(position_id=3, data_source=test_ds, qty_change=300, available_qty_change=300, cost=10)
        update_position(position_id=4, data_source=test_ds, qty_change=200, available_qty_change=100, cost=10)

        self.ts = Trader(
                account_id=1,
                operator=operator,
                broker=broker,
                datasource=test_ds,
                debug=False,
                **trader_kwargs,
        )
        self.ts.debug = True
        self.ts.renew_trade_log_file()

        self.tss = TraderShell(self.ts)

    def tearDown(self):
        """测试结束后清理测试数据，不污染默认数据源。"""
        if getattr(self, 'ts', None) is not None:
            _clear_cli_test_tables(self.ts.datasource)

    def test_properties(self):

        tss = self.tss

        self.assertEqual(tss.trader, self.ts)
        self.assertEqual(tss.status, None)
        self.assertEqual(tss.watch_list, ['000300.SH',
                                          '000001.SZ',
                                          '000002.SZ',
                                          '000004.SZ',
                                          '000005.SZ',
                                          '000006.SZ',
                                          '000007.SZ'])

    def test_command_status(self):
        """ test status command """
        tss = self.tss

        print('testing status command that runs normally and returns None')
        self.assertIsNone(tss.do_status(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_status('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_status('wrong_argument'))

    def test_command_pause(self):
        """ test pause command"""
        tss = self.tss

        print('testing pause command that runs normally and returns None')
        self.assertIsNone(tss.do_pause(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_pause('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_pause('wrong_argument'))

    def test_command_resume(self):
        """ test resume command"""
        tss = self.tss

        print('testing resume command that runs normally and returns None')
        self.assertIsNone(tss.do_resume(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_resume('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_resume('wrong_argument'))

    def test_command_bye(self):
        """ test bye command"""
        tss = self.tss

        print('testing bye command that runs normally and returns True to exit the shell')
        self.assertTrue(tss.do_bye(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_bye('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_bye('wrong_argument'))

    def test_command_stop(self):
        """ test stop command"""
        tss = self.tss

        print('testing stop command that runs normally and returns True to exit the shell')
        self.assertTrue(tss.do_stop(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_stop('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_stop('wrong_argument'))

    def test_command_info(self):
        """ test info command"""
        tss = self.tss

        print('testing info command that runs normally and returns None')
        self.assertIsNone(tss.do_info(''))
        self.assertIsNone(tss.do_info('-d'))
        self.assertIsNone(tss.do_info('-s'))
        self.assertIsNone(tss.do_info('-d -s'))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_info('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_info('wrong_argument'))
        self.assertFalse(tss.do_info('-d -w wrong_optional_argument'))

    def test_command_exit(self):
        """ test exit command"""
        tss = self.tss

        print('testing exit command that runs normally and returns True to exit the shell')
        self.assertTrue(tss.do_exit(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_exit('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_exit('wrong_argument'))

    def test_request_shutdown_calls_trader_runtime_stop(self):
        """测试 shell 退出路径调用 Trader.stop(wait=False)。"""
        tss = self.tss
        print('\n[TestTraderCLI] _request_shutdown runtime stop call')
        with patch.object(tss.trader, 'stop') as mock_stop:
            tss._shutdown_requested = False
            tss._request_shutdown()
        print(' stop call args:', mock_stop.call_args)
        mock_stop.assert_called_once_with(wait=False, include_post_close=True)
        self.assertEqual(tss.status, 'stopped')

    def test_command_pool(self):
        """ test pool command"""
        tss = self.tss

        print('testing pool command that runs normally and returns None')
        self.assertIsNone(tss.do_pool(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_pool('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_pool('wrong_argument'))

    def test_command_watch(self):
        """ test watch command"""
        tss = self.tss

        print('testing watch command that runs normally and returns None')
        self.assertIsNone(tss.do_watch(''))

        print('testing add a new stock to watch list')
        self.assertIsNone(tss.do_watch('000002.SZ'))
        self.assertEqual(tss.watch_list, ['000001.SZ',
                                          '000002.SZ',
                                          '000004.SZ',
                                          '000005.SZ',
                                          '000006.SZ',
                                          '000007.SZ'])
        self.assertIsNone(tss.do_watch('000002.SZ'))
        self.assertEqual(tss.watch_list, ['000002.SZ',
                                          '000004.SZ',
                                          '000005.SZ',
                                          '000006.SZ',
                                          '000007.SZ'])

        print('testing remove a stock from watch list')
        self.assertIsNone(tss.do_watch('-r 000001.SH'))
        self.assertEqual(tss.watch_list, ['000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ', '000007.SZ'])
        self.assertIsNone(tss.do_watch('-r 000001.SH'))
        self.assertEqual(tss.watch_list, ['000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ', '000007.SZ'])

        print('testing remove all stocks from watch list')
        self.assertIsNone(tss.do_watch('-c'))
        self.assertEqual(tss.watch_list, [])

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_watch('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_watch('wrong_argument'))

    def test_command_buy(self):
        """ test buy command"""
        tss = self.tss
        # set live prices in trader for all assets for testing
        tss.trader.live_price = {
            '000001.SZ': 10.0,
            '000002.SZ': 20.0,
            '000004.SZ': 30.0,
            '000005.SZ': 40.0,
            '000006.SZ': 50.0,
            '000007.SZ': 60.0,
        }

        print('testing buy command that runs normally and returns None')
        self.assertIsNone(tss.do_buy('100 000001.SZ -p 10.0'))
        order = read_trade_order_detail(order_id=1, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000001.SZ')
        self.assertEqual(order['direction'], 'buy')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'limit')
        self.assertEqual(order['price'], 10.0)
        self.assertIsNone(tss.do_buy('100 000001.SZ -p 30 -s long'))
        order = read_trade_order_detail(order_id=2, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000001.SZ')
        self.assertEqual(order['direction'], 'buy')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'limit')
        self.assertEqual(order['price'], 30.0)
        print(f'testing buy command with no price given and use live price')
        print('trader live price is:', tss.trader.live_price)
        self.assertIsNone(tss.do_buy('100 000002.SZ'))
        order = read_trade_order_detail(order_id=3, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000002.SZ')
        self.assertEqual(order['direction'], 'buy')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'market')
        self.assertEqual(order['price'], 20.0)

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_buy('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_buy('no_qty 000001.SZ -p 10.0'))
        self.assertFalse(tss.do_buy('100 wrong_symbol -p 10.0'))
        self.assertFalse(tss.do_buy('100 000001.SZ -p 10.0 -s wrong_position'))
        self.assertFalse(tss.do_buy('100 000001.SZ -p -s long'))  # no price given
        self.assertFalse(tss.do_buy('-100 000001.SZ -p 10.0 -s long'))  # negative qty
        self.assertFalse(tss.do_buy('100 000001.SZ -p -10.0 -s long'))  # negative price
        self.assertFalse(tss.do_buy('100 000001.SZ -p 10.0 -s long -w wrong_argument'))
        self.assertFalse(tss.do_buy('11.2 000001.SZ -p 10.0 -s long'))  # qty not multiple of moq
        print(f'change moq to 0 and then test again')
        self.assertIsNone(tss.do_config('trade_batch_size -s 0.1'))
        self.assertIsNone(tss.do_buy('11.2 000001.SZ -p 10.0 -s long'))  # qty now accepted

    def test_command_sell(self):
        """ test sell command"""
        tss = self.tss
        # set live prices in trader for all assets for testing
        tss.trader.live_price = {
            '000001.SZ': 10.0,
            '000002.SZ': 20.0,
            '000004.SZ': 30.0,
            '000005.SZ': 40.0,
            '000006.SZ': 50.0,
            '000007.SZ': 60.0,
        }

        print('testing sell command that runs normally and returns None')
        self.assertIsNone(tss.do_sell('100 000001.SZ -p 10.0'))
        order = read_trade_order_detail(order_id=1, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000001.SZ')
        self.assertEqual(order['direction'], 'sell')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'limit')
        self.assertEqual(order['price'], 10.0)
        self.assertIsNone(tss.do_sell('100 000001.SZ -p 30 -s long'))
        order = read_trade_order_detail(order_id=2, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000001.SZ')
        self.assertEqual(order['direction'], 'sell')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'limit')
        self.assertEqual(order['price'], 30.0)
        print(f'testing sell command with no price given and use live price')
        print('trader live price is:', tss.trader.live_price)
        self.assertIsNone(tss.do_sell('100 000002.SZ'))
        order = read_trade_order_detail(order_id=3, data_source=tss.trader.datasource)
        self.assertEqual(order['account_id'], 1)
        self.assertEqual(order['position'], 'long')
        self.assertEqual(order['symbol'], '000002.SZ')
        self.assertEqual(order['direction'], 'sell')
        self.assertEqual(order['qty'], 100)
        self.assertEqual(order['order_type'], 'market')
        self.assertEqual(order['price'], 20.0)

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_sell('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_sell('no_qty 000001.SZ -p 10.0'))
        self.assertFalse(tss.do_sell('100 wrong_symbol -p 10.0'))
        self.assertFalse(tss.do_sell('100 000001.SZ -p 10.0 -s long'))
        self.assertFalse(tss.do_sell('100 000001.SZ -p -s long'))  # no price given
        self.assertFalse(tss.do_sell('-100 000001.SZ -p 10.0 -s long'))  # negative qty
        self.assertFalse(tss.do_sell('100 000001.SZ -p -10.0 -s long'))  # negative price

    def test_command_buy_prints_risk_summary_on_reject(self):
        """测试 buy 命令在风控拒单时输出摘要。"""
        tss = self.tss
        tss.trader.risk_manager = RiskManager((MaxOrderQtyRule('mx', 5.0),))
        tss.trader.live_price = {'000001.SZ': 10.0}
        before_count = len(query_trade_orders(tss.trader.account_id, data_source=tss.trader.datasource))
        capture = io.StringIO()
        with redirect_stdout(capture):
            self.assertIsNone(tss.do_buy('100 000001.SZ -p 10.0'))
        out = capture.getvalue()
        after_count = len(query_trade_orders(tss.trader.account_id, data_source=tss.trader.datasource))
        print('\n[TestTraderCLI] risk reject stdout:\n', out)
        print(' order count before/after:', before_count, after_count)
        self.assertIn('Order submission rejected by risk manager', out)
        self.assertIn('rule_id=', out)
        self.assertIn('reason=', out)
        self.assertEqual(before_count, after_count)

    def test_command_positions(self):
        """ test positions command"""
        tss = self.tss

        print('testing positions command that runs normally and returns None')
        self.assertIsNone(tss.do_positions(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_positions('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_positions('wrong_argument'))

    def test_command_overview(self):
        """ test overview command"""
        tss = self.tss

        print('testing overview command that runs normally and returns None')
        self.assertIsNone(tss.do_overview(''))
        self.assertIsNone(tss.do_overview('-d'))
        self.assertIsNone(tss.do_overview('-s'))
        self.assertIsNone(tss.do_overview('-d -s'))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_overview('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_overview('wrong_argument'))
        self.assertFalse(tss.do_overview('-d -w wrong_optional_argument'))

    def test_command_config(self):
        """ test config command"""
        tss = self.tss

        print(f'testing running with no arguments and print out configs up to level 2')
        self.assertIsNone(tss.do_config(''))
        print(f'testing running with -l 3 and print out configs up to level 3')
        self.assertIsNone(tss.do_config('-lll'))
        print(f'testing running with one key given and print out the value of the key')
        self.assertIsNone(tss.do_config('mode'))
        self.assertIsNone(tss.do_config('time_zone'))
        print(f'testing running with multiple keys given')
        self.assertIsNone(tss.do_config('mode time_zone'))
        print(f'testing running with multiple keys given with details')
        self.assertIsNone(tss.do_config('mode time_zone -d'))
        print(f'testing running with user defined keys')
        self.assertIsNone(tss.do_config('user_defined_key'))
        self.assertIsNone(tss.do_config('user_defined_key -d'))
        print(f'testing running with values to set to config key')
        self.assertEqual(tss.trader.config['sell_batch_size'], 100)
        self.assertIsNone(tss.do_config('sell_batch_size -s 1'))
        self.assertEqual(tss.trader.config['sell_batch_size'], 1)
        self.assertEqual(tss.trader.config['time_zone'], 'local')
        self.assertIsNone(tss.do_config('sell_batch_size time_zone -s 0 Asia/Shanghai'))
        self.assertEqual(tss.trader.config['sell_batch_size'], 1.0)
        self.assertEqual(tss.trader.config['time_zone'], 'Asia/Shanghai')
        self.assertIsNone(tss.do_config('sell_batch_size time_zone -s -5 Asia/Hong_Kong'))
        self.assertEqual(tss.trader.config['sell_batch_size'], 1.0)
        self.assertEqual(tss.trader.config['time_zone'], 'Asia/Hong_Kong')

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_config('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_config('user_defined_key -d positional_arg_in_wrong_place'))
        self.assertFalse(tss.do_config('--wrong_optional_arg'))
        self.assertFalse(tss.do_config('-w'))
        self.assertFalse(tss.do_config('argument -l 2'))
        self.assertFalse(tss.do_config('argument -l -s value_1 too_many_set_values'))
        self.assertFalse(tss.do_config('argument too_many_args -s too_few_set_value'))

    def test_command_history(self):
        """ test history command"""
        tss = self.tss
        test_ds = tss.trader.datasource

        # 添加测试交易订单以及交易结果
        print('Adding test trade orders and results...')
        self.stoppage = 0.1
        parsed_signals_batch = (
            ['000001.SZ', '000002.SZ', '000004.SZ', '000006.SZ', '000007.SZ', ],
            ['long', 'long', 'long', 'long', 'long'],
            ['buy', 'sell', 'sell', 'buy', 'buy'],
            [100, 100, 300, 400, 500],
            [60.0, 70.0, 80.0, 90.0, 100.0],
        )
        # save first batch of signals
        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=parsed_signals_batch[0],
                positions=parsed_signals_batch[1],
                directions=parsed_signals_batch[2],
                quantities=parsed_signals_batch[3],
                prices=parsed_signals_batch[4],
                data_source=test_ds,
        )
        # submit orders
        for order_id in order_ids:
            submit_order(order_id, test_ds)

        parsed_signals_batch = (
            ['000001.SZ', '000004.SZ', '000005.SZ', '000007.SZ', ],
            ['long', 'long', 'long', 'long'],
            ['sell', 'buy', 'buy', 'sell'],
            [200, 200, 100, 300],
            [70.0, 30.0, 56.0, 79.0],
        )
        # save first batch of signals
        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=parsed_signals_batch[0],
                positions=parsed_signals_batch[1],
                directions=parsed_signals_batch[2],
                quantities=parsed_signals_batch[3],
                prices=parsed_signals_batch[4],
                data_source=test_ds,
        )
        # submit orders
        for order_id in order_ids:
            submit_order(order_id, test_ds)

        # 添加交易订单执行结果
        delivery_config = {
            'cash_delivery_period':  0,
            'stock_delivery_period': 0,
        }
        raw_trade_result = {
            'order_id':        1,
            'filled_qty':      100,
            'price':           60.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        2,
            'filled_qty':      100,
            'price':           70.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        3,
            'filled_qty':      200,
            'price':           80.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        4,
            'filled_qty':      400,
            'price':           89.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        5,
            'filled_qty':      500,
            'price':           100.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        3,
            'filled_qty':      100,
            'price':           78.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        6,
            'filled_qty':      200,
            'price':           69.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        7,
            'filled_qty':      200,
            'price':           31.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        time.sleep(self.stoppage)
        raw_trade_result = {
            'order_id':        9,
            'filled_qty':      300,
            'price':           91.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result=raw_trade_result, data_source=test_ds)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)
        # order 8 is canceled
        time.sleep(self.stoppage)
        process_account_delivery(account_id=1, data_source=test_ds, **delivery_config)

        print('testing history command that runs normally and returns None')
        self.assertIsNone(tss.do_history(''))
        self.assertIsNone(tss.do_history('000001.SZ'))
        self.assertIsNone(tss.do_history('000002.SZ'))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_history('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_history('wrong_argument'))
        self.assertFalse(tss.do_history('000001.SZ -w wrong_optional_argument'))

    def test_command_orders(self):
        """ test orders command"""
        tss = self.tss

        # add testing orders to test data source
        print('Adding test trade orders and results...')
        self.stoppage = 0.1
        parsed_signals_batch = (
            ['000001.SZ', '000002.SZ', '000004.SZ', '000006.SZ', '000007.SZ', ],
            ['long', 'long', 'long', 'long', 'long'],
            ['buy', 'sell', 'sell', 'buy', 'buy'],
            [100, 100, 300, 400, 500],
            [60.0, 70.0, 80.0, 90.0, 100.0],
        )
        # save first batch of signals
        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=parsed_signals_batch[0],
                positions=parsed_signals_batch[1],
                directions=parsed_signals_batch[2],
                quantities=parsed_signals_batch[3],
                prices=parsed_signals_batch[4],
                data_source=tss.trader.datasource,
        )
        # submit orders
        for order_id in order_ids:
            submit_order(order_id, tss.trader.datasource)

        # create order results
        delivery_config = {
            'cash_delivery_period':  0,
            'stock_delivery_period': 0,
        }
        # order 1 is filled
        raw_trade_result = {
            'order_id':        1,
            'filled_qty':      100,
            'price':           60.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result, tss.trader.datasource)
        process_account_delivery(account_id=1, data_source=tss.trader.datasource, **delivery_config)
        time.sleep(self.stoppage)
        # order 2 is filled
        raw_trade_result = {
            'order_id':        2,
            'filled_qty':      100,
            'price':           70.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result, tss.trader.datasource)
        process_account_delivery(account_id=1, data_source=tss.trader.datasource, **delivery_config)
        time.sleep(self.stoppage)
        # order 3 is partially-filled
        raw_trade_result = {
            'order_id':        3,
            'filled_qty':      200,
            'price':           80.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result, tss.trader.datasource)
        process_account_delivery(account_id=1, data_source=tss.trader.datasource, **delivery_config)
        time.sleep(self.stoppage)
        # order 4 is partially-filled
        raw_trade_result = {
            'order_id':        4,
            'filled_qty':      200,
            'price':           89.5,
            'transaction_fee': 5.0,
            'canceled_qty':    0.0,
        }
        process_trade_result(raw_trade_result, tss.trader.datasource)
        process_account_delivery(account_id=1, data_source=tss.trader.datasource, **delivery_config)
        time.sleep(self.stoppage)
        # order 5 is canceled
        raw_trade_result = {
            'order_id':        5,
            'filled_qty':      0,
            'price':           0.0,
            'transaction_fee': 0.0,
            'canceled_qty':    500.0,
        }
        process_trade_result(raw_trade_result, tss.trader.datasource)
        time.sleep(self.stoppage)

        print('testing orders command that runs normally and returns None')
        print(f'\nprint all orders')
        self.assertIsNone(tss.do_orders(''))
        print(f'\nprint orders of 000001.SZ')
        self.assertIsNone(tss.do_orders('000001'))
        print(f'\nprint orders that are filled')
        self.assertIsNone(tss.do_orders('--status filled'))
        print(f'\nprint orders that are canceled')
        self.assertIsNone(tss.do_orders('-s canceled'))
        print(f'\nprint orders that are executed today')
        self.assertIsNone(tss.do_orders('--time today'))
        print(f'\nprint orders that are executed yesterday')
        self.assertIsNone(tss.do_orders('-t yesterday'))
        print(f'\nprint orders that are buy orders')
        self.assertIsNone(tss.do_orders('--type buy'))
        print(f'\nprint orders that are sell orders')
        self.assertIsNone(tss.do_orders('-y sell'))
        print(f'\nprint orders that are on the long side')
        self.assertIsNone(tss.do_orders('--side long'))
        print(f'\nprint orders that are on the short side')
        self.assertIsNone(tss.do_orders('-d short'))
        print(f'\nprint buy long orders of 000001.SZ that are filled today')
        self.assertIsNone(tss.do_orders('000001 --status filled -t today --type buy -d long'))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_orders('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_orders('wrong_argument'))
        self.assertFalse(tss.do_orders('000001 -w wrong_optional_argument'))
        self.assertFalse(tss.do_orders('000001 -t wrong_optional_argument'))

    def test_command_orders_supports_submitted_active_and_nat_format(self):
        """orders 支持 submitted/active 过滤，且 NaT 时统一格式输出。"""
        tss = self.tss
        print('\n[TestTraderCLI] orders --status submitted / --active / NaT format')

        parsed_signals_batch = (
            ['000001.SZ'],
            ['long'],
            ['buy'],
            [100],
            [10.0],
        )
        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=parsed_signals_batch[0],
                positions=parsed_signals_batch[1],
                directions=parsed_signals_batch[2],
                quantities=parsed_signals_batch[3],
                prices=parsed_signals_batch[4],
                data_source=tss.trader.datasource,
        )
        print(' order_ids:', order_ids)
        submit_order(order_ids[0], tss.trader.datasource)

        self.assertIsNone(tss.do_orders('--status submitted'))
        self.assertIsNone(tss.do_orders('--active'))

        fake_df = tss.trader.history_orders().copy()
        fake_df.loc[fake_df.index[0], 'execution_time'] = pd.NaT
        capture = io.StringIO()
        with patch.object(tss.trader, 'history_orders', return_value=fake_df):
            with redirect_stdout(capture):
                self.assertIsNone(tss.do_orders(''))
        out = capture.getvalue()
        print(' orders output with NaT:\n', out)
        self.assertIn('--', out)

    def test_command_cancel_success_and_failure(self):
        """cancel 命令成功与失败路径。"""
        tss = self.tss
        print('\n[TestTraderCLI] cancel command success/failure')

        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=['000001.SZ'],
                positions=['long'],
                directions=['buy'],
                quantities=[100],
                prices=[10.0],
                data_source=tss.trader.datasource,
        )
        order_id = order_ids[0]
        submit_order(order_id, tss.trader.datasource)
        detail_before = read_trade_order_detail(order_id=order_id, data_source=tss.trader.datasource)
        print(' before cancel:', detail_before)
        self.assertEqual(detail_before['status'], 'submitted')

        self.assertIsNone(tss.do_cancel(str(order_id)))
        detail_after = read_trade_order_detail(order_id=order_id, data_source=tss.trader.datasource)
        print(' after cancel:', detail_after)
        self.assertEqual(detail_after['status'], 'canceled')

        self.assertFalse(tss.do_cancel(str(order_id)))
        self.assertFalse(tss.do_cancel('999999'))

    def test_command_cancel_rejects_other_account_order(self):
        """cancel 命令不能撤销其它账户订单。"""
        tss = self.tss
        print('\n[TestTraderCLI] cancel should reject other-account order')
        account_id_2 = new_account(user_name='test_user2', cash_amount=100000, data_source=tss.trader.datasource)
        print(' created account_id_2:', account_id_2)
        get_or_create_position(
                account_id=account_id_2,
                symbol='000001.SZ',
                position_type='long',
                data_source=tss.trader.datasource,
        )
        order_ids = save_parsed_trade_orders(
                account_id=account_id_2,
                symbols=['000001.SZ'],
                positions=['long'],
                directions=['buy'],
                quantities=[100],
                prices=[10.0],
                data_source=tss.trader.datasource,
        )
        order_id = int(order_ids[0])
        submit_order(order_id, tss.trader.datasource)
        detail_before = read_trade_order_detail(order_id=order_id, data_source=tss.trader.datasource)
        print(' detail_before:', detail_before)
        self.assertEqual(detail_before['status'], 'submitted')
        self.assertEqual(detail_before['account_id'], account_id_2)

        cancel_ok = tss.do_cancel(str(order_id))
        detail_after = read_trade_order_detail(order_id=order_id, data_source=tss.trader.datasource)
        print(' cancel_ok:', cancel_ok)
        print(' detail_after:', detail_after)
        self.assertFalse(cancel_ok)
        self.assertEqual(detail_after['status'], 'submitted')

    def test_submit_rejected_order_status_is_rejected(self):
        """Broker 拒单后，订单状态应落为 rejected。"""
        tss = self.tss
        print('\n[TestTraderCLI] rejected submit should persist order status as rejected')
        before_orders = query_trade_orders(account_id=1, data_source=tss.trader.datasource)
        before_ids = set(before_orders.index.tolist())
        print(' before_ids:', sorted(before_ids))
        tss.trader.live_price = pd.Series({'000001.SZ': 10.0})
        tss.trader.broker._reject_submit_probability = 1.0
        try:
            self.assertIsNone(tss.do_buy('100 000001.SZ'))
        finally:
            tss.trader.broker._reject_submit_probability = 0.0
        after_orders = query_trade_orders(account_id=1, data_source=tss.trader.datasource)
        after_ids = set(after_orders.index.tolist())
        new_ids = sorted(after_ids - before_ids)
        print(' after_ids:', sorted(after_ids))
        print(' new_ids:', new_ids)
        self.assertTrue(new_ids)
        rejected_order_id = int(new_ids[-1])
        rejected_detail = read_trade_order_detail(order_id=rejected_order_id, data_source=tss.trader.datasource)
        print(' rejected_detail:', rejected_detail)
        self.assertEqual(rejected_detail['status'], 'rejected')

    def test_command_buy_order_type_market_vs_limit(self):
        """buy 命令应按是否显式 -p 区分 market/limit。"""
        tss = self.tss
        print('\n[TestTraderCLI] buy order_type should split by explicit price argument')
        tss.trader.live_price = pd.Series({'000001.SZ': 10.0})

        before_orders = query_trade_orders(account_id=1, data_source=tss.trader.datasource)
        before_ids = set(before_orders.index.tolist())
        print(' before_ids:', sorted(before_ids))

        self.assertIsNone(tss.do_buy('100 000001.SZ -p 11.2'))
        self.assertIsNone(tss.do_buy('100 000001.SZ'))

        after_orders = query_trade_orders(account_id=1, data_source=tss.trader.datasource)
        new_orders = after_orders.loc[~after_orders.index.isin(before_ids)].sort_index()
        print(' new_orders:\n', new_orders[['order_type', 'price', 'status']])
        self.assertEqual(len(new_orders), 2)
        self.assertEqual(new_orders.iloc[0]['order_type'], 'limit')
        self.assertEqual(new_orders.iloc[1]['order_type'], 'market')

    def test_command_change(self):
        """ test change command"""
        tss = self.tss

        print('testing change command that runs normally and returns None')
        print('testing change quantity without price')
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 200)
        self.assertEqual(position['available_qty'], 200)
        self.assertEqual(position['cost'], 10)
        self.assertIsNone(tss.do_change('000001 --amount 100'))
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 300)
        self.assertEqual(position['available_qty'], 300)
        self.assertEqual(position['cost'], 10)
        self.assertIsNone(tss.do_change('000001 -a -100'))
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 200)
        self.assertEqual(position['available_qty'], 200)
        self.assertEqual(position['cost'], 10)
        print('testing reducing quantity exceeding holding qty')
        self.assertIsNone(tss.do_change('000001 -a -300'))  # reduce 300 out of 200 will leads to no change
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 200)
        self.assertEqual(position['available_qty'], 200)
        self.assertEqual(position['cost'], 10)
        print('testing add short side quantity')
        self.assertIsNone(tss.do_change('000001 -a 300 -s short'))  # 已经拥有000001多头仓位的同时不能拥有空头仓位
        with self.assertRaises(RuntimeError):
            get_position_by_id(5, tss.trader.datasource)
        # 必须首先将多头仓位将为0后才能添加空头仓位
        self.assertIsNone(tss.do_change('000001 -a -200'))
        self.assertIsNone(tss.do_change('000001 -a 200 -s short'))
        position_1 = get_position_by_id(1, tss.trader.datasource)
        position_5 = get_position_by_id(5, tss.trader.datasource)
        # 多头仓位已经为0，空头仓位200
        self.assertEqual(position_1['symbol'], '000001.SZ')
        self.assertEqual(position_1['position'], 'long')
        self.assertEqual(position_1['qty'], 0)
        self.assertEqual(position_1['available_qty'], 0)
        self.assertEqual(position_1['cost'], 0)

        self.assertEqual(position_5['symbol'], '000001.SZ')
        self.assertEqual(position_5['position'], 'short')
        self.assertEqual(position_5['qty'], 200)
        self.assertEqual(position_5['available_qty'], 200)
        self.assertEqual(position_5['cost'], 10)
        # clear short position of 000001 again
        self.assertIsNone(tss.do_change('000001 -a -200 -s short'))

        print('testing change quantity with price')
        self.assertIsNone(tss.do_change('000001 --amount 200 --price 10'))
        self.assertIsNone(tss.do_change('000001 --amount 200 --price 20'))
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 400)
        self.assertEqual(position['available_qty'], 400)
        self.assertEqual(position['cost'], 15)
        self.assertIsNone(tss.do_change('000001 -a -200 -p 10'))
        position = get_position_by_id(1, tss.trader.datasource)
        self.assertEqual(position['qty'], 200)
        self.assertEqual(position['available_qty'], 200)
        self.assertEqual(position['cost'], 20)
        print(f'testing change cash and available cashes')
        account = get_account(1, data_source=tss.trader.datasource)

        self.assertEqual(account['cash_amount'], 100000)
        self.assertEqual(account['available_cash'], 100000)
        self.assertEqual(account['total_invest'], 100000)
        self.assertIsNone(tss.do_change('--cash 10000'))
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(account['cash_amount'], 110000)
        self.assertEqual(account['available_cash'], 110000)
        self.assertEqual(account['total_invest'], 110000)
        self.assertIsNone(tss.do_change('-c -10000'))
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(account['cash_amount'], 100000)
        self.assertEqual(account['available_cash'], 100000)
        self.assertEqual(account['total_invest'], 100000)
        print('testing reducing cash amount exceeding on hand cash')
        self.assertIsNone(tss.do_change('-c -300000'))  # reducing 300k out of 100k will change nothing
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(account['cash_amount'], 100000)
        self.assertEqual(account['available_cash'], 100000)
        self.assertEqual(account['total_invest'], 100000)

        print(f'testing change cash and position quantities in the same time')
        position = get_position_by_id(2, tss.trader.datasource)
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(position['qty'], 200)
        self.assertEqual(position['available_qty'], 200)
        self.assertEqual(position['cost'], 10)
        self.assertEqual(account['cash_amount'], 100000)
        self.assertEqual(account['available_cash'], 100000)
        self.assertEqual(account['total_invest'], 100000)
        self.assertIsNone(tss.do_change('000002 --amount 300 --cash 10000 --price 20'))
        position = get_position_by_id(2, tss.trader.datasource)
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(position['qty'], 500)
        self.assertEqual(position['available_qty'], 500)
        self.assertEqual(position['cost'], 16)
        self.assertEqual(account['cash_amount'], 110000)
        self.assertEqual(account['available_cash'], 110000)
        self.assertEqual(account['total_invest'], 110000)
        self.assertIsNone(tss.do_change('000002 -a -200 -c -10000 -p 10'))
        position = get_position_by_id(2, tss.trader.datasource)
        account = get_account(1, data_source=tss.trader.datasource)
        self.assertEqual(position['qty'], 300)
        self.assertEqual(position['available_qty'], 300)
        self.assertEqual(position['cost'], 20)
        self.assertEqual(account['cash_amount'], 100000)
        self.assertEqual(account['available_cash'], 100000)
        self.assertEqual(account['total_invest'], 100000)

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_change('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_change('wrong_argument'))
        self.assertFalse(tss.do_change('000001 -w wrong_optional_argument'))
        self.assertFalse(tss.do_change('000001 -t wrong_optional_argument'))
        self.assertFalse(tss.do_change('-a 100'))  # no symbol given
        self.assertFalse(tss.do_change('-p 100'))  # only price is given
        self.assertFalse(tss.do_change('-a 100 -p 100'))  # no symbol given
        self.assertFalse(tss.do_change('000100 -a 100'))  # symbol not in pool
        self.assertFalse(tss.do_change('000001 -a -100'))  # negative quantity
        self.assertFalse(tss.do_change('000001 -a not_a_number'))
        self.assertFalse(tss.do_change('000001 -a 100 -p not_a_number'))
        self.assertFalse(tss.do_change('000001 -a 100 -p -10'))  # negative price
        self.assertFalse(tss.do_change('000001 -a 100 -p 10 -c not_a_number'))
        self.assertFalse(tss.do_change('000001 -a 100 -p 10 -c -10'))  # negative cash

    def test_command_dashboard(self):
        """ test dashboard command"""
        tss = self.tss

        print('testing dashboard command that runs normally and returns True to exit the shell')
        self.assertTrue(tss.do_dashboard(''))
        self.assertTrue(tss.do_dashboard('--rewind 20'))
        self.assertTrue(tss.do_dashboard('-r 20'))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_dashboard('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_dashboard('wrong_argument'))
        self.assertFalse(tss.do_dashboard('-r not_an_int'))
        self.assertFalse(tss.do_dashboard('-r -10'))  # negative number
        self.assertFalse(tss.do_dashboard('-r 10_000'))  # too large number
        self.assertFalse(tss.do_dashboard('-w wrong_optional_argument'))

    def test_command_strategies(self):
        """ test strategies command"""
        tss = self.tss

        print('testing strategies command that runs normally and returns None')

        print(f'\n-----------------------\n'
              f'testing operator info get without detail')
        self.assertIsNone(tss.do_strategies(''))
        print(f'\n-----------------------\n'
              f'testing operator info get with detail')
        self.assertIsNone(tss.do_strategies('-d'))

        print(f'\n-----------------------\n'
              f'testing strategies info get by id without detail')
        self.assertIsNone(tss.do_strategies('dma'))
        self.assertIsNone(tss.do_strategies('macd'))

        print(f'\n-----------------------\n'
              f'testing strategies info get two ids without detail')
        self.assertIsNone(tss.do_strategies('macd dma'))

        print(f'\n-----------------------\n'
              f'testing strategies info get by id with detail')
        self.assertIsNone(tss.do_strategies('dma -d'))
        self.assertIsNone(tss.do_strategies('macd -d'))

        print(f'\n-----------------------\n'
              f'testing strategies info get two ids with detail')
        self.assertIsNone(tss.do_strategies('macd dma -d'))

        print('\ntesting setting pars to one and two strategies')
        self.assertEqual(tss.trader.operator['macd'].par_values, (12, 26, 9))
        self.assertIsNone(tss.do_strategies('macd -s 35 25 55'))
        self.assertEqual(tss.trader.operator['macd'].par_values, (35, 25, 55))
        self.assertEqual(tss.trader.operator['dma'].par_values, (12, 26, 9))
        self.assertIsNone(tss.do_strategies('dma -s 35 25 55'))
        self.assertEqual(tss.trader.operator['dma'].par_values, (35, 25, 55))
        self.assertIsNone(tss.do_strategies('dma macd -s 40 41 42 -s 43 44 45'))
        self.assertEqual(tss.trader.operator['dma'].par_values, (40, 41, 42))
        self.assertEqual(tss.trader.operator['macd'].par_values, (43, 44, 45))

        print('\ntesting setting blender to timing')
        self.assertIsNone(tss.do_strategies('-b s0*s1 -g Group_1'))

        print(f'\ntesting getting help and returns False')
        self.assertFalse(tss.do_strategies('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_strategies('wrong_strategy'))
        self.assertFalse(tss.do_strategies('-w wrong_optional_argument'))
        self.assertFalse(tss.do_strategies('dma -s wrong par types'))
        self.assertFalse(tss.do_strategies('dma -s 1 2 3'))  # out of range pars
        self.assertFalse(tss.do_strategies('dma -s 44 44 44 44'))  # too many pars
        self.assertFalse(tss.do_strategies('dma macd -s 44 44 44'))  # value not match strategy
        self.assertFalse(tss.do_strategies('-d blender -s 44 44 44'))  # blender without group
        self.assertFalse(tss.do_strategies('-d blender -g wrong_group'))
        self.assertFalse(tss.do_strategies('-d wrong_blender -g wrong_group'))

    def test_command_schedule(self):
        """ test schedule command"""
        tss = self.tss

        print('testing schedule command that runs normally and returns None')
        self.assertIsNone(tss.do_schedule(''))

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_schedule('-h'))

        print('testing schedule command with wrong arguments and returns False')
        self.assertFalse(tss.do_schedule('wrong_argument'))

    def test_command_run(self):
        """ test run command"""
        tss = self.tss

        print('testing run command that runs normally and returns None')
        self.assertIsNone(tss.do_run('dma'))
        self.assertIsNone(tss.do_run('macd'))
        self.assertIsNone(tss.do_run('dma macd'))
        print('testing run task pre_open with patched _run_task to avoid external data channel dependency')
        with patch.object(tss.trader, '_run_task', return_value=None) as mock_run_task:
            self.assertIsNone(tss.do_run('--task pre_open'))
            mock_run_task.assert_called_with('pre_open', run_in_main_thread=True)
        print('testing run task diagnose_pending_orders with patched _run_task')
        with patch.object(tss.trader, '_run_task', return_value=None) as mock_run_task:
            self.assertIsNone(tss.do_run('--task diagnose_pending_orders'))
            mock_run_task.assert_called_with('diagnose_pending_orders', run_in_main_thread=True)
        print('testing run task open_market / close_market with patched _run_task')
        with patch.object(tss.trader, '_run_task', return_value=None) as mock_run_task:
            self.assertIsNone(tss.do_run('--task open_market'))
            mock_run_task.assert_called_with('open_market', run_in_main_thread=True)
        with patch.object(tss.trader, '_run_task', return_value=None) as mock_run_task:
            self.assertIsNone(tss.do_run('--task close_market'))
            mock_run_task.assert_called_with('close_market', run_in_main_thread=True)
        print('DEBUG_RUN_TASK_CHOICES:', DEBUG_RUN_TASK_CHOICES)
        self.assertIn('open_market', DEBUG_RUN_TASK_CHOICES)
        self.assertIn('close_market', DEBUG_RUN_TASK_CHOICES)

        print(f'testing getting help and returns False')
        self.assertFalse(tss.do_run('-h'))

        print(f'testing run command with wrong arguments and returns False')
        self.assertFalse(tss.do_run(''))
        self.assertFalse(tss.do_run('wrong_argument'))
        self.assertFalse(tss.do_run('--task not_a_valid_task'))

    def test_command_artifacts(self):
        """test artifacts command and ls-artifacts alias"""
        tss = self.tss
        ds = self.ts.datasource
        aid = self.ts.account_id

        print('\n[TestCommandArtifacts] help returns False')
        self.assertFalse(tss.do_artifacts('-h'))

        print('[TestCommandArtifacts] artifacts paths vs helpers')
        expected = list_live_trade_artifacts(aid, data_source=ds)
        print(' expected artifacts:', expected)
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_artifacts(''))
        out = buf.getvalue()
        print(' cli output:', out)
        self.assertIn('sys_log:', out)
        self.assertIn('trade_log:', out)
        self.assertIn('break_point:', out)
        self.assertIn('risk_log:', out)
        self.assertIn('.risk.log', out)
        self.assertIn(sys_log_file_path_name(aid, ds), out)
        self.assertIn(trade_log_file_path_name(aid, ds), out)
        self.assertIn(break_point_file_path_name(aid, ds), out)
        self.assertIn(risk_log_file_path_name(aid, ds), out)

        print('[TestCommandArtifacts] ls-artifacts alias via precmd')
        self.assertEqual(tss.precmd('ls-artifacts'), 'artifacts')

    def test_command_liveconfig(self):
        """test liveconfig command and live-config alias"""
        tss = self.tss

        print('\n[TestCommandLiveconfig] help returns False')
        self.assertFalse(tss.do_liveconfig('-h'))

        print('[TestCommandLiveconfig] human-readable summary')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_liveconfig(''))
        out = buf.getvalue()
        print(' summary output:', out)
        self.assertIn('Live Trade Config - Broker', out)
        self.assertIn('Broker Type', out)
        self.assertIn('simulator', out)
        self.assertNotIn('"broker_type"', out)

        print('[TestCommandLiveconfig] live-config alias via precmd')
        self.assertEqual(tss.precmd('live-config --detail'), 'liveconfig --detail')

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_liveconfig('--detail'))
        detail_out = buf.getvalue()
        print(' detail output:', detail_out)
        self.assertIn('Live Trade Config - Extended', detail_out)
        self.assertIn('Startup Gate Mode', detail_out)

    def test_command_tasks(self):
        """test tasks list and task show/cancel"""
        tss = self.tss

        print('\n[TestCommandTasks] empty queue table')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_tasks(''))
        out = buf.getvalue()
        print(' tasks output:', out)
        self.assertIn('Trader Tasks:', out)
        self.assertIn('No trader tasks found.', out)

        print('[TestCommandTasks] add task then list/show/cancel')
        task_id = tss.trader.add_task('refill', 'stock_daily')
        print(' task_id:', task_id)
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_tasks(''))
        out = buf.getvalue()
        print(' tasks after add:', out)
        self.assertIn('Trader Tasks:', out)
        self.assertIn(task_id, out)
        self.assertIn('refill', out)
        self.assertIn('status', out)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_task(task_id))
        task_json = json.loads(buf.getvalue())
        print(' task detail:', task_json)
        self.assertEqual(task_json['task_id'], task_id)
        self.assertEqual(task_json['name'], 'refill')

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_task(f'--cancel {task_id}'))
        cancel_out = buf.getvalue()
        print(' cancel output:', cancel_out)
        self.assertIn(f'Canceled Trader queue task: {task_id}', cancel_out)

        self.assertFalse(tss.do_task('missing-task-id'))

    def test_command_gate(self):
        """test gate command and startup-gate alias"""
        tss = self.tss

        print('\n[TestCommandGate] help returns False')
        self.assertFalse(tss.do_gate('-h'))

        print('[TestCommandGate] patched run_startup_gate True/False')
        with patch.object(tss.trader, 'run_startup_gate', return_value=True):
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertIsNone(tss.do_gate(''))
            out = buf.getvalue()
            print(' gate allowed output:', out)
            self.assertIn('Startup Gate', out)
            self.assertIn('Allowed', out)
            self.assertIn('True', out)

        with patch.object(tss.trader, 'run_startup_gate', return_value=False):
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertIsNone(tss.do_gate(''))
            out = buf.getvalue()
            print(' gate blocked output:', out)
            self.assertIn('Startup Gate', out)
            self.assertIn('False', out)

        print('[TestCommandGate] startup-gate alias via precmd')
        self.assertEqual(tss.precmd('startup-gate'), 'gate')

    def test_command_reconcile(self):
        """test reconcile command prints broker snapshot JSON"""
        tss = self.tss

        print('\n[TestCommandReconcile] help returns False')
        self.assertFalse(tss.do_reconcile('-h'))

        print('[TestCommandReconcile] snapshot on SimulatorBroker')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_reconcile(''))
        out = buf.getvalue()
        print(' reconcile output:', out)
        self.assertIn('Broker Reconcile', out)
        self.assertIn('Overall OK', out)
        self.assertIn('Remote Orders Count', out)
        self.assertNotIn('"is_ok"', out)

        print('[TestCommandReconcile] snapshot-reconcile alias via precmd')
        self.assertEqual(tss.precmd('snapshot-reconcile'), 'reconcile')

    def test_cli_command_aliases_defined(self):
        """Build 1 CLI 覆盖矩阵：别名与 DEBUG 任务白名单常量存在"""
        print('\n[TestCliCoverage] CLI_COMMAND_ALIASES:', CLI_COMMAND_ALIASES)
        self.assertEqual(CLI_COMMAND_ALIASES.get('ls-artifacts'), 'artifacts')
        self.assertEqual(CLI_COMMAND_ALIASES.get('live-config'), 'liveconfig')
        self.assertEqual(CLI_COMMAND_ALIASES.get('startup-gate'), 'gate')
        self.assertEqual(CLI_COMMAND_ALIASES.get('snapshot-reconcile'), 'reconcile')
        self.assertEqual(CLI_COMMAND_ALIASES.get('rotate-logs'), 'rotatelogs')
        self.assertEqual(CLI_COMMAND_ALIASES.get('rotatelog'), 'rotatelogs')
        self.assertEqual(CLI_COMMAND_ALIASES.get('pull-state'), 'sync')
        self.assertEqual(len(DEBUG_RUN_TASK_CHOICES), 7)

    def test_command_rotatelogs(self):
        """test rotatelogs command and rotate-logs alias"""
        tss = self.tss

        print('\n[TestCommandRotatelogs] help returns False')
        self.assertFalse(tss.do_rotatelogs('-h'))

        print('[TestCommandRotatelogs] rotate-logs alias via precmd')
        self.assertEqual(tss.precmd('rotate-logs --days 30'), 'rotatelogs --days 30')

        tmp_dir = tempfile.mkdtemp()
        original_path = qt.QT_TRADE_LOG_PATH
        try:
            old_path = os.path.join(tmp_dir, 'old_account.risk.log')
            recent_path = os.path.join(tmp_dir, 'recent_account.risk.log')
            with open(old_path, 'w', encoding='utf-8') as f:
                f.write('old-risk\n')
            with open(recent_path, 'w', encoding='utf-8') as f:
                f.write('recent-risk\n')
            old_time = time.time() - 40 * 24 * 3600
            recent_time = time.time() - 2 * 24 * 3600
            os.utime(old_path, (old_time, old_time))
            os.utime(recent_path, (recent_time, recent_time))

            print(' tmp_dir:', tmp_dir)
            print(' files before rotation:', sorted(os.listdir(tmp_dir)))
            qt.QT_TRADE_LOG_PATH = tmp_dir
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertIsNone(tss.do_rotatelogs('--days 30 --yes'))
            out = buf.getvalue()
            print(' rotatelogs output:', out)
            print(' files after rotation:', sorted(os.listdir(tmp_dir)))
            self.assertIn('Trade log rotation completed', out)
            self.assertIn('removed=1', out)
            self.assertIn(tmp_dir, out)
            self.assertFalse(os.path.exists(old_path))
            self.assertTrue(os.path.exists(recent_path))
        finally:
            qt.QT_TRADE_LOG_PATH = original_path
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_command_broker(self):
        """test broker status/connect/disconnect subcommands"""
        tss = self.tss
        broker = tss.trader.broker

        print('\n[TestCommandBroker] help returns False')
        self.assertFalse(tss.do_broker('-h'))

        print('[TestCommandBroker] status before connect')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_broker('status'))
        status_out = buf.getvalue()
        print(' status output:', status_out)
        self.assertIn('Broker Status', status_out)
        self.assertIn('Connected', status_out)
        self.assertIn('False', status_out)

        print('[TestCommandBroker] connect then status')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_broker('connect'))
        self.assertIn('Broker connected.', buf.getvalue())
        self.assertTrue(broker.is_connected)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_broker('status'))
        status_out = buf.getvalue()
        print(' status after connect:', status_out)
        self.assertIn('Broker Status', status_out)
        self.assertIn('True', status_out)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_broker('disconnect'))
        self.assertIn('Broker disconnected.', buf.getvalue())
        self.assertFalse(broker.is_connected)

        print(' BROKER_SUBCOMMANDS:', BROKER_SUBCOMMANDS)
        self.assertFalse(tss.do_broker(''))

    def test_orders_shows_order_ids(self):
        """orders 输出应包含 order_id 与 broker_order_id 列。"""
        tss = self.tss
        print('\n[TestOrdersShowsOrderIds] orders table includes id columns')

        order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=['000001.SZ'],
                positions=['long'],
                directions=['buy'],
                quantities=[100],
                prices=[10.0],
                data_source=tss.trader.datasource,
        )
        order_id = order_ids[0]
        submit_order(order_id, tss.trader.datasource)
        detail = read_trade_order_detail(order_id=order_id, data_source=tss.trader.datasource)
        print(' order detail:', detail)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_orders('--status submitted'))
        out = buf.getvalue()
        print(' orders output:\n', out)
        self.assertIn('id', out)
        self.assertIn('broker_id', out)
        self.assertIn(str(order_id), out)

    def test_history_excludes_unfilled_and_canceled(self):
        """history 应过滤无成交与已取消订单，仅保留 filled_qty>0 的记录。"""
        tss = self.tss
        test_ds = tss.trader.datasource
        print('\n[TestHistoryExcludesUnfilled] filter canceled/unfilled rows')

        filled_order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=['000001.SZ'],
                positions=['long'],
                directions=['buy'],
                quantities=[100],
                prices=[10.0],
                data_source=test_ds,
        )
        filled_order_id = filled_order_ids[0]
        submit_order(filled_order_id, test_ds)
        process_trade_result(
                raw_trade_result={
                    'order_id': filled_order_id,
                    'filled_qty': 100,
                    'price': 10.5,
                    'transaction_fee': 5.0,
                    'canceled_qty': 0.0,
                },
                data_source=test_ds,
        )

        canceled_order_ids = save_parsed_trade_orders(
                account_id=1,
                symbols=['000002.SZ'],
                positions=['long'],
                directions=['buy'],
                quantities=[100],
                prices=[11.0],
                data_source=test_ds,
        )
        canceled_order_id = canceled_order_ids[0]
        submit_order(canceled_order_id, test_ds)
        tss.do_cancel(str(canceled_order_id))

        history_df = tss.trader.history_orders()
        print(' history_orders:\n', history_df[['order_id', 'symbol', 'status', 'filled_qty', 'execution_time']])
        filled_rows = history_df[
            (history_df['filled_qty'].fillna(0) > 0) & history_df['execution_time'].notna()
        ]
        canceled_rows = history_df[history_df['order_id'] == canceled_order_id]
        print(' filled_rows count:', len(filled_rows), ' canceled row filled_qty:', canceled_rows['filled_qty'].tolist())
        self.assertEqual(len(filled_rows), 1)
        self.assertTrue(pd.isna(canceled_rows.iloc[0]['filled_qty']) or canceled_rows.iloc[0]['filled_qty'] == 0)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_history(''))
        out = buf.getvalue()
        print(' history output:\n', out)
        self.assertIn('000001.SZ', out)
        self.assertNotIn('000002.SZ', out)
        self.assertEqual(out.count('000001.SZ'), 1)

    def test_task_list_default_queued_and_all(self):
        """task -l 默认仅 queued；task -l -s all 与 tasks 等价。"""
        tss = self.tss
        print('\n[TestTaskList] default queued filter and all listing')

        queued_id = tss.trader.add_task('refill', 'stock_daily')
        done_spec = tss.trader._new_task_spec('pre_open', ())
        done_spec.status = 'done'
        tss.trader._task_registry[done_spec.task_id] = done_spec
        print(' queued_id:', queued_id, ' done_id:', done_spec.task_id)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_task('--list'))
        queued_out = buf.getvalue()
        print(' task -l output:', queued_out)
        self.assertIn(queued_id, queued_out)
        self.assertNotIn(done_spec.task_id, queued_out)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_task('--list --status all'))
        all_task_out = buf.getvalue()
        print(' task -l -s all output:', all_task_out)
        self.assertIn(queued_id, all_task_out)
        self.assertIn(done_spec.task_id, all_task_out)

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertIsNone(tss.do_tasks(''))
        tasks_out = buf.getvalue()
        print(' tasks output:', tasks_out)
        self.assertIn(queued_id, tasks_out)
        self.assertIn(done_spec.task_id, tasks_out)

        print(' normalize in-queue alias:', _normalize_task_status_filter('in queue'))
        self.assertEqual(_normalize_task_status_filter('in queue'), 'queued')

    def test_tasks_table_format_helper(self):
        """format_trader_tasks_table 输出 schedule 风格表格。"""
        print('\n[TestTasksTableFormat] helper table columns')
        task_spec = self.tss.trader._new_task_spec('refill', ('stock_daily',))
        table = format_trader_tasks_table([task_spec])
        print(' table:\n', table)
        self.assertIn('task_id', table)
        self.assertIn('name', table)
        self.assertIn('status', table)
        self.assertIn(task_spec.task_id, table)

    def test_rotatelogs_preview_and_confirm(self):
        """rotatelogs 预览、非 TTY 中止、--yes 执行删除。"""
        tss = self.tss
        print('\n[TestRotatelogsConfirm] preview/confirm/--yes behavior')

        tmp_dir = tempfile.mkdtemp()
        original_path = qt.QT_TRADE_LOG_PATH
        try:
            old_path = os.path.join(tmp_dir, 'old_account.risk.log')
            with open(old_path, 'w', encoding='utf-8') as f:
                f.write('old-risk\n')
            old_time = time.time() - 40 * 24 * 3600
            os.utime(old_path, (old_time, old_time))
            qt.QT_TRADE_LOG_PATH = tmp_dir

            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertIsNone(tss.do_rotatelogs('--days 30'))
            aborted_out = buf.getvalue()
            print(' non-interactive output:', aborted_out)
            self.assertIn('Rotation aborted (non-interactive)', aborted_out)
            self.assertTrue(os.path.exists(old_path))

            with patch('builtins.input', return_value='n'):
                with patch('qteasy.trader_cli._is_tty_stream', return_value=True):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        self.assertIsNone(tss.do_rotatelogs('--days 30'))
            canceled_out = buf.getvalue()
            print(' user declined output:', canceled_out)
            self.assertIn('Rotation canceled', canceled_out)
            self.assertTrue(os.path.exists(old_path))

            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertIsNone(tss.do_rotatelogs('--days 30 --yes'))
            yes_out = buf.getvalue()
            print(' --yes output:', yes_out)
            self.assertIn('Trade log rotation completed', yes_out)
            self.assertFalse(os.path.exists(old_path))
        finally:
            qt.QT_TRADE_LOG_PATH = original_path
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_command_sync_stub(self):
        """test sync stub and pull-state alias"""
        tss = self.tss

        print('\n[TestCommandSyncStub] help returns False')
        self.assertFalse(tss.do_sync('-h'))

        print('[TestCommandSyncStub] NOT_IMPLEMENTED message')
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertFalse(tss.do_sync(''))
        out = buf.getvalue()
        print(' sync output:', out)
        self.assertIn('[NOT_IMPLEMENTED]', out)
        self.assertIn('S2.1-b', out)

        print('[TestCommandSyncStub] pull-state alias via precmd')
        self.assertEqual(tss.precmd('pull-state'), 'sync')


class TestTraderCLIBuild1Coverage(unittest.TestCase):
    """Build 1 CLI 覆盖矩阵（PR 交付物）：Trader/Broker 公有能力与 CLI 命令对照。"""

    def test_coverage_matrix_rows(self):
        """核对矩阵关键行：Build1 新增命令已在 TraderShell 注册。"""
        print('\n[TestTraderCLIBuild1Coverage] TraderShell command registration')
        shell = TraderShell.__dict__
        build1_commands = (
            'do_artifacts',
            'do_liveconfig',
            'do_tasks',
            'do_task',
            'do_gate',
            'do_reconcile',
        )
        for cmd in build1_commands:
            print(' registered:', cmd, cmd in shell)
            self.assertIn(cmd, shell)


class TestTraderCLIBuild2Coverage(unittest.TestCase):
    """Build 2 CLI 覆盖矩阵：P2/P3 命令已在 TraderShell 注册。"""

    def test_coverage_matrix_rows(self):
        """核对 Build2 新增命令注册。"""
        print('\n[TestTraderCLIBuild2Coverage] TraderShell command registration')
        shell = TraderShell.__dict__
        build2_commands = (
            'do_rotatelogs',
            'do_broker',
            'do_sync',
        )
        for cmd in build2_commands:
            print(' registered:', cmd, cmd in shell)
            self.assertIn(cmd, shell)


class TestTraderCLIDashboardHelpers(unittest.TestCase):
    """CLI dashboard 辅助函数：日志过滤、消息队列排空与消息 coercion。"""

    def test_filter_sys_log_lines_excludes_debug(self):
        print('\n[TestTraderCLIDashboardHelpers] _filter_sys_log_lines')
        lines = [
            'INFO: normal line\n',
            'DEBUG: debug line\n',
            '<DEBUG><Jan01 10:00:00>running: trace\n',
        ]
        filtered = _filter_sys_log_lines(lines, include_debug=False)
        print(' filtered:', filtered)
        self.assertEqual(len(filtered), 1)
        self.assertIn('normal line', filtered[0])

    def test_filter_sys_log_lines_include_debug_returns_copy(self):
        print('\n[TestTraderCLIDashboardHelpers] _filter_sys_log_lines include_debug=True')
        lines = ['INFO: a\n', 'DEBUG: b\n']
        result = _filter_sys_log_lines(lines, include_debug=True)
        print(' result:', result)
        self.assertEqual(result, lines)
        self.assertIsNot(result, lines)

    def test_group_sys_log_physical_lines_merges_multiline_debug(self):
        print('\n[TestTraderCLIDashboardHelpers] group_sys_log_physical_lines multiline')
        lines = [
            '<DEBUG><May18 14:55:10>running: generated trade signals:\n',
            'symbols: []\n',
            'positions: []\n',
            '<May18 14:55:10>running: <RAN STRATEGY>: done.\n',
        ]
        grouped = group_sys_log_physical_lines(lines)
        print(' grouped count:', len(grouped))
        print(' grouped[0]:', repr(grouped[0]))
        print(' grouped[1]:', repr(grouped[1]))
        self.assertEqual(len(grouped), 2)
        self.assertIn('symbols: []', grouped[0])
        self.assertIn('positions: []', grouped[0])
        self.assertIn('<RAN STRATEGY>', grouped[1])

    def test_filter_sys_log_lines_excludes_multiline_debug_record(self):
        print('\n[TestTraderCLIDashboardHelpers] _filter_sys_log_lines multiline debug')
        lines = [
            'INFO: normal line\n',
            '<DEBUG><May18 14:55:10>running: generated trade signals:\n',
            'symbols: []\n',
            '<May18 14:55:10>running: done.\n',
        ]
        filtered = _filter_sys_log_lines(lines, include_debug=False)
        print(' filtered:', filtered)
        self.assertEqual(len(filtered), 2)
        self.assertIn('normal line', filtered[0])
        self.assertIn('done.', filtered[1])
        self.assertNotIn('symbols: []', ''.join(filtered))

    def test_drain_message_queue_returns_trader_messages(self):
        print('\n[TestTraderCLIDashboardHelpers] _drain_message_queue')
        from queue import Queue

        from qteasy.trader import drain_trader_message_queue

        queue = Queue()
        queue.put(TraderMessage(text='first', debug=False))
        queue.put('legacy-string')
        queue.put(TraderMessage(text='third', debug=True))

        drained = drain_trader_message_queue(queue)
        print(' drained:', drained)
        self.assertEqual(len(drained), 3)
        self.assertIsInstance(drained[0], TraderMessage)
        self.assertEqual(drained[0].text, 'first')
        self.assertFalse(drained[0].debug)
        self.assertEqual(drained[1].text, 'legacy-string')
        self.assertFalse(drained[1].debug)
        self.assertTrue(drained[2].debug)
        self.assertTrue(queue.empty())

    def test_coerce_trader_message_backward_compatible(self):
        print('\n[TestTraderCLIDashboardHelpers] coerce_trader_message')
        original = TraderMessage(text='structured', debug=True)
        coerced = coerce_trader_message(original)
        print(' coerced from TraderMessage:', coerced)
        self.assertIs(coerced, original)

        from_str = coerce_trader_message('plain text')
        print(' coerced from str:', from_str)
        self.assertIsInstance(from_str, TraderMessage)
        self.assertEqual(from_str.text, 'plain text')
        self.assertFalse(from_str.debug)

    def test_drain_message_queue_on_trader_instance(self):
        print('\n[TestTraderCLIDashboardHelpers] _drain_message_queue trader instance')
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            _detach_live_logger_handlers()
            trader.init_system_logger()
            trader.send_message('queued', debug=False)
            drained = _drain_message_queue(trader)
            print(' drained:', drained)
            self.assertEqual(len(drained), 1)
            self.assertEqual(drained[0].text, 'queued')
            self.assertTrue(trader.message_queue.empty())
        finally:
            clear_tables(test_ds)


class TestTraderCLIDashboardDisplay(unittest.TestCase):
    """TraderShell dashboard 显示：日志回放、状态行与普通行切换。"""

    def test_replay_dashboard_logs_drains_queue_and_filters_debug(self):
        """排空队列不重复打印；``include_debug=False`` 时系统日志回放不含 DEBUG 行。"""
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIDashboardDisplay] replay drains queue and filters debug')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            _detach_live_logger_handlers()
            trader.init_system_logger()
            log_path = sys_log_file_path_name(trader.account_id, test_ds)
            # 与同目录下固定 account_id=1 的其它用例隔离：重写种子行后再追加 send_message 的日志
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('INFO: cli_dashboard_extra_info_line\n')
                f.write('DEBUG: cli_dashboard_extra_debug_line\n')
            trader.send_message('cli_dashboard_queue_line', debug=False)
            trader.send_message('cli_dashboard_debug_logged_only', debug=True)
            print(' replay 前队列非空(仅非 debug send_message):', not trader.message_queue.empty())
            self.assertFalse(trader.message_queue.empty())
            shell = TraderShell(trader)
            buf = io.StringIO()
            with redirect_stdout(buf):
                shell._replay_dashboard_logs(50)
            out = buf.getvalue()
            print(' captured stdout:\n', out)
            self.assertTrue(trader.message_queue.empty())
            self.assertEqual(out.count('cli_dashboard_queue_line'), 1)
            self.assertEqual(out.count('cli_dashboard_extra_info_line'), 1)
            self.assertEqual(out.count('cli_dashboard_extra_debug_line'), 0)
            self.assertEqual(out.count('cli_dashboard_debug_logged_only'), 0)
        finally:
            clear_tables(test_ds)

    def test_replay_dashboard_logs_returns_requested_entry_count(self):
        """``_replay_dashboard_logs(N)`` 在 debug=False 时回放 N 条非 DEBUG 逻辑记录。"""
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIDashboardDisplay] replay returns N non-debug entries')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            _detach_live_logger_handlers()
            log_path = sys_log_file_path_name(trader.account_id, test_ds)
            with open(log_path, 'w', encoding='utf-8') as f:
                for i in range(30):
                    f.write(f'INFO: cli_replay_info_{i:02d}\n')
                for i in range(30):
                    f.write(f'DEBUG: cli_replay_debug_{i:02d}\n')
            shell = TraderShell(trader)
            buf = io.StringIO()
            with redirect_stdout(buf):
                shell._replay_dashboard_logs(20)
            out = buf.getvalue()
            print(' replay line count in stdout:', out.count('\n'))
            info_hits = sum(out.count(f'cli_replay_info_{i:02d}') for i in range(30))
            debug_hits = sum(out.count(f'cli_replay_debug_{i:02d}') for i in range(30))
            print(' info_hits:', info_hits, ' debug_hits:', debug_hits)
            self.assertEqual(info_hits, 20)
            self.assertEqual(debug_hits, 0)
            self.assertIn('cli_replay_info_29', out)
            self.assertIn('cli_replay_info_10', out)
        finally:
            clear_tables(test_ds)

    def test_print_status_line_pads_to_width(self):
        """状态行按终端宽度用空格填充并以 ``\\r`` 结尾。"""
        tss = TraderShell.__new__(TraderShell)
        tss._dashboard_on_status_line = False
        tss._last_status_plain = None
        print('\n[TestTraderCLIDashboardDisplay] status line padding')
        with patch('qteasy.trader_cli._terminal_width', return_value=18):
            buf = io.StringIO()
            with redirect_stdout(buf):
                tss._print_status_line(Text('xyz'))
        raw = buf.getvalue()
        print(' raw repr:', repr(raw))
        self.assertTrue(tss._dashboard_on_status_line)
        self.assertTrue(raw.endswith('\r'), msg=f'expected carriage return ending, got {raw!r}')
        if '\x1b' not in raw:
            self.assertEqual(len(raw.rstrip('\r')), 18)

    def test_print_log_line_clears_status_flag(self):
        """``_print_log_line`` 在状态行之后打印时清除状态并复位标志。"""
        tss = TraderShell.__new__(TraderShell)
        tss._dashboard_on_status_line = True
        print('\n[TestTraderCLIDashboardDisplay] log line clears status flag')
        with patch('qteasy.trader_cli._clear_current_line') as mock_clear:
            buf = io.StringIO()
            with redirect_stdout(buf):
                tss._print_log_line('log_after_status')
        mock_clear.assert_called_once()
        self.assertFalse(tss._dashboard_on_status_line)
        self.assertIn('log_after_status', buf.getvalue())

    def test_do_dashboard_no_duplicate_on_reentry(self):
        """单次回放不因「队列一次 + 日志一次」重复；再次回放日志尾部仍可含同一条入库消息。"""
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIDashboardDisplay] no duplicate queue on reentry')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            _detach_live_logger_handlers()
            trader.init_system_logger()
            log_path = sys_log_file_path_name(trader.account_id, test_ds)
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('INFO: cli_dashboard_reentry_log\n')
            trader.send_message('cli_dashboard_reentry_queue', debug=False)
            shell = TraderShell(trader)
            with patch('os.system'):
                buf1 = io.StringIO()
                with redirect_stdout(buf1):
                    shell.do_dashboard('-r 20')
            out1 = buf1.getvalue()
            print(' first do_dashboard output sample:', out1[:400])
            self.assertEqual(out1.count('cli_dashboard_reentry_queue'), 1)
            self.assertTrue(trader.message_queue.empty())
            buf2 = io.StringIO()
            with redirect_stdout(buf2):
                shell._replay_dashboard_logs(20)
            out2 = buf2.getvalue()
            print(' second replay output sample:', out2[:400])
            self.assertEqual(out2.count('cli_dashboard_reentry_queue'), 1)
            self.assertIn('cli_dashboard_reentry_log', out2)
        finally:
            clear_tables(test_ds)

    def test_new_sys_logger_idempotent_single_log_line(self):
        """连续 ``init_system_logger`` 仅保留一个 Handler；``send_message`` 在日志中只写一行。"""
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIDashboardDisplay] new_sys_logger idempotent')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            _detach_live_logger_handlers(trader.account_id)
            trader.init_system_logger()
            trader.init_system_logger()
            logger_name = _live_logger_name(trader.account_id)
            handler_count = len(logging.getLogger(logger_name).handlers)
            print(' handler_count after double init:', handler_count)
            self.assertEqual(handler_count, 1)

            log_path = sys_log_file_path_name(trader.account_id, test_ds)
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('')
            trader.send_message('cli_dashboard_marker_once', debug=False)
            with open(log_path, 'r', encoding='utf-8') as f:
                log_body = f.read()
            marker_count = log_body.count('cli_dashboard_marker_once')
            print(' marker_count in log:', marker_count)
            self.assertEqual(marker_count, 1)
        finally:
            clear_tables(test_ds)

    def test_print_status_line_cjk_single_physical_line(self):
        """窄终端 + 中文监视价时，状态行物理上仅一行（末尾 ``\\r``）。"""
        tss = TraderShell.__new__(TraderShell)
        tss._dashboard_on_status_line = False
        tss._last_status_plain = None
        print('\n[TestTraderCLIDashboardDisplay] CJK status single line')
        status = Text('<May18 09:45:00>running: acquire_live_price in about 5 sec')
        status.append(
                ' =600036招商银行/37.51/-0.37% =601398工商银行/7.23/-0.14% 1格力电器/40.24/-0.10%',
        )
        with patch('qteasy.trader_cli._terminal_width', return_value=40):
            buf = io.StringIO()
            with redirect_stdout(buf):
                tss._print_status_line(status)
        raw = buf.getvalue()
        print(' raw repr:', repr(raw))
        newline_count = raw.rstrip('\r').count('\n')
        print(' newline_count (excl trailing cr):', newline_count)
        self.assertEqual(newline_count, 0)
        self.assertTrue(raw.endswith('\r'))

    def test_dashboard_status_skip_unchanged_plain(self):
        """相同 ``plain`` 的连续状态行不重复写入 stdout。"""
        tss = TraderShell.__new__(TraderShell)
        tss._dashboard_on_status_line = False
        tss._last_status_plain = None
        print('\n[TestTraderCLIDashboardDisplay] status debounce')
        with patch('qteasy.trader_cli._terminal_width', return_value=80):
            buf = io.StringIO()
            with redirect_stdout(buf):
                tss._print_status_line(Text('same status plain'))
                tss._print_status_line(Text('same status plain'))
        raw = buf.getvalue()
        print(' write count via repr segments:', raw.count('same status plain'))
        self.assertEqual(raw.count('same status plain'), 1)

    def test_dataframe_log_preview_head_and_row_count(self):
        """``dataframe_log_preview`` 仅含 head 与总行数，不含整表。"""
        print('\n[TestTraderCLIDashboardDisplay] dataframe_log_preview')
        df = pd.DataFrame({'a': range(10)})
        preview = dataframe_log_preview(df, head=3)
        print(' preview:\n', preview)
        self.assertIn('rows total', preview)
        self.assertNotIn('\n4 ', preview)
        self.assertNotIn('\n9 ', preview)


class TestTraderCLIModeMenu(unittest.TestCase):
    """TraderShell Ctrl+C 模式选单：解析、超时读取与中断处理。"""

    def test_parse_mode_menu_choice(self):
        print('\n[TestTraderCLIModeMenu] _parse_mode_menu_choice')
        self.assertEqual(_parse_mode_menu_choice('1'), 'command')
        self.assertEqual(_parse_mode_menu_choice('2'), 'dashboard')
        self.assertEqual(_parse_mode_menu_choice('3'), 'exit')
        self.assertEqual(_parse_mode_menu_choice(None), 'resume')
        self.assertEqual(_parse_mode_menu_choice(''), 'resume')
        self.assertEqual(_parse_mode_menu_choice('9'), 'resume')
        print(' choices: 1->command, 2->dashboard, 3->exit, None/empty/9->resume')

    def test_read_line_with_timeout_non_tty_returns_none(self):
        print('\n[TestTraderCLIModeMenu] _read_line_with_timeout non-TTY')
        mock_in = io.StringIO()
        mock_out = io.StringIO()
        with patch('qteasy.trader_cli._is_tty_stream', return_value=False):
            result = _read_line_with_timeout('prompt> ', timeout=1.0,
                                             input_stream=mock_in, output_stream=mock_out)
        print(' result:', result, ' output:', repr(mock_out.getvalue()))
        self.assertIsNone(result)
        self.assertEqual(mock_out.getvalue(), '')

    def test_read_line_with_timeout_unix_returns_line(self):
        print('\n[TestTraderCLIModeMenu] _read_line_with_timeout_unix line')
        mock_in = MagicMock()
        mock_in.fileno.return_value = 0
        mock_in.read.side_effect = ['1']
        mock_out = io.StringIO()
        sentinel_attrs = object()
        with patch('termios.tcgetattr', return_value=sentinel_attrs), \
                patch('termios.tcsetattr') as mock_tcsetattr, \
                patch('tty.setcbreak'), \
                patch('select.select', return_value=([mock_in], [], [])):
            result = _read_line_with_timeout_unix(1.0, mock_in, mock_out)
        print(' result:', result, ' output:', repr(mock_out.getvalue()))
        self.assertEqual(result, '1')
        self.assertIn('1\n', mock_out.getvalue())
        mock_tcsetattr.assert_called()

    def test_read_line_with_timeout_unix_timeout(self):
        print('\n[TestTraderCLIModeMenu] _read_line_with_timeout_unix timeout')
        mock_in = MagicMock()
        mock_in.fileno.return_value = 0
        mock_out = io.StringIO()
        sentinel_attrs = object()
        with patch('termios.tcgetattr', return_value=sentinel_attrs), \
                patch('termios.tcsetattr'), \
                patch('tty.setcbreak'), \
                patch('select.select', return_value=([], [], [])):
            result = _read_line_with_timeout_unix(0.1, mock_in, mock_out)
        print(' result:', result, ' output:', repr(mock_out.getvalue()))
        self.assertIsNone(result)
        self.assertEqual(mock_out.getvalue(), '\n')

    def test_accept_mode_menu_char_single_key(self):
        print('\n[TestTraderCLIModeMenu] _accept_mode_menu_char')
        parts: list = []
        self.assertEqual(_accept_mode_menu_char('1', parts), '1')
        self.assertEqual(parts, [])
        parts = []
        self.assertEqual(_accept_mode_menu_char('2', parts), '2')
        parts = []
        self.assertIsNone(_accept_mode_menu_char('x', parts))
        self.assertEqual(parts, [])
        parts = []
        self.assertIsNone(_accept_mode_menu_char('\n', parts))
        self.assertEqual(parts, [])
        parts = ['1']
        self.assertEqual(_accept_mode_menu_char('\n', parts), '1')
        self.assertEqual(parts, [])
        parts = ['9']
        self.assertIsNone(_accept_mode_menu_char('\n', parts))
        self.assertEqual(parts, [])
        print(' accept: digits immediate; x/empty Enter ignored; buffered Enter for 1/9 cleared')

    def test_handle_mode_interrupt_resume(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIModeMenu] _handle_mode_interrupt resume on timeout')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'
            buf = io.StringIO()
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value=None):
                with redirect_stdout(buf):
                    keep_running = shell._handle_mode_interrupt()
            out = buf.getvalue()
            print(' keep_running:', keep_running, ' status:', shell._status, ' stdout sample:', out[:120])
            self.assertTrue(keep_running)
            self.assertEqual(shell._status, 'dashboard')
            self.assertIn('No input received; resuming previous mode.', out)
        finally:
            clear_tables(test_ds)

    def test_handle_mode_interrupt_command(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIModeMenu] _handle_mode_interrupt command choice')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value='1'):
                keep_running = shell._handle_mode_interrupt()
            print(' keep_running:', keep_running, ' status:', shell._status)
            self.assertTrue(keep_running)
            self.assertEqual(shell._status, 'command')
        finally:
            clear_tables(test_ds)

    def test_handle_mode_interrupt_dashboard(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIModeMenu] _handle_mode_interrupt dashboard choice')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value='2'):
                keep_running = shell._handle_mode_interrupt()
            print(' keep_running:', keep_running, ' status:', shell._status)
            self.assertTrue(keep_running)
            self.assertEqual(shell._status, 'dashboard')
        finally:
            clear_tables(test_ds)

    def test_handle_mode_interrupt_exit(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIModeMenu] _handle_mode_interrupt exit choice')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value='3'):
                keep_running = shell._handle_mode_interrupt()
            print(' keep_running:', keep_running, ' status:', shell._status)
            self.assertFalse(keep_running)
        finally:
            clear_tables(test_ds)

    def test_handle_mode_interrupt_second_ctrl_c_exits(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIModeMenu] _handle_mode_interrupt second Ctrl+C (ModeMenuExitRequest)')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'

            def _raise_exit(*_args, **_kwargs):
                raise ModeMenuExitRequest

            buf = io.StringIO()
            with patch('qteasy.trader_cli._read_line_with_timeout', side_effect=_raise_exit):
                with redirect_stdout(buf):
                    keep_running = shell._handle_mode_interrupt()
            out = buf.getvalue()
            print(' keep_running:', keep_running, ' stdout:', repr(out))
            self.assertFalse(keep_running)
            self.assertIn('Interrupted again; shutting down trader...', out)
        finally:
            clear_tables(test_ds)


class TestTraderCLIPhase4(unittest.TestCase):
    """Trader CLI Phase 4：监视价 debounce 与运行时错误恢复。"""

    def test_maybe_refresh_watched_prices_skips_alive_worker(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIPhase4] _maybe_refresh_watched_prices skips alive worker')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            trader.is_market_open = True
            mock_worker = MagicMock()
            mock_worker.is_alive.return_value = True
            with patch('qteasy.trader_cli.Thread') as mock_thread_cls:
                mock_thread_cls.return_value = mock_worker
                with patch.object(shell, 'format_watched_prices'):
                    shell._maybe_refresh_watched_prices()
                    first_thread_calls = mock_thread_cls.call_count
                    first_start_calls = mock_worker.start.call_count
                    shell._maybe_refresh_watched_prices()
                    second_thread_calls = mock_thread_cls.call_count
                    second_start_calls = mock_worker.start.call_count
            print(' thread calls:', first_thread_calls, '->', second_thread_calls)
            print(' start calls:', first_start_calls, '->', second_start_calls)
            self.assertEqual(first_thread_calls, 1)
            self.assertEqual(second_thread_calls, 1)
            self.assertEqual(first_start_calls, 1)
            self.assertEqual(second_start_calls, 1)
        finally:
            clear_tables(test_ds)

    def test_handle_runtime_error_default_dashboard(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIPhase4] _handle_runtime_error default dashboard on timeout')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'command'
            buf = io.StringIO()
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value=None):
                with redirect_stdout(buf):
                    keep_running = shell._handle_runtime_error(RuntimeError('test failure'))
            out = buf.getvalue()
            print(' keep_running:', keep_running, ' status:', shell._status, ' stdout sample:', out[:160])
            self.assertTrue(keep_running)
            self.assertEqual(shell._status, 'dashboard')
            self.assertIn('Returning to dashboard mode.', out)
            self.assertIn('Unexpected Error: test failure', out)
        finally:
            clear_tables(test_ds)

    def test_handle_runtime_error_exit_on_3(self):
        from tests.trader_test_helpers import create_trader_with_account, clear_tables

        print('\n[TestTraderCLIPhase4] _handle_runtime_error exit on choice 3')
        trader, test_ds = create_trader_with_account(debug=False, legacy=True)
        try:
            shell = TraderShell(trader)
            shell._status = 'dashboard'
            with patch('qteasy.trader_cli._read_line_with_timeout', return_value='3'):
                keep_running = shell._handle_runtime_error(ValueError('fatal'))
            print(' keep_running:', keep_running, ' status:', shell._status)
            self.assertFalse(keep_running)
        finally:
            clear_tables(test_ds)

    def test_parse_error_recovery_choice(self):
        print('\n[TestTraderCLIPhase4] _parse_error_recovery_choice')
        self.assertEqual(_parse_error_recovery_choice('3'), 'exit')
        self.assertEqual(_parse_error_recovery_choice('1'), 'dashboard')
        self.assertEqual(_parse_error_recovery_choice(None), 'dashboard')
        self.assertEqual(_parse_error_recovery_choice(''), 'dashboard')
        print(' choices: 3->exit, 1/None/empty->dashboard')


if __name__ == '__main__':
    unittest.main()
