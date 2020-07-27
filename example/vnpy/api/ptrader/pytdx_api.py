from typing import Any
from time import sleep
from copy import copy
from concurrent.futures import ThreadPoolExecutor, wait

import pandas as pd
from threading import Thread

from pytdx.hq import TdxHq_API

"""
# K线种类
# 0 5分钟K线
# 1 15分钟K线
# 2 30分钟K线
# 3 1小时K线
# 4 日K线
# 5 周K线
# 6 月K线
# 7 1分钟
# 8 1分钟K线
# 9 日K线
# 10 季K线
# 11 年K线
"""
# 周期转换
freq_dict = {}
freq_dict["5M"] = 0
freq_dict["15M"] = 1
freq_dict["30M"] = 2
freq_dict["1H"] = 3
freq_dict["1M"] = 6
freq_dict["1W"] = 5
freq_dict["1m"] = 7
freq_dict["1D"] = 4
freq_dict["1Q"] = 10
freq_dict["1Y"] = 11

# 市场代码对照表
exchange_map = {}
exchange_map["SH"] = 1
exchange_map["SZ"] = 0

# 周期乘数参数
period_dict = {}
period_dict["5M"] = 48
period_dict["15M"] = 16
period_dict["30M"] = 8
period_dict["1H"] = 4
period_dict["1M"] = 240
period_dict["1W"] = 1
period_dict["1m"] = 1
period_dict["1D"] = 1
period_dict["4m"] = 1
period_dict["1Y"] = 1


class PytdxApi:
    """TDX数据服务类"""

    def __init__(self):
        """Constructor"""
        self.connect_status: bool = False
        self.login_status: bool = False

        self.hq_api = None  # 行情API
        self.conc_code_num = 50  # 并发获取行情的股票个数

        # 行情订阅
        self.active = False
        self.run_subscribe = Thread(target=self.get_realtime_data)
        self.symbols = list()
        self.symbols_split = list()

    def connect_api(self, host: str = "", port: int = 0):
        """连接行情api"""
        # 连接行情API并检查连接情况
        try:
            if not self.connect_status:
                self.hq_api = TdxHq_API()
                self.hq_api.connect(host, port)
                self.connect_status = True
                self.login_status = True
                self.subscribe_start()

        except Exception as e:
            return e

    def get_realtime_quotes(self, quotes_list: list):
        """获取实时行情数据"""
        data = self.hq_api.get_security_quotes(quotes_list)
        return data

    def get_realtime_data(self):
        """获取实时行情切片"""
        try:
            while self.active:
                if not self.symbols_split:
                    sleep(1)
                    continue

                data = list()
                for symbols in self.symbols_split:
                    d = self.get_realtime_quotes(symbols)
                    data.extend(d)

                self.on_tick_data(data)
                sleep(2)
        except:
            error = dict()
            error["error_id"] = "pytdx"
            error["error_msg"] = "行情订阅失败"
            self.on_error(error)

    def get_transaction_count(self, market: int) -> int:
        """
        查询市场标的数量
        """
        return self.hq_api.get_security_count(market)

    def get_transaction_list(self, market: int, start: int) -> list:
        """查询市场标的列表"""
        return self.hq_api.get_security_list(market, start)

    def subscribe_start(self):
        """启动行情订阅"""
        self.active = True
        self.run_subscribe.start()

    def subscribe(self, symbol: Any):
        """订阅行情数据"""
        if isinstance(symbol, tuple):
            if symbol not in self.symbols:
                self.symbols.append(symbol)
        elif isinstance(symbol, list):
            for s in symbol:
                if s not in self.symbols:
                    self.symbols.append(s)
        else:
            error = dict()
            error["error_id"] = "pytdx"
            error["error_msg"] = f"订阅标的代码格式不正确{symbol}"
            self.on_error(error)
            return

        symbol_split = self.get_code_split()
        self.symbols_split = copy(symbol_split)

    def subscribe_close(self):
        """关闭订阅"""
        if self.active:
            self.active = False
            self.run_subscribe.join()

    def get_transaction_info(self):
        """获取所有合约信息"""
        for exchange in list(exchange_map.values()):
            count = self.get_transaction_count(exchange)
            for c in range(0, count, 1000):
                symbols = self.get_transaction_list(exchange, c)
                for symbol in symbols:
                    symbol["exchange"] = exchange
                    if symbol["code"][:2] in ["60", "30", "688", "00"]:
                        symbol["product"] = 3
                    else:
                        symbol["product"] = 2

                    self.on_contract_info(symbol, False)

            self.on_contract_info({"exchange": exchange}, True)

    def get_all_stock(self):
        """获取所有股票数据"""
        stocks = list()

        for exchange in list(exchange_map.values()):
            count = self.get_transaction_count(exchange)
            for s in range(0, count, 1000):
                d = self.get_transaction_list(exchange, s)
                stocks.extend(d)

        l = len(stocks) - 1
        for i, stock in enumerate(stocks):
            if stock["code"][:1] in ["60", "30", "688", "00"]:
                if i == l:
                    self.on_contract_info(stock, True)
                else:
                    self.on_contract_info(stock, False)

    def on_contract_info(self, data: dict, last: bool) -> None:
        """"""
        pass

    def on_tick_data(self, data):
        """订阅数据处理"""
        pass

    def on_error(self, err):
        """接口错误处理"""
        pass

    @staticmethod
    def generate_symbols(symbols: list):
        """组装symbols数据，pytdx接收的是以市场代码和标的代码组成的元祖的list"""
        new_symbols = []

        for symbol in symbols:
            code, exchange = symbol.split(".")
            new_symbol = (exchange_map[exchange], code)
            new_symbols.append(new_symbol)

        return new_symbols

    @staticmethod
    def get_fast_ip():
        """获取最快IP"""
        host = "210.51.39.201"
        port = 7709

        return host, port

    @staticmethod
    def check_symbol(symbol: str):
        """检查标的格式"""
        if symbol:
            code, market = symbol.split(".")
            market = exchange_map.get(market)
            return code, market

        else:
            return False

    def get_code_split(self):
        """获得切割好的股票代码段"""
        code_split_list = []
        for i in range(0, len(self.symbols) + 1, self.conc_code_num):
            code_split = self.symbols[i : i + self.conc_code_num]
            code_split_list.append(code_split)

        return code_split_list

    def exit(self):
        """数据服务关闭"""
        # 关闭订阅
        self.subscribe_close()

        # 关闭接口
        self.login_status = False
        self.connect_status = False
        self.hq_api.disconnect()
        self.hq_api = None
