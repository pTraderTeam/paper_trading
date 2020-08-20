from ..api.pytdx_api import exchange_map


class ExDividend(object):
    """除权除息"""

    def __init__(self, tdx_api, search_date):
        """
        初始化
        :param tdx_api: 通达信api
        :param search_date: 查询日期
        """
        self.api = tdx_api
        self.search_date = search_date

    def is_dr(self, pos_list: list):
        """当前持仓是否包含除权除息股票"""
        dr = self.fmt_dr(pos_list)
        return True if dr else False

    def fmt_dr(self, pos_list: list):
        """
        某个日期内的一组股票里筛选包含除权除息数据的股票
        :param pos_list:
        :return: {"000001": Data}
        """
        ret_data = {}
        for info in pos_list:
            market = exchange_map.get(info["exchange"])
            tmp_dict = {
                info["code"]: x
                for x in self.api.get_xdxr_info(market, info["code"])
                if self.search_date == f"{x['year']}{str(x['month']).zfill(2)}{str(x['day']).zfill(2)}" and x["category"] == 1
            }
            ret_data.update(tmp_dict)
        return ret_data

    def dr_cal(self, pos_list: list, account: dict):
        """除权除息计算"""
        dr_pos_list = []
        signals = []
        for info in pos_list:
            market = exchange_map.get(info["exchange"])
            xd_data = [
                x
                for x in self.api.get_xdxr_info(market, info["code"])
                if self.search_date == f"{x['year']}{str(x['month']).zfill(2)}{str(x['day']).zfill(2)}" and x["category"] == 1
            ]
            if not len(xd_data):
                continue
            signal_list = self.dr_opt(account, info, xd_data[0])
            dr_pos_list.append(info)
            signals.append(signal_list)
        return dr_pos_list, signals

    def dr_opt(self, account: dict, pos: dict, dr_data: dict):
        """
        由于送股、分红、配股等原因，处理股票持仓信息，一些概率小的没有做处理
        :param account: 账户资产
        :param pos: 持仓信息
        :param dr_data: dr数据
        :return:
        """
        signal_list = list()

        # 转赠/送股
        if dr_data["songzhuangu"]:
            stock_volume = int(pos["volume"] * dr_data["songzhuangu"] / 10)
            pos["volume"] += stock_volume
            pos["available"] = pos["volume"]
            pos["buy_price"] = pos["buy_price"] / (1 + dr_data["songzhuangu"] / 10)
            # 转赠/送股信号
            signal = {
                "SYMBOL": pos["code"],
                "TPRICE": 0,  # 除权交易的价格给
                "SIGNAL": 220010,  # 除权
                "TDATE": self.search_date,
                "SNAME": pos.get("symbol_name"),
                "OPERATOR": 0,  # 不需填
                "MARKET": pos["exchange"],
                "STKEFFEFT": stock_volume,
                "TAX": 0,
            }
            signal_list.append(signal)
        # 分红
        if dr_data["fenhong"]:
            bonus = int(pos["volume"] * dr_data["fenhong"] / 10)
            account["available"] += bonus
            account["market_value"] -= bonus
            pos["buy_price"] = pos["buy_price"] - dr_data["fenhong"] / 10
            # 除息的信号
            signal = {
                "SYMBOL": pos["code"],
                "TPRICE": 0,  # 除权交易的价格给
                "SIGNAL": 221007,  # 除息
                "TDATE": self.search_date,
                "SNAME": pos.get("symbol_name", ""),
                "OPERATOR": 0,  # 不需填
                "MARKET": pos["exchange"],
                "STKEFFEFT": bonus,
                "TAX": 0,
            }
            signal_list.append(signal)
        # 配股
        if dr_data["peigu"] and dr_data["peigujia"]:
            pass
        return signal_list
