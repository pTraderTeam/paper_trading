import pytz
from typing import Any, Dict, List
from datetime import datetime

from vnpy.api.ptrader.pt_api import PTraderApi
from vnpy.api.ptrader.pytdx_api import PytdxApi
from vnpy.event import EventEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import Exchange, Product, Direction, OrderType, Status, Offset
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import CancelRequest, OrderRequest, SubscribeRequest, TickData, ContractData, OrderData, TradeData, PositionData, AccountData
from vnpy.trader.utility import round_to


MARKET_PT2VT: Dict[int, Exchange] = {0: Exchange.SZSE, 1: Exchange.SSE}
MARKET_VT2PT: Dict[Exchange, str] = {v: k for k, v in MARKET_PT2VT.items()}

EXCHANGE_PT2VT: Dict[str, Exchange] = {"SH": Exchange.SSE, "SZ": Exchange.SZSE}
EXCHANGE_VT2PT: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_PT2VT.items()}

DIRECTION_STOCK_PT2VT: Dict[str, Any] = {"buy": (Direction.LONG, Offset.NONE), "sell": (Direction.SHORT, Offset.NONE)}
DIRECTION_STOCK_VT2PT: Dict[Any, int] = {v: k for k, v in DIRECTION_STOCK_PT2VT.items()}

POSITION_DIRECTION_PT2VT = {0: Direction.NET, 1: Direction.LONG, 2: Direction.SHORT, 3: Direction.SHORT}

ORDERTYPE: List[OrderType] = [OrderType.LIMIT, OrderType.MARKET]
ORDERTYPE_PT2VT: Dict[str, OrderType] = {"限价": OrderType.LIMIT, "市价": OrderType.MARKET}

STATUS_PT2VT: Dict[int, Status] = {
    Status.SUBMITTING.value: Status.SUBMITTING,
    Status.ALLTRADED.value: Status.ALLTRADED,
    Status.PARTTRADED.value: Status.PARTTRADED,
    Status.CANCELLED.value: Status.CANCELLED,
    Status.NOTTRADED.value: Status.NOTTRADED,
    Status.REJECTED.value: Status.REJECTED,
}

PRODUCT_PT2VT: Dict[int, Product] = {
    0: Product.EQUITY,
    1: Product.INDEX,
    2: Product.FUND,
    3: Product.BOND,
    4: Product.OPTION,
    5: Product.EQUITY,
    6: Product.OPTION,
}

OFFSET_VT2PT: Dict[Offset, int] = {Offset.NONE: 0, Offset.OPEN: 1, Offset.CLOSE: 2, Offset.CLOSETODAY: 4, Offset.CLOSEYESTERDAY: 5}
OFFSET_PT2VT: Dict[int, Offset] = {v: k for k, v in OFFSET_VT2PT.items()}

CHINA_TZ = pytz.timezone("Asia/Shanghai")

symbol_name_map: Dict[str, str] = {}
symbol_pricetick_map: Dict[str, float] = {}


class PTraderGateway(BaseGateway):

    default_setting: Dict[str, Any] = {
        "账号": "lbFOGxdbxjkYarJJ80TU",
        "行情地址": "210.51.39.201",
        "行情端口": 7709,
        "交易地址": "localhost",
        "交易端口": 5001,
    }

    exchanges: List[Exchange] = list(MARKET_VT2PT.keys())

    def __init__(self, event_engine: EventEngine):
        """"""
        super().__init__(event_engine, "PT")

        self.md_api = PtMdApi(self)
        self.td_api = PtTdApi(self)

    def connect(self, setting: dict) -> None:
        """"""
        userid = setting["账号"]
        quote_ip = setting["行情地址"]
        quote_port = int(setting["行情端口"])
        trader_ip = setting["交易地址"]
        trader_port = int(setting["交易端口"])

        self.md_api.connect(userid, quote_ip, quote_port)
        self.td_api.connect(userid, trader_ip, trader_port)
        self.init_query()

    def close(self) -> None:
        """"""
        self.md_api.close()
        self.td_api.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        self.md_api.add_subscrbie(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """"""
        self.td_api.query_position()

    def query_orders(self) -> None:
        """查询当日订单"""
        self.td_api.query_orders_today()

    def process_timer_event(self, event) -> None:
        """"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position, self.query_orders]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def write_error(self, msg: str, error: dict) -> None:
        """"""
        error_id = error["error_id"]
        error_msg = error["error_msg"]
        msg = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)


class PtMdApi(PytdxApi):
    def __init__(self, gateway: PTraderGateway):
        """"""
        super().__init__()

        self.gateway: PTraderGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.userid: str = ""
        self.server_ip: str = ""
        self.server_port: int = 0

        self.sse_inited: bool = False
        self.szse_inited: bool = False

    def on_disconnect(self, reason: int) -> None:
        """"""
        self.connect_status = False
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开, 原因{reason}")

        self.login_server()

    def on_error(self, error: dict) -> None:
        """"""
        self.gateway.write_error("行情接口报错", error)

    def on_tick_data(self, data: list) -> None:
        """"""
        if not data:
            error = dict()
            error["error_id"] = "pytdx"
            error["error_msg"] = "tick数据为空"
            self.on_error(error)
            return

        dt = datetime.now()
        for d in data:
            tick = TickData(
                symbol=d["code"],
                exchange=MARKET_PT2VT[d["market"]],
                datetime=dt,
                volume=d["vol"],
                last_price=d["price"],
                open_price=d["open"],
                high_price=d["high"],
                low_price=d["low"],
                pre_close=d["last_close"],
                gateway_name=self.gateway_name,
                bid_price_1=d["bid1"],
                bid_price_2=d["bid2"],
                bid_price_3=d["bid3"],
                bid_price_4=d["bid4"],
                bid_price_5=d["bid5"],
                ask_price_1=d["ask1"],
                ask_price_2=d["ask2"],
                ask_price_3=d["ask3"],
                ask_price_4=d["ask4"],
                ask_price_5=d["ask5"],
                bid_volume_1=d["bid_vol1"],
                bid_volume_2=d["bid_vol2"],
                bid_volume_3=d["bid_vol3"],
                bid_volume_4=d["bid_vol4"],
                bid_volume_5=d["bid_vol5"],
                ask_volume_1=d["ask_vol1"],
                ask_volume_2=d["ask_vol2"],
                ask_volume_3=d["ask_vol3"],
                ask_volume_4=d["ask_vol4"],
                ask_volume_5=d["ask_vol5"],
            )
            # pricetick = symbol_pricetick_map.get(tick.vt_symbol, 0)
            # if pricetick:
            #     tick.bid_price_1 = round_to(tick.bid_price_1, pricetick)
            #     tick.bid_price_2 = round_to(tick.bid_price_2, pricetick)
            #     tick.bid_price_3 = round_to(tick.bid_price_3, pricetick)
            #     tick.bid_price_4 = round_to(tick.bid_price_4, pricetick)
            #     tick.bid_price_5 = round_to(tick.bid_price_5, pricetick)
            #     tick.ask_price_1 = round_to(tick.ask_price_1, pricetick)
            #     tick.ask_price_2 = round_to(tick.ask_price_2, pricetick)
            #     tick.ask_price_3 = round_to(tick.ask_price_3, pricetick)
            #     tick.ask_price_4 = round_to(tick.ask_price_4, pricetick)
            #     tick.ask_price_5 = round_to(tick.ask_price_5, pricetick)

            tick.name = symbol_name_map.get(tick.vt_symbol, tick.symbol)
            self.gateway.on_tick(tick)

    def on_contract_info(self, data: dict, last: bool) -> None:
        """"""
        exchange = MARKET_PT2VT[data["exchange"]]
        if last:
            self.gateway.write_log(f"{exchange.value}合约信息查询成功")

            if exchange == Exchange.SSE:
                self.sse_inited = True
            else:
                self.szse_inited = True
        else:
            contract = ContractData(
                symbol=data["code"],
                exchange=exchange,
                name=data["name"],
                product=PRODUCT_PT2VT[data["product"]],
                size=1,
                pricetick=data["decimal_point"],
                min_volume=data["volunit"],
                gateway_name=self.gateway_name,
            )

            if contract.product != Product.OPTION:
                self.gateway.on_contract(contract)

            symbol_name_map[contract.vt_symbol] = data["name"]
            symbol_pricetick_map[contract.vt_symbol] = data["decimal_point"]

    def connect(self, userid: str, server_ip: str, server_port: int,) -> None:
        """"""
        self.userid = userid
        self.server_ip = server_ip
        self.server_port = server_port

        self.login_server()

    def login_server(self):
        n = self.connect_api(self.server_ip, self.server_port)

        if not n:
            msg = "行情服务器登录成功"

            # 获取合约信息
            self.get_transaction_info()
        else:
            msg = f"行情服务器登录失败，原因：{n}"

        self.gateway.write_log(msg)

    def close(self) -> None:
        """"""
        if self.connect_status:
            self.exit()

    def add_subscrbie(self, req: SubscribeRequest) -> None:
        """"""
        if self.login_status:
            exchange = MARKET_VT2PT.get(req.exchange, "")
            self.subscribe((exchange, req.symbol))


class PtTdApi(PTraderApi):
    def __init__(self, gateway: PTraderGateway):
        """"""
        super().__init__()

        self.user_id: str = ""
        self.gateway: PTraderGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.orders: Dict[str, OrderData] = {}

    def on_disconnected(self, reason: int) -> None:
        """"""
        self.connect_status = False
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开, 原因{reason}")

        self.login_server()

    def on_error(self, error: dict) -> None:
        """"""
        self.gateway.write_error("交易接口报错", error)

    def on_order_event(self, order: OrderData) -> None:
        """"""
        if order.vt_orderid not in self.orders:
            self.orders[order.vt_orderid] = order
            self.gateway.on_order(order)

            if order.status in [Status.ALLTRADED, Status.PARTTRADED]:
                self.on_trade_event(order)
        else:
            o_order = self.orders.get(order.vt_orderid)
            if o_order.status != order.status:
                self.orders[order.vt_orderid] = order
                self.gateway.on_order(order)
                if order.status in [Status.ALLTRADED, Status.PARTTRADED]:
                    self.on_trade_event(order)

    def on_trade_event(self, order: OrderData) -> None:
        """"""
        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=order.orderid,
            direction=order.direction,
            offset=order.offset,
            price=order.price,
            volume=order.volume,
            datetime=order.datetime,
            gateway_name=self.gateway_name,
        )

        self.gateway.on_trade(trade)

    def on_cancel_event(self, error: dict) -> None:
        """"""
        if not error or not error["error_id"]:
            return

        self.gateway.write_error("撤单失败", error)

    def on_pos_event(self, data: dict) -> None:
        """"""
        position = PositionData(
            symbol=data["code"],
            exchange=EXCHANGE_PT2VT[data["exchange"]],
            direction=Direction.LONG,
            volume=data["volume"],
            frozen=data["volume"] - data["available"],
            price=data["buy_price"],
            pnl=data["profit"],
            yd_volume=data["available"],
            gateway_name=self.gateway_name,
        )
        self.gateway.on_position(position)

    def on_account_event(self, data: dict) -> None:
        """"""
        account = AccountData(accountid=self.token, balance=data["assets"], frozen=data["assets"] - data["available"], gateway_name=self.gateway_name)
        self.gateway.on_account(account)

    def connect(self, user_id: str, server_ip: str, server_port: int,) -> None:
        """"""
        n = self.connect_server(server_ip, server_port)

        if not n:
            self.user_id = user_id
            self.login_server()
        else:
            self.gateway.write_log(n)

    def login_server(self) -> None:
        """"""
        status, data = self.login(self.user_id)

        if status:
            self.login_status = True
            # 账户信息绑定
            self.account_bind(data)
            msg = f"交易服务器登录成功, 账号：{self.token}"
        else:
            msg = f"交易服务器登录失败，原因：{data}"

        self.gateway.write_log(msg)

    def close(self) -> None:
        """"""
        if self.connect_status:
            # self.exit()
            pass

    def send_order(self, req: OrderRequest) -> str:
        """"""
        if not self.login_status:
            self.gateway.write_log(f"委托失败，未登录交易服务器{req.exchange.value}")
            return ""

        if req.exchange not in EXCHANGE_VT2PT:
            self.gateway.write_log(f"委托失败，不支持的交易所{req.exchange.value}")
            return ""

        if req.type not in ORDERTYPE:
            self.gateway.write_log(f"委托失败，不支持的委托类型{req.type.value}")
            return ""

        date, time = datetime.now().strftime("%Y%m%d %H:%S:%M").split(" ")
        pt_req = {
            "code": req.symbol,
            "exchange": EXCHANGE_VT2PT[req.exchange],
            "account_id": self.token,
            "order_price": req.price,
            "volume": int(req.volume),
            "price_type": req.type.value,
            "order_date": date,
            "order_time": time,
        }

        pt_req["order_type"] = DIRECTION_STOCK_VT2PT.get((req.direction, Offset.NONE), "")

        status, data = self.order_send(pt_req)

        if status:
            order = req.create_order_data(str(data.get("order_id", "")), self.gateway_name)
            self.on_order_event(order)
            return order.vt_orderid
        else:
            error = dict()
            error["error_id"] = "pt_api"
            error["error_msg"] = data
            self.gateway.write_error("交易委托失败", error)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        if not self.login_status:
            return

        status, data = self.order_cancel(req.orderid)

        if status:
            self.on_cancel_event({})
        else:
            error = dict()
            error["error_id"] = "pt_api"
            error["error_msg"] = data
            self.on_cancel_event(error)

    def query_account(self) -> None:
        """"""
        if not self.login_status:
            return

        status, data = self.account()
        if status:
            self.on_account_event(data)
        else:
            error = dict()
            error["error_id"] = "pt_api"
            error["error_msg"] = data
            self.on_error(error)

    def query_position(self) -> None:
        """"""
        if not self.login_status:
            return

        status, data = self.pos()
        if status:
            if isinstance(data, list):
                for d in data:
                    self.on_pos_event(d)
        else:
            error = dict()
            error["error_id"] = "pt_api"
            error["error_msg"] = data
            self.on_error(error)

    def query_orders_today(self) -> None:
        """查询当日订单"""
        if not self.login_status:
            return

        status, data = self.orders_today()
        if status:
            if not isinstance(data, str):
                for d in data:
                    direction, offset = DIRECTION_STOCK_PT2VT[d["order_type"]]
                    order = OrderData(
                        symbol=d["code"],
                        exchange=EXCHANGE_PT2VT[d["exchange"]],
                        orderid=d["order_id"],
                        type=ORDERTYPE_PT2VT[d["price_type"]],
                        direction=direction,
                        offset=offset,
                        price=d["order_price"],
                        volume=d["volume"],
                        traded=d["traded"],
                        status=STATUS_PT2VT[d["status"]],
                        datetime=datetime.strptime(d["order_date"] + d["order_time"], "%Y%m%d%H:%M:%S"),
                        gateway_name=self.gateway_name,
                    )
                    self.on_order_event(order)
        else:
            error = dict()
            error["error_id"] = "pt_api"
            error["error_msg"] = data
            self.on_error(error)
