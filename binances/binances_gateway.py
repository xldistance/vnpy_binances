import csv
import hashlib
import hmac
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from threading import Lock
from time import sleep, time
import re
from typing import Any, Callable, Dict, List, Sequence, Tuple,Union
from urllib.parse import urlencode

from peewee import chunked
from requests.exceptions import SSLError
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import binance_account_main  # 导入账户字典
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    delete_dr_data,
    extract_vt_symbol,
    get_folder_path,
    get_local_datetime,
    is_target_contract,
    load_json,
    remain_alpha,
    save_json,
    error_monitor
)

REST_HOST: str = "https://fapi.binance.com"
WEBSOCKET_PUBLIC_HOST: str = "wss://fstream.binance.com/public/stream"
WEBSOCKET_MARKET_HOST: str = "wss://fstream.binance.com/market/stream"
WEBSOCKET_PRIVATE_HOST: str = "wss://fstream.binance.com/private/ws"
PRIVATE_WEBSOCKET_EVENTS: str = "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"

TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"
TESTNET_WEBSOCKET_PUBLIC_HOST: str = "wss://fstream.binancefuture.com/public/stream"
TESTNET_WEBSOCKET_MARKET_HOST: str = "wss://fstream.binancefuture.com/market/stream"
TESTNET_WEBSOCKET_PRIVATE_HOST: str = "wss://fstream.binancefuture.com/private/ws"

STATUS_BINANCES2VT: Dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED,
}

ORDERTYPE_VT2BINANCES: Dict[OrderType, Tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", ""),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_BINANCES2VT: Dict[Tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCES.items()}
ORDERTYPE_BINANCES2VT[("MARKET", "GTC")] = OrderType.MARKET

DIRECTION_VT2BINANCES: Dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}
DIRECTION_BINANCES2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCES.items()}

INTERVAL_VT2BINANCES: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinancesGateway(BaseGateway):
    """
    * 币安正向永续接口(下单数量：币)
    * 只支持单向持仓,全仓模式
    """

    # default_setting由vnpy.trader.ui.widget调用
    default_setting = {
        "key": "",
        "secret": "",
        "server": ["TESTNET", "REAL"],
        "host": "",
        "port": 0,
    }

    exchanges: List[Exchange] = [Exchange.BINANCES]  # 由main_engine add_gateway调用
    get_file_path = GetFilePath()
    # -------------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine):
        """ """
        super().__init__(event_engine, "BINANCES")
        self.orders: Dict[str, OrderData] = {}
        self.trade_ws_api = BinancesTradeWebsocketApi(self)
        self.market_ws_api = BinancesDataWebsocketApi(self)
        self.rest_api = BinancesRestApi(self)

        # 所有合约列表
        self.recording_list = self.get_file_path.recording_list
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 查询历史数据合约列表
        self.history_contract = copy(self.recording_list)
        self.leverage_contract = copy(self.recording_list)
        self.query_count = 0
        self.query_functions = [self.query_account, self.query_position, self.query_order]
        # 下载历史数据状态
        self.history_status: bool = True
        # 订阅成交数据状态
        self.book_trade_status: bool = False
    # -------------------------------------------------------------------------------------------------------
    def connect(self, log_account: dict = {}):
        """ """
        if not log_account:
            log_account = binance_account_main
        key = log_account["key"]
        secret = log_account["secret"]
        server = log_account["server"]
        proxy_host = log_account["host"]
        proxy_port = log_account["port"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.market_ws_api.connect(proxy_host, proxy_port, server)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TIMER, self.query_rest_data)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
    # -------------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """ """
        self.market_ws_api.subscribe(req)
    # -------------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """ """
        return self.rest_api.send_order(req)
    # -------------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> Request:
        """ """
        self.rest_api.cancel_order(req)
    # -------------------------------------------------------------------------------------------------------
    def query_position(self):
        self.rest_api.query_position()
    # -------------------------------------------------------------------------------------------------------
    def query_account(self):
        self.rest_api.query_account()
    # -------------------------------------------------------------------------------------------------------
    def query_order(self):
        self.rest_api.query_order()
    # -------------------------------------------------------------------------------------------------------
    def query_rest_data(self, event: Event) -> None:
        """
        rest api定时查询账户,委托单和持仓
        """
        self.query_count += 1
        if self.query_count < 5:
            return
        self.query_count = 0
        self.rest_api.query_time()
        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)
    # -------------------------------------------------------------------------------------------------------
    def query_history(self, event: Event):
        """
        查询合约历史数据
        """
        if not self.history_contract:
            return
        symbol, exchange, gateway_name = extract_vt_symbol(self.history_contract.pop(0))
        req = HistoryRequest(
            symbol=symbol,
            exchange=Exchange(exchange),
            interval=Interval.MINUTE,
            start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
            end=datetime.now(TZ_INFO),
            #start=datetime(2026,1,31,tzinfo=TZ_INFO),
            #end=datetime(2026,4,16,tzinfo=TZ_INFO),
            gateway_name=self.gateway_name,
        )
        self.rest_api.query_history(req)
    # -------------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        收到委托单推送，BaseGateway推送数据
        """
        self.orders[order.vt_orderid] = copy(order)
        super().on_order(order)
    # -------------------------------------------------------------------------------------------------------
    def get_order(self, vt_orderid: str) -> OrderData:
        """
        用vt_orderid获取委托单数据
        """
        return self.orders.get(vt_orderid, None)
    # -------------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭接口
        """
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()
    # -------------------------------------------------------------------------------------------------------
    def process_timer_event(self, event: Event) -> None:
        """
        处理定时任务
        """
        self.rest_api.keep_user_stream()
        if not self.leverage_contract:
            return
        symbol, exchange, gateway_name = extract_vt_symbol(self.leverage_contract.pop(0))
        self.rest_api.set_leverage(symbol)
# -------------------------------------------------------------------------------------------------------
class BinancesRestApi(RestClient):
    """
    BINANCE REST API
    """
    # -------------------------------------------------------------------------------------------------------
    def __init__(self, gateway: BinancesGateway):
        """ """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinancesTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.count_datetime: int = 0
        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
    # -------------------------------------------------------------------------------------------------------
    def get_timestamp(self):
        """
        获取时间戳
        """
        timestamp = int(time() * 1000)
        if self.time_offset > 0:
            timestamp -= abs(self.time_offset)
        elif self.time_offset < 0:
            timestamp += abs(self.time_offset)
        return timestamp
    # -------------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        """
        生成币安接口签名
        """
        security = request.data.get("security", None)
        if security == Security.NONE:
            request.data = None
            return request

        # 初始化 path
        path = request.path + "?" + urlencode(request.params) if request.params else request.path
        # params None值改为空字典
        if not request.params:
            request.params = {}
        if security == Security.SIGNED:
            timestamp = self.get_timestamp()
            recv_window = self.recv_window
            request.params.update({"timestamp": timestamp, "recvWindow": recv_window})
            query = urlencode(sorted(request.params.items()))
            signature = hmac.new(self.secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
            query += "&signature={}".format(signature)
            path = request.path + "?" + query

        request.path = path
        request.data = {}
        request.params = {}
        # 设置 headers
        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json", "X-MBX-APIKEY": self.key}
        return request
    # -------------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, server: str, proxy_host: str, proxy_port: int) -> None:
        """
        初始化REST服务连接
        """
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)

        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")

        self.query_contract()
        self.set_position_side()
        self.start_user_stream()
    # -------------------------------------------------------------------------------------------------------
    def query_time(self):
        """
        查询交易所时间
        """
        data = {"security": Security.NONE}
        path = "/fapi/v1/time"

        self.add_request(
            "GET", path, callback=self.on_query_time, data=data
        )  
    # -------------------------------------------------------------------------------------------------------
    def set_leverage(self, symbol: str):
        """
        设置合约杠杆
        """
        data = {"security": Security.SIGNED}
        params = {"symbol": symbol, "leverage": 10}
        self.add_request(
            method="POST", path="/fapi/v1/leverage", callback=self.on_leverage, on_failed=self.on_leverage_failed, data=data, params=params, extra=params
        )
    # -------------------------------------------------------------------------------------------------------
    def on_leverage(self, data, request: Request):
        """
        收到杠杆数据回调
        """
        pass
    # -------------------------------------------------------------------------------------------------------
    def on_leverage_failed(self, status_code, request: Request):
        """
        收到设置杠杆失败回调
        """
        error_code: int = request.response.json()["code"]
        symbol: str = request.extra["symbol"]
        # -1122错误设置杠杆合约为过期合约，从dr_data中删除
        if error_code == -1122:
            delete_dr_data(symbol, self.gateway_name)
    # -------------------------------------------------------------------------------------------------------
    def set_position_side(self):
        """
        设置持仓模式
        """
        data = {"security": Security.SIGNED}
        # "true": 双向持仓模式；"false": 单向持仓模式
        params = {"dualSidePosition": "false"}
        self.add_request(method="POST", path="/fapi/v1/positionSide/dual", callback=self.on_position_side, data=data, params=params)
    # -------------------------------------------------------------------------------------------------------
    def on_position_side(self, data, request: Request):
        pass
    # -------------------------------------------------------------------------------------------------------
    def query_account(self):
        """
        查询账户数据
        """
        data = {"security": Security.SIGNED}

        self.add_request(method="GET", path="/fapi/v3/account", callback=self.on_query_account, data=data,on_failed = self.on_account_failed)
    # -------------------------------------------------------------------------------------------------------
    def on_account_failed(self,status_code:int,request: Request):
        """
        """
        data = request.response.json()
        error_code = data["code"]
        request_ip = re.search(r"request ip:\s*([\d.]+)", data["msg"]).group(1)
        # 本地ip不在api 绑定ip名单中，发送错误信息到钉钉
        if error_code == -2015:
            error_monitor.send_text(f"交易接口：{self.gateway_name}，本地ip地址：{request_ip}，不在api白名单中，请添加ip地址到api白名单")
            # 重置last_text，允许重复发送重要消息
            error_monitor.last_text = ""
    # -------------------------------------------------------------------------------------------------------
    def query_position(self):
        """
        查询仓位
        """
        data = {"security": Security.SIGNED}

        self.add_request(method="GET", path="/fapi/v3/positionRisk", callback=self.on_query_position, data=data)
    # -------------------------------------------------------------------------------------------------------
    def query_order(self):
        """
        查询活动委托单
        """
        data = {"security": Security.SIGNED}

        self.add_request(method="GET", path="/fapi/v1/openOrders", callback=self.on_query_order, data=data)
    # -------------------------------------------------------------------------------------------------------
    def query_contract(self) -> Request:
        """
        查询合约
        """
        data = {"security": Security.NONE}
        self.add_request(method="GET", path="/fapi/v1/exchangeInfo", callback=self.on_query_contract, data=data)
    # -------------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        返回加线程锁的order_count
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # -------------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        self.count_datetime = int(datetime.now(TZ_INFO).strftime("%Y%m%d%H%M%S"))

        orderid = req.symbol + "-" + str(self.count_datetime) + str(self._new_order_id()).rjust(8,"0")
        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = datetime.now(TZ_INFO)
        self.gateway.on_order(order)

        data = {"security": Security.SIGNED}

        order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
        params = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINANCES[req.direction],
            "type": order_type,
            "quantity": float(req.volume),
            "newClientOrderId": orderid
            }
        # 非市价单才有price，timeInForce参数
        if time_condition:
            params["price"] = float(req.price)
            params["timeInForce"] = time_condition

        if req.offset == Offset.CLOSE:
            params["reduceOnly"] = True

        self.add_request(
            method="POST",
            path="/fapi/v1/order",
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )

        return order.vt_orderid
    # -------------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        order: OrderData = self.gateway.get_order(req.vt_orderid)
        data = {"security": Security.SIGNED}

        params = {"symbol": req.symbol, "origClientOrderId": req.orderid}

        self.add_request(
            method="DELETE", path="/fapi/v1/order", callback=self.on_cancel_order, params=params, data=data, on_failed=self.on_cancel_failed, extra=order
        )
    # -------------------------------------------------------------------------------------------------------
    def start_user_stream(self) -> Request:
        """
        生成listenKey
        """
        data = {"security": Security.API_KEY}

        self.add_request(method="POST", path="/fapi/v1/listenKey", callback=self.on_start_user_stream, data=data)
    # -------------------------------------------------------------------------------------------------------
    def keep_user_stream(self) -> Request:
        """
        保持listenKey连接
        """
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0
        data = {"security": Security.API_KEY}

        params = {"listenKey": self.user_stream_key}

        self.add_request(
            method="PUT", path="/fapi/v1/listenKey", callback=self.on_keep_user_stream, params=params, data=data, on_error=self.on_keep_user_stream_error
        )
    # -------------------------------------------------------------------------------------------------------
    def on_keep_user_stream_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        收到keep_user_stream错误回报
        """
        # 处理非超时错误
        if not issubclass(exception_type, TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)
    # -------------------------------------------------------------------------------------------------------
    def on_query_time(self, data, request: Request) -> None:
        """
        收到交易所时间推送
        """
        local_time = int(time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time
    # -------------------------------------------------------------------------------------------------------
    def on_query_account(self, data, request: Request) -> None:
        """
        收到账户数据推送
        """
        for asset in data["assets"]:
            account = AccountData(
                accountid=asset["asset"] + "_" + self.gateway_name,
                balance=float(asset["walletBalance"]),
                frozen=float(asset["maintMargin"]),
                margin=float(asset["positionInitialMargin"]),
                position_profit=float(asset["unrealizedProfit"]),
                available=float(asset["availableBalance"]),
                datetime=datetime.now(TZ_INFO),
                file_name=self.gateway.account_file_name,
                gateway_name=self.gateway_name,
            )

            if account.balance:
                self.gateway.on_account(account)
                self.accounts_info[account.accountid] = account.__dict__
        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = self.gateway.get_file_path.account_path(self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # -------------------------------------------------------------------------------------------------------
    def on_query_position(self, data: Union[dict,list], request: Request) -> None:
        """
        处理查询持仓数据的响应
        """
        # 不在持仓推送列表中的symbol持仓赋值为0
        holding_symbols = [item["symbol"] for item in data]

        for symbol in list(self.gateway.market_ws_api.ticks):
            if symbol not in holding_symbols:
                # 生成多空持仓数据
                self.create_position_pair(
                    symbol=symbol,
                    exchange=Exchange.BINANCES,
                    volume=0,
                    avg_price=0,
                    unrealized_pnl=0
                )

        # 处理持仓推送数据
        for pos_data in data:
            symbol = pos_data["symbol"]
            volume = float(pos_data["positionAmt"])
            avg_price = float(pos_data["entryPrice"])
            unrealized_pnl = float(pos_data["unRealizedProfit"])
            
            # 生成多空持仓数据
            self.create_position_pair(
                symbol=symbol,
                exchange=Exchange.BINANCES,
                volume=volume,
                avg_price=avg_price,
                unrealized_pnl=unrealized_pnl
            )
    # -------------------------------------------------------------------------------------------------------
    def create_position_pair(self, symbol: str, exchange: Exchange, volume: float, avg_price: float, unrealized_pnl: float):
        """
        创建双向持仓数据对
        """
        direction = Direction.LONG if volume >= 0 else Direction.SHORT
        
        # 创建持仓对象
        position_1 = PositionData(
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            volume=abs(volume),
            price=avg_price,
            pnl=unrealized_pnl,
            gateway_name=self.gateway_name,
        )
        
        # 创建对立持仓对象
        position_2 = PositionData(
            symbol=symbol,
            exchange=exchange,
            direction=OPPOSITE_DIRECTION[direction],
            volume=0,
            price=0,
            pnl=0,
            gateway_name=self.gateway_name,
        )
        
        self.gateway.on_position(position_1)
        self.gateway.on_position(position_2)
    # -------------------------------------------------------------------------------------------------------
    def on_query_order(self, data, request: Request) -> None:
        """
        收到委托单查询回报
        """
        for raw_data in data:
            order_datetime = get_local_datetime(raw_data["time"] / 1000)
            order_type = ORDERTYPE_BINANCES2VT.get((raw_data["type"], raw_data["timeInForce"]), None)
            if not order_type:
                continue
            order = OrderData(
                orderid=raw_data["clientOrderId"],
                symbol=raw_data["symbol"],
                exchange=Exchange.BINANCES,
                price=float(raw_data["price"]),
                volume=float(raw_data["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCES2VT[raw_data["side"]],
                traded=float(raw_data["executedQty"]),
                status=STATUS_BINANCES2VT.get(raw_data["status"], None),
                datetime=order_datetime,
                gateway_name=self.gateway_name,
            )
            if raw_data["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    # -------------------------------------------------------------------------------------------------------
    def on_query_contract(self, data, request: Request) -> None:
        """
        收到合约数据推送
        """
        for raw_data in data["symbols"]:
            min_volume: float = 0.0
            max_volume: float = 0.0
            for filters in raw_data["filters"]:
                if filters["filterType"] == "PRICE_FILTER":
                    price_tick = float(filters["tickSize"])
                elif filters["filterType"] == "LOT_SIZE":
                    min_volume = float(filters["minQty"])
                    max_volume = float(filters["maxQty"])

            contract = ContractData(
                symbol=raw_data["symbol"],
                exchange=Exchange.BINANCES,
                name=remain_alpha(raw_data["symbol"]),
                price_tick=price_tick,
                size=20,  # 默认杠杆
                min_volume=min_volume,
                max_volume=max_volume,
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    # -------------------------------------------------------------------------------------------------------
    def on_send_order(self, data, request: Request) -> None:
        """ """
        pass
    # -------------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code, request: Request) -> None:
        """
        收到发送委托单失败回报
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}，错误委托单：{order}"
        self.gateway.write_log(msg)
    # -------------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        收到发送委托单错误回报
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)
    # -------------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data, request: Request) -> None:
        """
        收到取消委托单回报
        """
        pass
    # -------------------------------------------------------------------------------------------------------
    def on_cancel_failed(self, status_code, request: Request) -> None:
        """
        收到取消委托单失败回报
        """
        if not request.extra:
            return
        order: OrderData = request.extra
        order.status = Status.REJECTED
        error_code = str(request.response.json()["code"])
        # 仅当错误码为-2011时设置reject_code
        if error_code == "-2011":
            order.reject_code = error_code
        self.gateway.on_order(order)

        msg = f"撤单失败，状态码：{status_code}，错误信息：{request.response.text}"
        self.gateway.write_log(msg)
    # -------------------------------------------------------------------------------------------------------
    def on_start_user_stream(self, data, request: Request) -> None:
        """
        收到user_stream回报
        """
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = f"{WEBSOCKET_PRIVATE_HOST}?listenKey={self.user_stream_key}&events={PRIVATE_WEBSOCKET_EVENTS}"
        else:
            url = f"{TESTNET_WEBSOCKET_PRIVATE_HOST}?listenKey={self.user_stream_key}&events={PRIVATE_WEBSOCKET_EVENTS}"

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)
    # -------------------------------------------------------------------------------------------------------
    def on_keep_user_stream(self, data, request: Request) -> None:
        """ """
        pass
    # -------------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest):
        """
        查询历史数据
        """
        history = []
        limit = 200
        start_time = req.start
        time_consuming_start = time()

        while start_time < req.end:
            end_time = min(start_time + timedelta(minutes=limit), req.end)
            params = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit,
                "startTime": int(start_time.timestamp() * 1000),  # 转换成毫秒
                "endTime": int(end_time.timestamp() * 1000),      # 转换成毫秒
            }

            resp = self.request("GET", "/fapi/v1/klines", data={"security": Security.NONE}, params=params)
            if not resp or resp.status_code // 100 != 2:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{getattr(resp, 'status_code', '无响应')}，信息：{getattr(resp, 'text', '')}"
                self.gateway.write_log(msg)
                break

            data = resp.json()
            if not data:
                msg = f"标的：{req.vt_symbol}获取历史数据为空，开始时间：{start_time}"
                self.gateway.write_log(msg)
                break
            buf = [BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=get_local_datetime(raw[0]),
                interval=req.interval,
                volume=float(raw[5]),
                open_price=float(raw[1]),
                high_price=float(raw[2]),
                low_price=float(raw[3]),
                close_price=float(raw[4]),
                gateway_name=self.gateway_name,
            ) for raw in data]

            history.extend(buf)
            start_time = buf[-1].datetime + TIMEDELTA_MAP[req.interval]  # 更新开始时间

        if history:
            try:
                database_manager.save_bar_data(history, False)  # 保存数据到数据库
                time_consuming_end = time()
                query_time = round(time_consuming_end - time_consuming_start, 3)
                msg = f"载入{req.vt_symbol}: bar数据，开始时间：{history[0].datetime}，结束时间：{history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
                self.gateway.write_log(msg)
            except Exception as err:
                self.gateway.write_log(f"{err}")
        else:
            msg = f"未获取到标的：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            delete_dr_data(req.symbol, self.gateway_name)
# -------------------------------------------------------------------------------------------------------
class BinancesTradeWebsocketApi(WebsocketClient):
    """ """
    # -------------------------------------------------------------------------------------------------------
    def __init__(self, gateway: BinancesGateway):
        """ """
        super().__init__()

        self.gateway: BinancesGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.func_map = {
            "ACCOUNT_UPDATE": self.on_position,
            "ORDER_TRADE_UPDATE": self.on_order
        }
    # -------------------------------------------------------------------------------------------------------
    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """ """
        self.init(url, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # -------------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket 交易API连接成功")
    # -------------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket 交易API连接断开")
    # -------------------------------------------------------------------------------------------------------
    def on_packet(self, packet: dict) -> None:
        """
        """
        event_type = packet["e"]
        func = self.func_map.get(event_type)
        if func:
            func(packet)
        elif event_type not in {"ACCOUNT_CONFIG_UPDATE", "listenKeyExpired", "TRADE_LITE"}:
            # 过滤收到账户杠杆调整回报，listenKey过期回报，过滤精简交易回报
            self.gateway.write_log(f"交易接口：{self.gateway_name}，未处理的WS私有回报：{packet}")
    # -------------------------------------------------------------------------------------------------------
    def on_position(self, packet) -> None:
        """
        收到持仓数据推送
        """
        for pos_data in packet["a"]["P"]:
            symbol = pos_data["s"]
            volume = float(pos_data["pa"])
            direction = Direction.LONG if volume >= 0 else Direction.SHORT
            position_1 = PositionData(
                symbol=symbol,
                exchange=Exchange.BINANCES,
                direction=direction,
                volume=abs(volume),
                price=float(pos_data["ep"]),
                pnl=float(pos_data["cr"]),
                gateway_name=self.gateway_name,
            )
            position_2 = PositionData(
                symbol=symbol,
                exchange=Exchange.BINANCES,
                direction=OPPOSITE_DIRECTION[direction],
                volume=0,
                price=0,
                pnl=0,  # 持仓盈亏
                frozen=0,  # 持仓冻结保证金
                gateway_name=self.gateway_name,
            )

            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)
    # -------------------------------------------------------------------------------------------------------
    def on_order(self, packet) -> None:
        """
        收到委托数据推送
        """
        order_datetime = get_local_datetime(packet["E"] / 1000)  # 委托单时间
        order_data = packet["o"]
        order_type = ORDERTYPE_BINANCES2VT.get((order_data["o"], order_data["f"]), None)
        if not order_type:
            return
        order = OrderData(
            symbol=order_data["s"],
            exchange=Exchange.BINANCES,
            orderid=str(order_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[order_data["S"]],
            price=float(order_data["p"]),
            volume=float(order_data["q"]),
            traded=float(order_data["z"]),
            status=STATUS_BINANCES2VT[order_data["X"]],
            datetime=order_datetime,
            gateway_name=self.gateway_name,
        )
        # 只减仓单设置方向为平仓
        if order_data["R"]:
            order.offset = Offset.CLOSE
        self.gateway.on_order(order)

        # 推送成交事件
        trade_volume = float(order_data["l"])
        if not trade_volume:
            return

        trade_dt = get_local_datetime(order_data["T"] / 1000)  # 成交时间

        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=order_data["t"],
            direction=order.direction,
            offset=order.offset,
            price=float(order_data["L"]),
            volume=trade_volume,
            datetime=trade_dt,
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)
# -------------------------------------------------------------------------------------------------------
class BinancesDataWebsocketApi:
    """统一管理 Public/Market 两条行情连接。"""

    def __init__(self, gateway: BinancesGateway):
        """ """
        self.gateway: BinancesGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.public_ws_api = BinancesPublicWebsocketApi(self)
        self.market_ws_api = BinancesMarketWebsocketApi(self)
    # -------------------------------------------------------------------------------------------------------
    def connect(self, proxy_host: str, proxy_port: int, server: str) -> None:
        """分别连接 Public 和 Market 两类公共数据入口。"""
        self.public_ws_api.connect(proxy_host, proxy_port, server)
        self.market_ws_api.connect(proxy_host, proxy_port, server)
    # -------------------------------------------------------------------------------------------------------
    def stop(self) -> None:
        """关闭所有公共行情连接。"""
        self.public_ws_api.stop()
        self.market_ws_api.stop()
    # -------------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        self.subscribed[req.vt_symbol] = req
        self.init_tick(req)
        self.public_ws_api.subscribe(req)
        self.market_ws_api.subscribe(req)
    # -------------------------------------------------------------------------------------------------------
    def init_tick(self, req: SubscribeRequest) -> TickData:
        """初始化tick"""
        tick = self.ticks.get(req.symbol, None)
        if not tick:
            tick = TickData(
                symbol=req.symbol,
                name=remain_alpha(req.symbol),
                exchange=Exchange.BINANCES,
                datetime=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.ticks[req.symbol] = tick
    # -------------------------------------------------------------------------------------------------------
    def on_tick(self, data: dict):
        """
        收到tick数据推送
        """
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.volume = float(data["v"])      # 近24小时成交量币的数量
        tick.open_price = float(data["o"])
        tick.high_price = float(data["h"])
        tick.low_price = float(data["l"])
        tick.last_price = float(data["c"])
        tick.datetime = get_local_datetime(int(data["E"]))
    # -------------------------------------------------------------------------------------------------------
    def on_depth(self, data: dict) -> None:
        """
        收到深度数据推送
        """
        symbol = data["s"]
        tick = self.ticks[symbol]
        bids = data["b"]
        asks = data["a"]
        # 提取前5个最佳买入价格和量，并为tick对象设置属性
        bid_data = bids[:min(len(bids), 5)]
        for idx, (price, volume) in enumerate(bid_data, start=1):
            attr_name = f"bid_price_{idx}"
            attr_name_volume = f"bid_volume_{idx}"
            tick.__setattr__(attr_name, float(price))
            tick.__setattr__(attr_name_volume, float(volume))

        # 提取前5个最佳卖出价格和量，并为tick对象设置属性
        ask_data = asks[:min(len(asks), 5)]
        for idx, (price, volume) in enumerate(ask_data, start=1):
            attr_name = f"ask_price_{idx}"
            attr_name_volume = f"ask_volume_{idx}"
            tick.__setattr__(attr_name, float(price))
            tick.__setattr__(attr_name_volume, float(volume))
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # -------------------------------------------------------------------------------------------------------
    def on_book_ticker(self, data: dict) -> None:
        """
        收到逐笔一档深度数据推送
        """
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(int(data["E"]))
        tick.bid_price_1, tick.bid_volume_1 = float(data["b"]), float(data["B"])
        tick.ask_price_1, tick.ask_volume_1 = float(data["a"]), float(data["A"])
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # -------------------------------------------------------------------------------------------------------
    def on_book_trade(self, data: dict) -> None:
        """
        收到逐笔成交数据推送
        """
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(int(data["T"]))
        tick.last_price = float(data["p"])


class BinancesDataWebsocketBase(WebsocketClient):
    """Binance 公共行情 websocket 基类。"""

    def __init__(self, api: BinancesDataWebsocketApi):
        """ """
        super().__init__()

        self.api: BinancesDataWebsocketApi = api
        self.gateway: BinancesGateway = api.gateway
        self.gateway_name: str = api.gateway_name

        self.reqid: int = 0
        self.connection_status: bool = False
    # -------------------------------------------------------------------------------------------------------
    def connect(self, proxy_host: str, proxy_port: int, server: str) -> None:
        """按环境连接对应 websocket 入口。"""
        if server == "REAL":
            host = self.get_real_host()
        else:
            host = self.get_testnet_host()

        self.init(host, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # -------------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """ """
        self.connection_status = True
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket {self.get_api_label()} API连接成功")
        for req in list(self.api.subscribed.values()):
            self.subscribe(req)
    # -------------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """ """
        self.connection_status = False
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket {self.get_api_label()} API连接断开")
    # -------------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅当前连接负责的流。"""
        params = self.get_subscribe_params(req)
        if not params:
            return

        while True:
            if not self.connection_status:
                sleep(1)
            else:
                break

        self.reqid += 1
        reqs: dict = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": self.reqid,
        }
        self.send_packet(reqs)
        # 间隔200ms订阅一次，防止触发交易接口限制
        sleep(0.2)
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_api_label() -> str:
        raise NotImplementedError
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_real_host() -> str:
        raise NotImplementedError
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_testnet_host() -> str:
        raise NotImplementedError
    # -------------------------------------------------------------------------------------------------------
    def get_subscribe_params(self, req: SubscribeRequest) -> List[str]:
        raise NotImplementedError


class BinancesPublicWebsocketApi(BinancesDataWebsocketBase):
    """Public 高频公共行情/盘口 websocket。"""

    @staticmethod
    def get_api_label() -> str:
        return "Public行情"
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_real_host() -> str:
        return WEBSOCKET_PUBLIC_HOST
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_testnet_host() -> str:
        return TESTNET_WEBSOCKET_PUBLIC_HOST
    # -------------------------------------------------------------------------------------------------------
    def get_subscribe_params(self, req: SubscribeRequest) -> List[str]:
        symbol_lower = req.symbol.lower()
        return [
            symbol_lower + "@depth5@100ms",
            # symbol_lower + "@bookTicker",  # 逐笔一档深度(订阅该主题ws行情会不断断开重连，暂不使用)
        ]
    # -------------------------------------------------------------------------------------------------------
    def on_packet(self, packet: dict) -> None:
        """处理 Public 入口的组合行情推送。"""
        stream = packet.get("stream", None)
        if not stream:
            return

        channel = stream.split("@")[1]
        data = packet["data"]
        if channel.startswith("depth"):
            self.api.on_depth(data)
        elif channel == "bookTicker":
            self.api.on_book_ticker(data)


class BinancesMarketWebsocketApi(BinancesDataWebsocketBase):
    """Market 常规公共市场数据 websocket。"""

    @staticmethod
    def get_api_label() -> str:
        return "Market行情"
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_real_host() -> str:
        return WEBSOCKET_MARKET_HOST
    # -------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_testnet_host() -> str:
        return TESTNET_WEBSOCKET_MARKET_HOST
    # -------------------------------------------------------------------------------------------------------
    def get_subscribe_params(self, req: SubscribeRequest) -> List[str]:
        symbol_lower = req.symbol.lower()
        params: List[str] = [symbol_lower + "@miniTicker"]
        if self.gateway.book_trade_status:
            # 币安最新公告中成交类公共市场数据归属 Market 入口
            params.append(symbol_lower + "@aggTrade")
        return params
    # -------------------------------------------------------------------------------------------------------
    def on_packet(self, packet: dict) -> None:
        """处理 Market 入口的组合行情推送。"""
        stream = packet.get("stream", None)
        if not stream:
            return

        channel = stream.split("@")[1]
        data = packet["data"]
        if channel == "miniTicker":
            self.api.on_tick(data)
        elif channel == "aggTrade":
            self.api.on_book_trade(data)
