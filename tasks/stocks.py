import logging

from pymongo import UpdateOne
from pytdx.hq import TdxHq_API

from paper_trading.api.db import MongoDBService
from paper_trading.utility.setting import SETTINGS


def sync_data():
    """将股票列表更新到数据库"""
    host = SETTINGS.get('MONGO_HOST', "localhost")
    port = SETTINGS.get('MONGO_PORT', 27017)
    ms = MongoDBService(host, port)
    ms.connect_db()
    collection = ms.db_client["stocks"]["security"]
    # 模拟交易flask配置参数
    api = TdxHq_API()
    with api.connect(SETTINGS["TDX_HOST"], SETTINGS["TDX_PORT"]):
        for i in range(0, 2):
            n = 0
            batch_list = []
            while True:
                data = api.get_security_list(i, n * 1000)
                if not len(data):
                    logging.warning(f"market[{i}] finished")
                    break
                logging.warning(f"[{n*1000}-{(n+1)*1000}] write to db")
                batch_list.extend(
                    [
                        UpdateOne(
                            {"code": x.get("code"), "market": str(i)},
                            {"$set": dict(x)},
                            upsert=True,
                        )
                        for x in data
                    ]
                )
                n += 1
            if batch_list:
                collection.bulk_write(batch_list, ordered=False)
