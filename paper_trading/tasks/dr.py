from datetime import datetime, timedelta

from pytdx.hq import TdxHq_API

from ..api.db import MongoDBService
from ..utility.setting import SETTINGS
from ..utility.xd import ExDividend


def ex_dividend():
    """除权除息处理"""
    host = SETTINGS.get("MONGO_HOST", "localhost")
    port = SETTINGS.get("MONGO_PORT", 27017)
    ms = MongoDBService(host, port)
    ms.connect_db()
    account_col = ms.db_client[SETTINGS["ACCOUNT_DB"]]
    pos_col = ms.db_client[SETTINGS["POSITION_DB"]]
    # 模拟交易flask配置参数
    api = TdxHq_API()
    search_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    account_list = account_col.list_collection_names()
    with api.connect(SETTINGS["TDX_HOST"], SETTINGS["TDX_PORT"]):
        for account_id in account_list:
            ed = ExDividend(api, search_date)
            pos = list(pos_col[account_id].find({}, {"_id": 0}))
            if ed.is_dr(pos):
                account = account_col[account_id].find_one({}, {"_id": 0})
                dr_pos, _ = ed.dr_cal(pos, account)
                update_account(account_col[account_id], account)
                update_pos(pos_col[account_id], dr_pos)


def update_account(collection, account):
    """更新账户资产信息"""
    collection.update_one({"account_id": account["account_id"]}, {"$set": account})


def update_pos(collection, pos_list):
    """更新持仓"""
    for pos in pos_list:
        collection.update_one({"code": pos["code"]}, {"$set": pos})


if __name__ == "__main__":
    ex_dividend()
