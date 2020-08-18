import copy

import pytest

from paper_trading.utility.xd import ExDividend


class TestExDividend:
    def test_is_dr(self, tdx_api, fake_positions_data):
        # 正向
        xd = ExDividend(tdx_api, "20200817")
        ret_data = xd.is_dr(fake_positions_data)
        assert ret_data is True
        # 反向
        xd = ExDividend(tdx_api, "20200818")
        ret_data = xd.is_dr(fake_positions_data)
        assert ret_data is False

    def test_fmt_dr(self, tdx_api, fake_positions_data):
        xd = ExDividend(tdx_api, "20200817")
        ret_data = xd.fmt_dr(fake_positions_data)
        assert fake_positions_data[-1]["code"] in ret_data.keys()

        xd = ExDividend(tdx_api, "20200818")
        ret_data = xd.fmt_dr(fake_positions_data)
        assert ret_data == {}

    def test_dr_cal(self, tdx_api, fake_positions_data, fake_account_data):
        xd = ExDividend(tdx_api, "20200817")
        pos_data = copy.deepcopy(fake_positions_data)
        account_data = copy.deepcopy(fake_account_data)
        xd.dr_cal(pos_data, account_data)
        assert account_data["available"] == 779713.6

    def test_dr_opt(self, tdx_api, fake_positions_data, fake_account_data):
        xd = ExDividend(tdx_api, "20200817")
        pos_data = copy.deepcopy(fake_positions_data)[-1]
        account_data = copy.deepcopy(fake_account_data)
        xd_data = [
            x
            for x in tdx_api.get_xdxr_info(1, "600372")
            if "20200817" == f"{x['year']}{str(x['month']).zfill(2)}{str(x['day']).zfill(2)}" and x["category"] == 1
        ][0]
        xd.dr_opt(account_data, pos_data, xd_data)
        assert account_data["available"] == 779713.6


@pytest.fixture(scope="session")
def fake_account_data():
    data = {
        "account_id": "JXtGZOLmxpRV05co2rph",
        "assets": 1003235.6,
        "available": 779653.6,
        "market_value": 223582.0,
        "capital": 1000000.0,
        "cost": 0.0003,
        "tax": 0.001,
        "slippoint": 0.03,
        "account_info": "bhy",
    }
    return data


@pytest.fixture(scope="session")
def fake_positions_data():
    data = [
        {
            "code": "600030",
            "exchange": "SH",
            "account_id": "JXtGZOLmxpRV05co2rph",
            "buy_date": "20200807",
            "volume": 1000,
            "available": 1000,
            "buy_price": 31.96,
            "now_price": 32.97,
            "profit": 2050.41,
            "pt_symbol": "600030.SH",
        },
        {
            "code": "600519",
            "exchange": "SH",
            "account_id": "JXtGZOLmxpRV05co2rph",
            "buy_date": "20200817",
            "volume": 100,
            "available": 100,
            "buy_price": 1690.0,
            "now_price": 1703.12,
            "profit": 1261.3,
            "pt_symbol": "600519.SH",
        },
        {
            "code": "600372",
            "exchange": "SH",
            "account_id": "JXtGZOLmxpRV05co2rph",
            "buy_date": "20200818",
            "volume": 1000,
            "available": 1000,
            "buy_price": 20.37,
            "now_price": 20.3,
            "profit": -76.11,
            "pt_symbol": "600372.SH",
        },
    ]
    return data
