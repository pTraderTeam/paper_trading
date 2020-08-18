import pytest
from pytdx.hq import TdxHq_API

from paper_trading.app import creat_test_app
from paper_trading.utility.constant import ConfigType


@pytest.fixture(scope="session")
def app():
    """初始化测试用的 client"""
    config_name = ConfigType.DEFAULT.value
    app = creat_test_app(config_name)
    ctx = app.app_context()
    ctx.push()
    yield app
    ctx.pop()


@pytest.fixture
def client(request, app):
    app.testing = True
    # 得到测试客户端
    with app.test_client() as test_client:
        yield test_client

    def teardown():
        app.testing = False

    # 执行回收函数
    request.addfinalizer(teardown)

    return client


@pytest.fixture(scope="session")
def tdx_api():
    api = TdxHq_API()
    with api.connect("119.147.212.81", 7709) as tdx:
        yield tdx


