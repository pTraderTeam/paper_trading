from datetime import timedelta
from logging.config import dictConfig

from flask import Flask

from ..config import config
from ..tasks.base import init_tasks
from .views import init_blue


def creat_app(config_name: str, engine):
    __all__ = ["app"]
    # 创建app实例前先配置好日志文件
    dictConfig(config[config_name].LOG_FORMAT)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = config[config_name].SECRET_KEY

    # 设置session的保存时间。
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

    # 注册蓝本
    init_blue(app, engine)
    # 注册定时任务
    init_tasks(app, engine)
    return app
