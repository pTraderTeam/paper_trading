# Paper Trading
专注于证券市场模拟交易后台服务，交易市场可扩展。

Paper Trading项目已经由pTraderTeam小组来维护

![image](https://github.com/pTraderTeam/paper_trading/blob/master/data_flow.jpg)


## 系统安装

### 安装Python

至少Python3.7以上

### 安装mongodb

安装好之后，将mongodb服务开启

### 安装
```shell script
pip install paper_trading
```

## 配置

### 配置你的setting.py

setting.py 包括了所有模拟交易程序的运行参数。你要自己考虑需要什么样的模拟交易程序

### 配置flask app

根据你的需要修改run.py中的IP地址和端口，及调试模式

## 使用
```shell script
python run.py test # 开启回测模式
python run.py dev  # 开启开发模式
python run.py      # 开启模拟交易模式
```
开始模拟交易吧

## 接口
flask app 只提供了模拟交易服务的接口，需要你自己向这个接口发送不同的请求。
你可以自己用requests或者其他工具写一个url请求模块，把server.py中的接口都封装一下，或者直接使用exampe。
把example文件夹中的pt_api.py文件放入你的量化交易程序，在引入相关函数后，你就可以使用模拟交易程序的功能了。

### 对接vnpy
使用vnpy的朋友们，你们可以使用example中的vnpy接口包，按照文件夹的名字放入vnpy的api和gateway目录中，修改vnpy的启动文件代码，加载paper_trading的接口，开始你的模拟交易吧

## 开发环境

### 安装
```shell script
poetry install
pre-commit install
```

### 打包
```shell script
poetry build
```
### 发布
```shell script
poetry publish
```

## 各模块功能

* paper_trading
    > 主程序目录
    * api

      > 模拟交易程序使用到的api

      * db.py

        > mongodb数据服务类

      * pytdx_api.py

        > 封装了pytdx的行情服务模块，主要用来获取市场实时行情

      * tushare_api.py

        > 封装了tushare的行情服务模块，主要用来获取市场实时行情

* app

  > 使用flask为模拟交易程序提供网络接口

* event

  > 事件引擎类，直接使用了VNPY中的事件引擎类

* trade

  > 核心模块
  * account.py

    > 交易员类与各种数据接口生成器

  * account_engine.py

    > 账户引擎，与账户、持仓、交易记录、订单有关的所有功能

  * data_center.py

    > 数据中心，包含web功能常用的行情数据

  * market.py

    > 交易市场类，里面包含了两种撮合成交的模式，注意根据你的使用需求进行配置

  * pt_engine.py

    > 程序主引擎

  * db_model.py

    > 数据库相关操作函数集合

* utility

  > 工具箱
  * constant.py

    > 常量类，所有的常量都在这里

  * errors.py

    > 错误类，继承自Exception

  * event.py

    > 事件引擎使用的所有事件类型

  * model.py

    > 数据模型类

  * setting.py

    > 程序设置

* docs

  > 接口说明文档

  > 项目更新日志

* example

  > pt_api.py 已经包括了对flask 服务的封装，你可以将pt_api.py放到你的量化交易程序中，import你需要的函数进行使用

  > vnpy 作为vnpy的接口使用，复制文件到vnpy项目对应的文件夹

* tests
  测试代码，目前为空


## 项目参考

[https://github.com/pTraderTeam/paper_trading]
