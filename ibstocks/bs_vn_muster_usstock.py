import multiprocessing
import sys
from time import sleep
from datetime import datetime, time, timedelta, timezone
from logging import INFO

from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine

#from vnpy.gateway.ctp import CtpGateway
#from vnpy.gateway.xtp import XtpGateway
from vnpy.gateway.ib import IbGateway
from vnpy.app.cta_strategy import CtaEngine
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG

from vnpy.trader.constant import Exchange
from vnpy.trader.object import SubscribeRequest

import winsound
import bs_vn_base as bs

SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

#--------------------Settings-------------------------
#TODO-01: CTA策略信息设置 - 步步高对冲止盈
class_name = "BubugaoZZ_short"
strategy_name = "bbg_zz_short_0922_AAPL" # specific instance
vt_symbol = "120549942.SMART" #

strategy_setting = {
    "zz_count_max": 1,
    "above_zz_1": 0.0,
    "duobeili_threshold": 1.005, # necessary
    "init_long_pos": 3, # necessary
    "cover_before_close": True,
    "last_15mins_bar_index": 375, # necessary
    "backtest_flag": False
}
#-----------------------------------------------------

def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(IbGateway)
    main_engine.add_app(CtaStrategyApp)
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    main_engine.connect(bs.ib_setting, "IB")
    main_engine.write_log("连接IB接口")

    sleep(10)

    # 订阅行情(IB必须手动订阅)
    req1 = SubscribeRequest("120549942",Exchange.SMART) #创建行情订阅
    #req1 = SubscribeRequest("12087792",Exchange.IDEALPRO) #创建行情订阅
    main_engine.subscribe(req1,"IB")

    # 创建CTA策略引擎
    cta_engine = CtaEngine(main_engine, event_engine)

    # 初始化CTA策略引擎, 会依次调用init_rqdata(), load_strategy_class()等函数
    cta_engine.init_engine()

    # 创建属于我们自己的策略，首次创建成功后会将参数写入到C:\Users\Administrator\.vntrader文件夹下的cta_strategy_setting.json文件内
    if strategy_name not in cta_engine.strategies:
        main_engine.write_log(f"创建{strategy_name}策略")
        cta_engine.add_strategy(class_name, strategy_name, vt_symbol, strategy_setting)
    else:
        cta_engine.update_strategy_setting(strategy_name, strategy_setting)

    # 初始化刚创建的策略
    cta_engine.init_strategy(strategy_name)

    # 留有足够的时间来进行策略初始化
    sleep(10)

    # 启动刚创建的策略
    cta_engine.start_strategy(strategy_name)

    # cta_engine.init_all_strategies()

    # sleep(60)
    # main_engine.write_log("CTA策略全部初始化")

    # cta_engine.start_all_strategies()
    # main_engine.write_log("CTA策略全部启动")

    print("正在交易中...")

    # get log file path
    today_date = datetime.now().strftime("%Y%m%d")
    filename = f"vt_{today_date}.log"
    log_path = "c://Users/jason/.vntrader/log/" #get_folder_path("log")
    log_file_path = os.path.join(log_path, filename)

    no_bar_count = 0
    while True:
        sleep(10)
        trading = bs.check_trading_period_usstock()

        if not trading:
            print("关闭子进程")
            main_engine.close()
            sys.exit(0)

        # 检测bar时间是否有异常：> 60+20s 重启
        time_diff = bs.last_bar_time_diff(log_file_path)

        #TODO：支持有短暂休市的市场
        if time_diff == -1:
            no_bar_count += 1
            if no_bar_count > 6: #60s
                today_hour = datetime.now().strftime("%H")
                if today_hour >= 22 or today_hour < 4:
                    print("log文件异常；或盘中重启后一直无bar数据，检测TWS及网络状况 。。。")
                    winsound.PlaySound(bs.SOUND_WARNING_LOST, winsound.SND_FILENAME)
        else:
            no_bar_count = 0
            if time_diff > 80:
                print("数据异常，重启子进程 。。。")
                if time_diff > 120:
                    print("数据异常，重启无法修复，检测TWS及网络状况 。。。")
                    winsound.PlaySound(bs.SOUND_WARNING_LOST, winsound.SND_FILENAME) 

                main_engine.close()
                sys.exit(0)

def run_parent():
    """
    Running in the parent process.
    """
    print("启动CTA策略守护父进程")

    child_process = None

    while True:
        trading = bs.check_trading_period_usstock()

        # Start child process in trading period
        if trading:
            if child_process is None:
                print("启动子进程")
                child_process = multiprocessing.Process(target=run_child)
                child_process.start()
                print("子进程启动成功")
            else:
                if not child_process.is_alive():
                    child_process.terminate()
                    child_process =  multiprocessing.Process(target=run_child)
                    child_process.start()
                    print("子进程重启成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    #TODO-03: 监控异常，重启子进程
    run_parent()