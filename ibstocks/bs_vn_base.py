from datetime import datetime, time, timedelta, timezone
import sys
import os

#---------------------------functions---------------------------------
# 自动重启本程序
def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

# 美股交易时段09：30~16：00
DAY_START_USSTOCK = time(9, 25)
DAY_END_USSTOCK = time(16, 5)
# 纽约外汇交易时段# 24小时
def check_trading_period_usstock():
    utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    md_dt = utc_dt.astimezone(timezone(timedelta(hours=-4)))
    current_time = md_dt.time()
    trading = False
    if current_time >= DAY_START_USSTOCK and current_time <= DAY_END_USSTOCK:
        trading = True
    
    return trading

DAY_START = time(8, 45)
DAY_END = time(14, 29)
NIGHT_START = time(20, 45)
NIGHT_END = time(2, 45)
def check_trading_period_future_china():
    """"""
    current_time = datetime.now().time()

    trading = False
    if (
        (current_time >= DAY_START and current_time <= DAY_END)
        or (current_time >= NIGHT_START)
        or (current_time <= NIGHT_END)
    ):
        trading = True

    return trading

#--------------------------vn trading setting-------------------------
# https://www.vnpy.com/docs/cn/gateway.html
# IB接口连接设置
ib_setting = {
    "TWS地址": "127.0.0.1",
    "TWS端口": 7497, # necessary #模拟端口 #实盘端口 7496？
    "客户号": 1
}