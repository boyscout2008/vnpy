from datetime import datetime, time, timedelta, timezone
import sys
import os

SOUND_WARNING_LOST = "c://proj-futures/vnpy/ibstocks/warning_lost.wav" # 30s
SOUND_NOTICE_ORDER = "c://proj-futures/vnpy/ibstocks/notice_order.wav" # 5~10s
SOUND_MANUAL_INTERUPT = "c://proj-futures/vnpy/ibstocks/manual_interupt.wav" # 10~20s

#---------------------------functions---------------------------------
# 自动重启本程序
def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

# 美股交易时段09：30~16：00
DAY_START_USSTOCK = time(9, 25)
DAY_END_USSTOCK = time(16, 0)
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

# 返回bar时间跟当前时间时间差（seconds）
def diff_time(bar_time):
    utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    md_dt = utc_dt.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d %H:%M")
    cur_time = datetime.strptime(md_dt, "%Y-%m-%d %H:%M")
    return (cur_time -bar_time).seconds

#返回log文件中最后一根bar和当前时间的时间差（seconds）
def last_bar_time_diff(log_file_path):
    time_diff = -1
    if not os.path.exists(log_file_path):
        return time_diff

    log_file = open(log_file_path, 'rb')
    if not log_file:
        return time_diff

    if os.path.getsize(log_file_path) > 3000:
        log_file.seek(-3000,2)

    lines = log_file.readlines()[1:]

    for i in range(0, lines.__len__())[::-1]:
        #print(lines[i])
        line = str(lines[i], encoding = "utf8")
        if 'API_STABILITY_MONITOR' in line:
            # check datetime
            #utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
            #md_dt = utc_dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
            md_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur_time = datetime.strptime(md_dt, "%Y-%m-%d %H:%M:%S")

            bar_time = line.split('INFO')[0].strip().split(',')[0]
            bar_time = datetime.strptime(bar_time, "%Y-%m-%d %H:%M:%S")
            time_diff = (cur_time - bar_time).seconds
            #print(cur_time, bar_time)
            break
    log_file.close()
    return time_diff

#--------------------------vn trading setting-------------------------
# https://www.vnpy.com/docs/cn/gateway.html
# IB接口连接设置
ib_setting = {
    "TWS地址": "127.0.0.1",
    "TWS端口": 7497, # necessary #模拟端口 #实盘端口 7496？
    "客户号": 1
}