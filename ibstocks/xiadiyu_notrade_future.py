"""
Intraday xiadiyu basic signals with trading option, corresponding parameters and usage:
1. PARAM: zd_count_max & below_zd_1 - control three ZY modes:
    1.1 close position at first zd( zd_count_max=1&below_zd_1=0)
    1.2 close position at second zd( zd_count_max=2&below_zd_1=0)
    1.3 close position x below first zd price(zd_count_max=1&>below_zd_1 = x)
2. Other key PARAMs: kongbeili_threshold, dst_short_pos, zy_threshold
    2.1 kongbeili_threshold - decide a valid kong trend, which is equity dependent.
3. internal VARIABLE: zuliwei,zhichengwei - other ZY related parameters
4. common internal VARIABLES(close prices): first20_low, first20_high, yestoday_close, (day_low, day_high), 
    4.1 (poxiang: day_high > first20_high*1.006 | day_low < min(yestoday_close, open)*1.006)
5. zz & zd related parameters, which are patial values from the latest median_start: 
    5.1 median_start - the latest starting of kong strategy
    5.2 zd_count, zd_1_low
6. position related VARIABLEs:
    6.1 actual_short_pos - it's an internal variable for self.pos
    6.2 partial_short_count - info manual interupt

The Signals: 
1. k2a, k2b --- kong step by step 
2. k3a, k3b, k3c ---
3. k4a k4b --- gaokai or xianduo

The trading logic:
1. kaicang
2. zhiying
3. zhishun
NOTE: Variable zd_count will reset for new kong trend
"""

from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager
)
from vnpy.trader.object import (BarData,TickData)
from vnpy.trader.constant import Exchange, Interval
import jqdatasdk as jq

import pandas as pd
import operator
from datetime import datetime, time, timedelta
#import winsound
import sys
sys.path.append(r"E:\proj-futures\vnpy\ibstocks")
from feishu_api import FeiShutalkChatbot
import bs_vn_base as bs

#partial Xiadiyu strategy with close fuction while zhizhang
class XiadySignalFuture(CtaTemplate):
    """"""
    author = "boyscout"

    #Parameters
    zd_count_max = 1
    below_zd_1 = 0.0#1.001
    kongbeili_threshold = 0.994 # 针对各品种微调，目前铁矿5,7,9,11个点分别是0.994，0.992...
    dst_short_pos = 0
    zy_threshold = 0.99 # zhongying
    short_mode = "k2a k3a_0 k3a_1 k3b k3c k4a k4b"
    cover_before_close = True
    email_note = 0

    short_avg_price = 0.0
    xianduo_zz_kong = False

    #Variables
    median_start = 1
    zd_count = 0
    zd_1_low = 0.0

    strategies = {}
    first20_high = 0.0
    yestoday_close = 1000000.0
    yestoday_settlement = 1000000.0

    SOUND_WARNING_LOST = "e://proj-futures/vnpy/ibstocks/warning_lost.wav" # 30s
    SOUND_NOTICE_ORDER = "e://proj-futures/vnpy/ibstocks/notice_order.wav" # 5~10s
    SOUND_MANUAL_INTERUPT = "e://proj-futures/vnpy/ibstocks/manual_interupt.wav" # 10~20s

    parameters = ["zd_count_max", "below_zd_1", "kongbeili_threshold", "dst_short_pos", "zy_threshold", "short_mode", "cover_before_close", "email_note"]
    variables = ["median_start", "zd_count", "zd_1_low"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super(XiadySignalFuture, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        #Step-1: minute generator creation
        self.bg = BarGenerator(self.on_bar)
        self.bars = pd.DataFrame(columns=('datetime','tradingdate','time','open','high','low','close','volume'))
        self.bars_30k = pd.DataFrame(columns=('datetime', 'tradingdate', 'time', 'open', 'high', 'low', 'close', 'volume'))
        self.cur_trading_date = datetime.now().date()
        # signal file
        #curDay = time.strftime('%Y%m%d', time.localtime(time.time()))
        self.symbol = vt_symbol.split('.')[0]
        self.exchange = Exchange(vt_symbol.split(".")[1])
        self.symbol_jq = self.to_jq_symbol(self.symbol, self.exchange)

        self.signal_log = 'E://proj-futures/logs_vnpy/' + strategy_name + '.log'
        
        self.sh = None
        self.zz_prices = []
        self.zd_prices = []
        self.is_30k_negtive = False

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")

        #回测调用数据库，email_note==0表示回测
        if not self.email_note: 
            self.load_bar(1) #载入1天的历史数据,实盘中用于中断和盘中启动
            return

        #-----------------实盘调用jqdata初始化
        # 获取当前日期
        cur_dt = datetime.today()
        endDate = cur_dt.strftime('%Y-%m-%d %H:%M:%S')

        # jqdata登陆
        jq.auth(bs.JqAccount["Username"], bs.JqAccount["Password"])

        initData = []
        trade_days_list = jq.get_trade_days(end_date=cur_dt, count=2)

        # 获取前多日如数，按倒叙排序
        minute_df = jq.get_price(self.symbol_jq, start_date=trade_days_list[0], end_date=endDate, frequency='1m')

        # 将数据转换为loadCsv中处理的数据类型，方便处理
        del minute_df['money']
        minute_df = minute_df.reset_index()
        minute_df.rename(columns={'index': 'trade_date', 'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low','volume': 'TotalVolume'}, inplace=True)
        minute_df["Date"] = minute_df["trade_date"].map(lambda x: str(x)[0:10])
        minute_df["Time"] = minute_df["trade_date"].map(lambda x: str(x)[11:])
        del minute_df['trade_date']

        # 将数据传入到数据队列当中
        for index, row in minute_df.iterrows():
            #bar = BarData()
            bardate = datetime.strptime(row['Date'], '%Y-%m-%d').strftime('%Y%m%d')
            bartime = datetime.strptime(row['Time'], '%H:%M:%S').strftime('%H%M%S')

            hour = bartime[0:2]
            minute = bartime[2:4]
            sec = bartime[4:6]
            if minute == "00":
                minute = "59"
        
                h = int(hour)
                if h == 0:
                    h = 24
        
                hour = str(h - 1).rjust(2, '0')
            else:
                minute = str(int(minute) - 1).rjust(2, '0')
            bartime = hour + minute + sec
  
            bardatetime = datetime.strptime(' '.join([bardate, bartime]), '%Y%m%d %H%M%S')

            bar = BarData(
                symbol = self.symbol,
                exchange = self.exchange,
                interval= Interval.MINUTE,
                gateway_name="DB",
                #bar.open_interest=row['open_interest']
                datetime = bardatetime,
                open_price = row['Open'],
                high_price = row['High'],
                low_price = row['Low'],
                close_price = row['Close'], 
                volume = row['TotalVolume'],             
            )

            initData.append(bar)

        for bar in initData:
            self.on_bar(bar)

        if 'au' in self.symbol:
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: API_STABILITY_MONITOR: %f before inited\n"%(initData[-1].datetime, initData[-1].close_price))

        with open(self.signal_log, mode='a') as self.sh:
            self.sh.write("STRATIGY INITED\n")

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        #Step-1_1: 聚合呈minute数据
        self.bg.update_tick(tick)
        self.put_event()

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        #self.bg.update_bar(bar) #如有必要再合成5-minute的bar
        #Step-2:策略实体
        cur_time = bar.datetime.time()
        cur_date = bar.datetime.date()

        NIGNT_START = time(hour=20, minute=58)
        DAY_END = time(hour=15,minute=0)

        trading_date = cur_date
        if cur_time > NIGNT_START:
            if bar.datetime.weekday() is 4:
                trading_date = (bar.datetime + timedelta(days = 3)).date()
            else:
                trading_date = (bar.datetime + timedelta(days = 1)).date()
        elif cur_time <= DAY_END:
            if cur_time < time(hour=3, minute=0):
                if bar.datetime.weekday() is 5:
                    trading_date = (bar.datetime + timedelta(days = 2)).date()

        if trading_date != self.cur_trading_date:
            self.cur_trading_date = trading_date

        #TODOs: get cur_bar 345+ using history interface once network breakup or startup among trading
        # internal logic is independent although computation comsuming
        df_bar = pd.DataFrame({'datetime':[bar.datetime], 'tradingdate':[trading_date], 'time':[cur_time], 'open':[bar.open_price], \
            'high':[bar.high_price], 'low':[bar.low_price], 'close':[bar.close_price], 'volume':[bar.volume]})

        self.bars = self.bars.append(df_bar, ignore_index=True)

        mk_days = self.bars.set_index('datetime')
        mk = mk_days[mk_days['tradingdate']==self.cur_trading_date]

        num_bar = len(mk)

        if 'au' in self.symbol:
            if self.inited:
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: API_STABILITY_MONITOR: %f, actual time %s, num_bar = %d\n"%(mk.index[-1], \
                        bar.close_price, datetime.now().time(), num_bar))
            return

        #if 'ag' in self.symbol and num_bar >= 150:
        #    with open(self.signal_log, mode='a') as self.sh:
        #        self.sh.write("%s: API_STABILITY_MONITOR: %f, %d, num_bar = %d\n"%(mk.index[-1], bar.close_price, bar.volume, num_bar))


        if num_bar == 1:
            if len(mk_days) > 1:
                self.yestoday_close = mk_days['close'][-2]
                #开盘提醒
                #if self.inited:
                #    winsound.PlaySound(self.SOUND_MANUAL_INTERUPT, winsound.SND_FILENAME)
            self.zz_prices.clear()
            self.zd_prices.clear()
            self.strategies.clear()
            self.xianduo_zz_kong = False

        # 30k计算及对应时间匹配检测（TODO）
        if num_bar%30 == 0:
            df_30k = pd.DataFrame({'datetime':[bar.datetime], 'date':[trading_date], 'time':[cur_time], 'open':[mk['open'][-30]], \
                'high':[mk['high'][-30:].max()], 'low':[mk['low'][-30:].min()], 'close':[mk['close'][-1]]})
            self.bars_30k = self.bars_30k.append(df_30k, ignore_index=True)
            self.is_30k_negtive = mk['close'][-1] <= self.bars_30k['close'][-10:].mean()
            with open(self.signal_log, mode='a') as self.sh:
                self.sh.write("%s: 30k, Price: %.2f; Positive: %d\n"%(mk["time"][-1], mk["close"][-1], self.is_30k_negtive))

        if num_bar%15 == 0 and cur_time > time(hour=14,minute=58) and cur_time < time(hour=15,minute=29):
            df_30k = pd.DataFrame({'datetime':[bar.datetime], 'date':[trading_date], 'time':[cur_time], 'open':[mk['open'][-15]], \
                'high':[mk['high'][-15:].max()], 'low':[mk['low'][-15:].min()], 'close':[mk['close'][-1]]})
            self.bars_30k = self.bars_30k.append(df_30k, ignore_index=True)
            with open(self.signal_log, mode='a') as self.sh:
                self.sh.write("%s: CLOSE_30k, Price: %.2f; Positive: %d\n"%(mk["time"][-1], mk["close"][-1], self.is_30k_negtive))

        # 盘中重启，当日历史数据必须全部参与计算
        if not self.inited and self.yestoday_close > 999999:
            return

        # feishu warning
        feishu = None

        # 尾盘强制平仓 TODO: 根据当前时间定是否是尾盘
        if cur_time > time(hour=14,minute=45) and cur_time <= time(hour=15,minute=0):
            if  self.short_avg_price > 0.1 and self.cover_before_close:
                res = (-mk["close"][-1]+self.short_avg_price)/self.short_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: weipan_pingcang_short with profit %.1f at price %.2f\n"%(mk["time"][-1], res, mk["close"][-1]))
                self.short_avg_price = 0.0
                self.strategies.clear()
                if self.email_note and self.inited:
                    msg = f"{cur_time}: weipan_pingcang_short {self.symbol} with profit {res}!"
                    if not feishu:
                        feishu = FeiShutalkChatbot()
                    feishu.send_text(msg)
                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
            #if num_bar%15 == 0:
            #    self.yestoday_settlement = (mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()
            return

        mk = mk.assign(vwap = ((mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()).ffill())
        # 重置参数，重新开始波段计算
        if num_bar == 1 or mk['close'][-1] >= mk['vwap'][-1]:
            self.median_start = num_bar
            self.zd_count = 0
            self.zd_1_low = 0.0

        day_CH_index, day_CH = max(enumerate(mk["close"]), key=operator.itemgetter(1))
        day_CL_index, day_CL = min(enumerate(mk["close"]), key=operator.itemgetter(1))
        # 早盘：k2a|k3a_0 - 2种模式
        if num_bar > 10 and num_bar <= 30: # 夜盘前一小时或日盘品种前半小时
            if num_bar == 20:
                self.first20_high = max(mk["close"].max(), mk["open"][0])
            # 无空背离 + 开盘偏空信号 + 最近10分钟滞涨 + 无破相空多 + 长期偏空走稳|小高局部止涨
            if day_CL > mk["vwap"][day_CL_index]*self.kongbeili_threshold and mk["close"][:4].min() < mk["open"][0] \
                and (mk['close'][-10:] <= mk["close"][day_CL_index:-10].max()).all() and mk['close'][-10:].min() > day_CL \
                and day_CH < max(self.yestoday_close, mk["open"][0])*1.006:
                if (mk["close"] <= mk["vwap"]*1.001).all() and 'k2a' in self.short_mode.split(' '):
                    if 'k2a' not in self.strategies and self.is_30k_negtive:
                        self.short_avg_price = mk["close"][-1]
                        self.strategies['k2a'] = mk["close"][-1]
                        with open(self.signal_log, mode='a') as self.sh:
                            self.sh.write("%s: SIGNAL_mode_k2a_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                            self.sh.write("Double check whether it's zhicheng or not, like last day's low or recent low.\n")
                        if self.email_note and self.inited:
                            msg = f"{cur_time}: SIGNAL_mode_k2a_kaicang {self.symbol}!"
                            if not feishu:
                                feishu = FeiShutalkChatbot()
                            feishu.send_text(msg)
                elif day_CH > day_CL*1.003 and len(mk[mk['close'] > mk["vwap"]])>=10 \
                    and 'k3a_0' not in self.strategies and 'k3a_0' in self.short_mode.split(' '):
                    #and (self.is_30k_positive or (mk["close"][-10:]>self.bars_30k['close'][-1]).any()):
                    self.short_avg_price = mk["close"][-1]
                    self.strategies['k3a_0'] = mk["close"][-1]
                    with open(self.signal_log, mode='a') as self.sh:
                        self.sh.write("%s: SIGNAL_mode_k3a_xiaogao_10mins_zhizhang_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        if 'k2a' in self.strategies:
                            self.sh.write("WARNING: zhuan_duo_jubu_zhizhang_k3a_0 after xiaokong_k2a, had better wait for the k3a_1 signal!\n")
                    if self.email_note and self.inited:
                        msg = f"{cur_time}: SIGNAL_mode_k3a_0_xiaogao_10mins_zhizhang_kaicang {self.symbol}!"
                        if not feishu:
                            feishu = FeiShutalkChatbot()
                        feishu.send_text(msg)
        elif num_bar > 30:
            # Signal：mode_k3a_1
            # 无空背离|空背离 + 最近30分钟滞涨 + 无破相多 + 长期偏空走稳|小高局部止涨
            median_adjust_high = mk["close"][day_CL_index:-30].max()
            if (mk['close'][-30:] < median_adjust_high*1.001).all() and mk['close'][-1] < median_adjust_high \
                and day_CH < max(self.yestoday_close, self.first20_high)*1.006:
                if ((mk["close"][-10:] < mk["vwap"][-10:]*1.001).all() or (mk["close"][-30:] > mk["vwap"][-30:]).all()):
                    if day_CL > mk["vwap"][day_CL_index]*self.kongbeili_threshold:
                        #normal signal
                        if (len(self.zz_prices) == 0 or median_adjust_high != self.zz_prices[-1][0]):
                            self.zz_prices.append((median_adjust_high, num_bar))
                            if len(self.zz_prices) > 1 and (self.zz_prices[-1][1] - self.zz_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode_k3a_1_xiaogao_30mins_zhizhang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                if self.email_note == 1 and self.inited:
                                    msg = f"{cur_time}: NORMAL_SIGNAL_mode_k3a_1_xiaogao_30mins_zhizhang {self.symbol}!"
                                    if not feishu:
                                        feishu = FeiShutalkChatbot()
                                    feishu.send_text(msg)
                        if 'k3a_1' in self.short_mode.split(' ') and 'k3a_1' not in self.strategies \
                            and (cur_time > time(hour=21,minute=30) or cur_time < time(hour=9,minute=50)):
                            #and self.is_30k_positive:
                            self.strategies['k3a_1'] = mk["close"][-1]
                            if self.short_avg_price < 0.1:
                                self.short_avg_price = mk["close"][-1]
                            else:
                                self.short_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode_k3a_1_xiaogao_30mins_zhizhang_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                if 'k3a_0' in self.strategies:
                                    self.sh.write("NOTE: mode_k3a_1 after mode_k3a_0, ignore it if necessary!\n")
                            if self.email_note and self.inited:
                                msg = f"{cur_time}: SIGNAL_mode_k3a_1_xiaogao_30mins_zhizhang_kaicang {self.symbol}!"
                                if not feishu:
                                    feishu = FeiShutalkChatbot()
                                feishu.send_text(msg)
                            # k3a_1 提醒
                            #winsound.PlaySound(self.SOUND_MANUAL_INTERUPT, winsound.SND_FILENAME)
                    elif day_CL <= mk["vwap"][day_CL_index]*self.kongbeili_threshold and mk["close"][day_CL_index:-30].max() > day_CL*1.004:
                        #normal signal
                        if (len(self.zz_prices) == 0 or median_adjust_high != self.zz_prices[-1][0]):
                            self.zz_prices.append((median_adjust_high, num_bar))
                            if len(self.zz_prices) > 1 and (self.zz_prices[-1][1] - self.zz_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode_k3b_kongtiaozheng_zhizhang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                if self.email_note == 1 and self.inited:
                                    msg = f"{cur_time}: NORMAL_SIGNAL_mode_k3b_kongtiaozheng_zhizhang {self.symbol}!"
                                    if not feishu:
                                        feishu = FeiShutalkChatbot()
                                    feishu.send_text(msg)
                        if 'k3b' in self.short_mode.split(' ') and 'k3b' not in self.strategies \
                            and (cur_time > time(hour=21,minute=30) or cur_time < time(hour=14,minute=0)) \
                            and self.zd_count < self.zd_count_max and self.is_30k_negtive:
                            self.strategies['k3b'] = mk["close"][-1]
                            if self.short_avg_price < 0.1:
                                self.short_avg_price = mk["close"][-1]
                            else:
                                self.short_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode_k3b_kongtiaozheng_zhizhang_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                self.sh.write("Double check whether it's weizhi zhendang_kong and already below zhongying or it. if so, just zhiying.\n")
                            # k3b 提醒
                            if self.email_note and self.inited:
                                msg = f"{cur_time}: SIGNAL_mode_k3b_kongtiaozheng_zhizhang_kaicang {self.symbol}!"
                                if not feishu:
                                    feishu = FeiShutalkChatbot()
                                feishu.send_text(msg)
                            #winsound.PlaySound(self.SOUND_MANUAL_INTERUPT, winsound.SND_FILENAME)

                    # 根据时间判定k3c机会:类似k3a类机会，只是中间可能是第二波小多止涨后空信号(考虑第二波小多不创新高的情况)
                    if cur_time > time(hour=9,minute=50) and cur_time < time(hour=14,minute=10) \
                        and mk['close'][-1] > mk["vwap"][-1]*0.997 and day_CL > mk["vwap"][day_CL_index]*self.kongbeili_threshold:
                        #首次空止跌 | 二次空止跌 | 二次不创新低空止跌
                        if (len(self.zz_prices) == 0 or median_adjust_high != self.zz_prices[-1][0]):
                            self.zz_prices.append((median_adjust_high, num_bar))
                            if len(self.zz_prices) > 1 and (self.zz_prices[-1][1] - self.zz_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode_k3c_xiangduigap_30mins_zhizhang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                if self.email_note == 1 and self.inited:
                                    msg = f"{cur_time}: NORMAL_SIGNAL_mode_k3c_xiangduigap_30mins_zhizhang {self.symbol}!"
                                    if not feishu:
                                        feishu = FeiShutalkChatbot()
                                    feishu.send_text(msg)
                        if 'k3c' in self.short_mode.split(' ') and 'k3c' not in self.strategies:# and self.is_30k_positive:
                            self.strategies['k3c'] = mk["close"][-1]
                            if self.short_avg_price < 0.1:
                                self.short_avg_price = mk["close"][-1]
                            else:
                                self.short_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode_k3c_xiangduigao_30mins_zhizhang_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                self.sh.write("Double check 30k is above MA.\n")
                            if self.email_note and self.inited:
                                msg = f"{cur_time}: SIGNAL_mode_k3c_xiangduigao_30mins_zhizhang_kaicang {self.symbol}!"
                                if not feishu:
                                    feishu = FeiShutalkChatbot()
                                feishu.send_text(msg)
                            # 3c 提醒
                            #.PlaySound(self.SOUND_MANUAL_INTERUPT, winsound.SND_FILENAME)

            # Signal：mode_k4b 先多滞涨震荡空
            if (cur_time > time(hour=21,minute=30) or cur_time < time(hour=11,minute=0)) \
                and (num_bar - day_CH_index == 31 or num_bar - day_CH_index == 19) \
                and day_CH > self.yestoday_close*1.006 and day_CH*self.kongbeili_threshold > mk["vwap"][day_CH_index] \
                and mk['close'][-num_bar + day_CH_index + 1:].min() > (day_CL + day_CH)*0.5 \
                and day_CL > mk["open"][0]*0.995:
                self.xianduo_zz_kong = True
                if (len(self.zz_prices) == 0 or median_adjust_high != self.zz_prices[-1][0]):
                    self.zz_prices.append((median_adjust_high, num_bar))
                    if len(self.zz_prices) > 1 and (self.zz_prices[-1][1] - self.zz_prices[-2][1]) < 5:
                        pass
                    else:
                        with open(self.signal_log, mode='a') as self.sh:
                            self.sh.write("%s: NORMAL_SIGNAL_mode_k4b_duo_18or30mins_zhizhang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        if self.email_note == 1 and self.inited:
                            msg = f"{cur_time}: NORMAL_SIGNAL_mode_k4b_duo_18or30mins_zhizhang {self.symbol}!"
                            if not feishu:
                                feishu = FeiShutalkChatbot()
                            feishu.send_text(msg)
                if 'k4b' in self.short_mode.split(' ') and 'k4b' not in self.strategies:
                    self.strategies['k4b'] = mk["close"][-1]
                    if self.short_avg_price < 0.1:
                        self.short_avg_price = mk["close"][-1]
                    else:
                        self.short_avg_price = sum(self.strategies.values())/len(self.strategies)
                    with open(self.signal_log, mode='a') as self.sh:
                        self.sh.write("%s: SIGNAL_mode_k4b_duo_18or30mins_zhizhang_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        self.sh.write("Double check whether it's confirming zuli or it. if not, second duo_zz is a good choice for long.\n")
                    if self.email_note and self.inited:
                        msg = f"{cur_time}: SIGNAL_mode_k4b_duo_18or30mins_zhizhang_kaicang {self.symbol}!"
                        if not feishu:
                            feishu = FeiShutalkChatbot()
                        feishu.send_text(msg)

            # Signal：zhishun
            # 11：05~11:20定点止损
            mk_l20 = mk[-20:]
            if cur_time > time(hour=11,minute=5) and cur_time < time(hour=11,minute=20) and self.short_avg_price >0.1 \
                and len(mk_l20[mk_l20["close"] < mk_l20["vwap"]*1.001]) < 16:
                res = (-mk["close"][-1]+self.short_avg_price)/self.short_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: SIGNAL_dingdian_zhishun_short_1110 at price %.2f with profit: %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                    self.sh.write("Double check whether it's creating xingao. if not, wait until 14:45 and check 30k.\n")
                self.short_avg_price = 0.0
                if self.email_note and self.inited:
                    msg = f"{cur_time}: SIGNAL_dingdian_zhishun_kong_1110 {self.symbol}!"
                    if not feishu:
                        feishu = FeiShutalkChatbot()
                    feishu.send_text(msg)
            # 破相后下挫时局部止跌止损
            if self.short_avg_price >0.1 and self.xianduo_zz_kong == False \
                and day_CH >= max(self.yestoday_close, self.first20_high)*1.006 \
                and (mk_l20["close"][-1] < (day_CL + day_CH)/2 or mk_l20["close"][-1]<mk_l20["vwap"][-1]*1.001) \
                and (mk_l20["close"][-5:] > mk_l20["close"][:-5].min()*0.999).all():
                res = (-mk["close"][-1]+self.short_avg_price)/self.short_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: SIGNAL_changqi_pianduo_xiachuo_zhishun at price %.2f with profit %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                self.short_avg_price = 0.0
                if self.email_note and self.inited:
                    msg = f"{cur_time}: SIGNAL_changqi_pianduo_xiachuo_zhishun {self.symbol}!"
                    if not feishu:
                        feishu = FeiShutalkChatbot()
                    feishu.send_text(msg)
            # 止盈
            if num_bar - self.median_start > 10:
                median_CL_index, median_CL = min(enumerate(mk["close"][self.median_start:]), key=operator.itemgetter(1))
                median_l_zd = mk[self.median_start+median_CL_index:]
                if median_CL < median_l_zd["vwap"][0] * self.kongbeili_threshold:
                    if len(median_l_zd) == 10 or len(median_l_zd) == 18:
                        with open(self.signal_log, mode='a') as self.sh:
                            self.sh.write("%s: NORMAL_SIGNAL_jubu_zd %d minutes, at price %.2f\n"%(mk.index[-1], len(median_l_zd), mk["close"][-1]))
                        if self.email_note == 1 and self.inited:
                            msg = f"{cur_time}: NORMAL_SIGNAL_jubu_zd 10 or 18mins {self.symbol}!"
                            if not feishu:
                                feishu = FeiShutalkChatbot()
                            feishu.send_text(msg)
                    elif len(median_l_zd) >= 30 and len(median_l_zd) < 60:
                        if len(median_l_zd) == 30:
                            self.zd_count += 1
                            if self.zd_count == 1:
                                self.zd_1_low = median_CL
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: NORMAL_SIGNAL_zd: %d times, at price %.2f\n"%(mk.index[-1], self.zd_count, mk["close"][-1]))
                            if self.email_note == 1 and self.inited:
                                msg = f"{cur_time}: NORMAL_SIGNAL_zd 30 mins {self.symbol}!"
                                if not feishu:
                                    feishu = FeiShutalkChatbot()
                                feishu.send_text(msg)
                        #case1&2: close long while zz 1 or 2 times
                        if self.zd_count >= self.zd_count_max \
                            and median_CL < self.yestoday_close*self.zy_threshold and mk["close"][-1] < (mk["vwap"][-1] + median_CL )*0.5:
                            if self.short_avg_price > 0.1:
                                res = (-mk["close"][-1]+self.short_avg_price)/self.short_avg_price*100
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: SIGNAL_zd_zhiying: %d times, at price %.2f with profit %.1f\n"%(mk.index[-1], \
                                        self.zd_count, mk["close"][-1], res))
                                    self.sh.write("Double check whether it's a strong trending; if so, wait second ZD signal.\n")
                                self.short_avg_price = 0.0
                                if self.email_note and self.inited:
                                    msg = f"{cur_time}: SIGNAL_zd_zhiying {self.symbol}!"
                                    if not feishu:
                                        feishu = FeiShutalkChatbot()
                                    feishu.send_text(msg)

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        self.put_event()

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        self.put_event()

    def to_jq_symbol(self, symbol: str, exchange: Exchange):
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        if exchange in [Exchange.SSE, Exchange.SZSE]:
            if exchange == Exchange.SSE:
                jq_symbol = f"{symbol}.XSHG"  # 上海证券交易所
            else:
                jq_symbol = f"{symbol}.XSHE"  # 深圳证券交易所
        elif exchange == Exchange.SHFE:
            jq_symbol = f"{symbol}.XSGE"  # 上期所
        elif exchange == Exchange.CFFEX:
            jq_symbol = f"{symbol}.CCFX"  # 中金所
        elif exchange == Exchange.DCE:
            jq_symbol = f"{symbol}.XDCE"  # 大商所
        elif exchange == Exchange.INE:
            jq_symbol = f"{symbol}.XINE"  # 上海国际能源期货交易所
        elif exchange == Exchange.CZCE:
            # 郑商所 的合约代码年份只有三位 需要特殊处理
            for count, word in enumerate(symbol):
                if word.isdigit():
                    break

            # Check for index symbol
            time_str = symbol[count:]
            if time_str in ["88", "888", "99", "8888"]:
                return f"{symbol}.XZCE"

            # noinspection PyUnboundLocalVariable
            product = symbol[:count]
            year = symbol[count]
            month = symbol[count + 1:]

            if year == "9":
                year = "1" + year
            else:
                year = "2" + year

            jq_symbol = f"{product}{year}{month}.XZCE"

        return jq_symbol.upper()

'''
    def test_market_order(self):
        """"""
        self.buy(self.last_tick.limit_up, 1)
        self.write_log("执行市价单测试")

    def test_limit_order(self):
        """"""
        self.buy(self.last_tick.limit_down, 1)
        self.write_log("执行限价单测试")

    def test_stop_order(self):
        """"""
        self.buy(self.last_tick.ask_price_1, 1, True)
        self.write_log("执行停止单测试")

    def test_cancel_all(self):
        """"""
        self.cancel_all()
        self.write_log("执行全部撤单测试")
'''
