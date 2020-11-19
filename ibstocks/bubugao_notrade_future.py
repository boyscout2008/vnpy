"""
Intraday bubugao basic signals with trading option, corresponding parameters and usage:
1. PARAM: zz_count_max & above_zz_1 - control three ZY modes:
    1.1 close position at first zz( zz_count_max=1&above_zz_1=0)
    1.2 close position at second zz( zz_count_max=2&above_zz_1=0)
    1.3 close position x above first zz price(zz_count_max=1&>above_zz_1 = x)
2. Other key PARAMs: duobeili_threshold, dst_long_pos, zy_threshold
    2.1 duobeili_threshold - decide a valid long trend, which is equity dependent.
3. internal VARIABLE: zuliwei - other ZY related parameters：
    2.1 the fourth ZY mode with zuliwei - 根据涨幅、日内波段和背离分时状况选择：+-1%以内，+3%，+5~10%止盈
4. common internal VARIABLES(close prices): first20_low, yestoday_close, (day_low, day_high), (poxiang: day_low < first20_low*0.99)
5. zz & zd related parameters, which are patial values from the latest median_start: 
    5.1 median_start - the latest starting of long strategy
    5.2 zz_count, zz_1_high
6. position related VARIABLEs:
    6.1 actual_long_pos - it's an internal variable for self.pos
    6.2 partial_pos_count - info manual interupt

The Signals: 
1. 2a, 2b --- duo step by step 
2. 3a, 3b, 3c --- duo with small or long zhidie period 
3. 4a 4b --- dikai or xiankong

The trading logic:
1. kaicang
2. zhiying
3. zhishun
NOTE: Variable zz_count will reset for new long trend
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

import pandas as pd
import operator
from datetime import datetime, time, timedelta
import winsound
#from trading_hour import TRADINGHOUR 
#import bs_vn_base as bs

#partial bubugao strategy with close fuction while zhizhang
class BubugaoSignalFuture(CtaTemplate):
    """"""
    author = "boyscout"

    #signal_log = ''
    #sh = None

    #Parameters
    zz_count_max = 1
    above_zz_1 = 0.0#1.001
    duobeili_threshold = 1.006 # 针对各品种微调，目前铁矿5,7,9,11个点分别是1.006，1.008，1.01，1.012
    dst_long_pos = 0
    zy_threshold = 1.025 # zhongyang
    #long_mode = ['2a', '3a_0', '3a_1', '3b', '3c'] #omit 4a and 4b now
    long_mode = "2a 3a_0 3a_1 3b 3c"
    cover_before_close = True
    #last_15mins_bar_index = 345-15 #铁矿+夜盘；日盘：225；白银沪镍等品种另算

    long_avg_price = 0.0
    xiankong_zd_duo = False
    pianduo_tiaozheng = False
    #day_end = time(hour=14,minute=45)
    #day_close = time(hour=15,minute=0)

    #Variables
    median_start = 1
    zz_count = 0
    zz_1_high = 0.0
    #0：初始化状态，无多空信号； 2: bubugao_mode2； 2<<1: 多转滞涨
    #3: bubugao_mode3a_10min; 3<<2: bubugao_mode3a_30min; 3<<3: bubugao_mode3a_zhizhang
    #-1：背离空破相，偏空趋势；-1<<1: 空止跌（结合30k，潜在买点）
    # internal
    #bar_30m = []
    strategies = {}
    first20_low = 0.0
    yestoday_close = 9999.0
    yestoday_settlement = 9999.0
    # position related 
    #partial_pos_count = 0
    #actual_long_pos = 0

    parameters = ["zz_count_max", "above_zz_1", "duobeili_threshold", "dst_long_pos", "zy_threshold", "long_mode", "cover_before_close"]
    variables = ["median_start", "zz_count", "zz_1_high", "trending"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super(BubugaoSignalFuture, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        #Step-1: minute generator creation
        #self.bg = BarGenerator(self.on_bar, 15, self.on_30min_bar)
        self.bg = BarGenerator(self.on_bar)
        self.bars = pd.DataFrame(columns=('datetime','tradingdate','time','open','high','low','close','volume'))
        self.bars_30k = pd.DataFrame(columns=('datetime', 'tradingdate', 'time', 'open', 'high', 'low', 'close', 'volume'))
        self.cur_trading_date = datetime.now().date()
        # signal file
        #curDay = time.strftime('%Y%m%d', time.localtime(time.time()))
        self.signal_log = 'E://proj-futures/logs/' + strategy_name + '_' + vt_symbol.split('.')[0] + '.log'
        self.sh = None
        self.zz_prices = []
        self.zd_prices = []
        self.is_30k_positive = True

        #self.tradingtime = TRADINGHOUR()
        #self.symbol = "".join(re.findall(r"\D+",self.get_data()["vt_symbol"].split(".")[0])).upper()
        #self.start_time,self.end_time = self.tradingtime.get_trading_time(self.symbol)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")

        self.load_bar(1) #载入1天的历史数据,实盘中用于中断和盘中启动

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

        if cur_time > NIGNT_START:
            if bar.datetime.weekday() is 4:
                trading_date = (bar.datetime + timedelta(days = 3)).date()
            else:
                trading_date = (bar.datetime + timedelta(days = 1)).date()
        elif cur_time <= DAY_END:
            if cur_time < time(hour=3, minute=0):
                if bar.datetime.weekday() is 5:
                    trading_date = (bar.datetime + timedelta(days = 2)).date()
                else:
                    trading_date = cur_date
            else:
                trading_date = cur_date

        if trading_date != self.cur_trading_date:
            #print("#####%s:  %s, %s\n"%(bar.datetime, trading_date, self.cur_trading_date))
            self.cur_trading_date = trading_date

        #TODOs: get cur_bar 345+ using history interface once network breakup or startup among trading
        # internal logic is independent although computation comsuming
        df_bar = pd.DataFrame({'datetime':[bar.datetime], 'tradingdate':[trading_date], 'time':[cur_time], 'open':[bar.open_price], \
            'high':[bar.high_price], 'low':[bar.low_price], 'close':[bar.close_price], 'volume':[bar.volume]})

        self.bars = self.bars.append(df_bar, ignore_index=True)

        mk_days = self.bars.set_index('datetime')
        mk = mk_days[mk_days['tradingdate']==self.cur_trading_date]

        num_bar = len(mk)

        #TODOs: get bar 30mins
        #self.bar_30m = mk_days['close'][-225:-1:30]
        if self.inited and 'au' in self.signal_log:
            with open(self.signal_log, mode='a') as self.sh:
                self.sh.write("%s: API_STABILITY_MONITOR: %f, %d, num_bar = %d\n"%(mk["time"][-1], bar.close_price, bar.volume, num_bar))
            return

        if num_bar == 1:
            if len(mk_days) > 1:
                self.yestoday_close = mk_days['close'][-2]
            self.zz_prices.clear()
            self.zd_prices.clear()
            self.strategies.clear()

        # 30k计算及对应时间匹配检测（TODO）
        if num_bar%30 == 0:
            df_30k = pd.DataFrame({'datetime':[bar.datetime], 'date':[trading_date], 'time':[cur_time], 'open':[mk['open'][-30]], \
                'high':[mk['high'][-30:].max()], 'low':[mk['low'][-30:].min()], 'close':[mk['close'][-1]]})
            self.bars_30k = self.bars_30k.append(df_30k, ignore_index=True)
            self.is_30k_positive = mk['close'][-1] >= self.bars_30k['close'][-10:].mean()
            with open(self.signal_log, mode='a') as self.sh:
                self.sh.write("%s: 30k, Price: %.2f; Positive: %d\n"%(mk["time"][-1], mk["close"][-1], self.is_30k_positive))

        if num_bar%15 == 0 and cur_time > time(hour=14,minute=58) and cur_time < time(hour=15,minute=29):
            df_30k = pd.DataFrame({'datetime':[bar.datetime], 'date':[trading_date], 'time':[cur_time], 'open':[mk['open'][-15]], \
                'high':[mk['high'][-15:].max()], 'low':[mk['low'][-15:].min()], 'close':[mk['close'][-1]]})
            self.bars_30k = self.bars_30k.append(df_30k, ignore_index=True)

        # 尾盘强制平仓 TODO: 根据当前时间定是否是尾盘
        if cur_time > time(hour=14,minute=45) and cur_time <= time(hour=15,minute=0):
            if  self.long_avg_price > 0.1 and self.cover_before_close:
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: weipan_pingcang with profit %.1f at price %.2f\n"%(mk["time"][-1], res, mk["close"][-1]))
                self.long_avg_price = 0.0
                self.strategies.clear()
                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
            #if num_bar%15 == 0:
            #    self.yestoday_settlement = (mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()
            return

        #TODO: 30k及其ma5和ma10
        mk = mk.assign(vwap = ((mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()).ffill())
        # 重置参数，重新开始波段计算
        if num_bar == 1 or mk['close'][-1] <= mk['vwap'][-1]:
            self.median_start = num_bar
            self.zz_count = 0
            self.zz_1_high = 0.0

        day_CH_index, day_CH = max(enumerate(mk["close"]), key=operator.itemgetter(1))
        day_CL_index, day_CL = min(enumerate(mk["close"]), key=operator.itemgetter(1))
        # 早盘：2a|3a_0 - 2种模式， 4a|4b -- 暂不提供信号
        if num_bar > 10 and num_bar <= 30: # 夜盘前一小时或日盘品种前半小时
            if num_bar == 20:
                self.first20_low = min(mk["close"].min(), mk["open"][0])
            # 无多背离 + 开盘偏多信号 + 最近10分钟滞跌 + 无破相空 + 长期偏多走稳|小低局部止跌
            if day_CH < mk["vwap"][day_CH_index]*self.duobeili_threshold and mk["close"][:4].max() > mk["open"][0] \
                and (mk['close'][-10:] >= mk["close"][day_CH_index:-10].min()).all() and mk['close'][-10:].max() < day_CH \
                and day_CL > min(self.yestoday_close, mk["open"][0])*0.994:
                if (mk["close"] >= mk["vwap"]*0.999).all() and '2a' in self.long_mode.split(' '):
                    if '2a' not in self.strategies and self.is_30k_positive:
                        self.long_avg_price = mk["close"][-1]
                        self.strategies['2a'] = mk["close"][-1]
                        with open(self.signal_log, mode='a') as self.sh:
                            self.sh.write("%s: SIGNAL_mode2a_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                            self.sh.write("Double check whether it's zuliwei or not, like last day's high or recent high.\n")
                elif day_CL < day_CH*0.997 and len(mk[mk['close'] < mk["vwap"]])>=10 \
                    and '3a_0' not in self.strategies and '3a_0' in self.long_mode.split(' '):
                    #and (self.is_30k_positive or (mk["close"][-10:]>self.bars_30k['close'][-1]).any()):
                    self.long_avg_price = mk["close"][-1]
                    self.strategies['3a_0'] = mk["close"][-1]
                    with open(self.signal_log, mode='a') as self.sh:
                        self.sh.write("%s: SIGNAL_mode3a_xiaodi_10mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        if '2a' in self.strategies:
                            self.sh.write("WARNING: zhuan_kong_jubu_zhidie_3a_0 after xiaoduo_2a, had better wait for the 3a_1 signal!\n")

        elif num_bar > 30:
            #pre30m_CH_index, pre30m_CH = max(enumerate(mk["close"][:30]), key=operator.itemgetter(1))
            # Signal：mode_3a_1
            # 无多背离|多背离 + 最近30分钟滞跌 + 无破相空 + 长期偏多走稳|小低局部止跌
            median_adjust_low = mk["close"][day_CH_index:-30].min()
            if (mk['close'][-30:] > median_adjust_low*0.999).all() and mk['close'][-1] > median_adjust_low \
                and day_CL > min(self.yestoday_close, mk["open"][0])*0.994:
                if ((mk["close"][-10:] > mk["vwap"][-10:]*0.999).all() or (mk["close"][-30:] < mk["vwap"][-30:]).all()):
                    if day_CH < mk["vwap"][day_CH_index]*self.duobeili_threshold:
                        #normal signal
                        if (len(self.zd_prices) == 0 or median_adjust_low != self.zd_prices[-1][0]):
                            self.zd_prices.append((median_adjust_low, num_bar))
                            if len(self.zd_prices) > 1 and (self.zd_prices[-1][1] - self.zd_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode3a_1_xiaodi_30mins_zhidie at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        #print(self.long_mode.split(' '))
                        if '3a_1' in self.long_mode.split(' ') and '3a_1' not in self.strategies \
                            and (cur_time > time(hour=21,minute=30) or cur_time < time(hour=9,minute=50)):
                            #and self.is_30k_positive:
                            self.strategies['3a_1'] = mk["close"][-1]
                            if self.long_avg_price < 0.1:
                                self.long_avg_price = mk["close"][-1]
                            else:
                                self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode3a_1_xiaodi_30mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                if '3a_0' in self.strategies:
                                    self.sh.write("NOTE: mode_3a_1 after mode_3a_0, ignore it if necessary!\n") 
                    elif day_CH >= mk["vwap"][day_CH_index]*self.duobeili_threshold and mk["close"][day_CH_index:-30].min() < day_CH*0.996:
                        #normal signal
                        if (len(self.zd_prices) == 0 or median_adjust_low != self.zd_prices[-1][0]):
                            self.zd_prices.append((median_adjust_low, num_bar))
                            if len(self.zd_prices) > 1 and (self.zd_prices[-1][1] - self.zd_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode3b_duotiaozheng_zhidie at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        
                        if '3b' in self.long_mode.split(' ') and '3b' not in self.strategies \
                            and (cur_time > time(hour=21,minute=30) or cur_time < time(hour=14,minute=0)) \
                            and self.zz_count < self.zz_count_max and self.is_30k_positive:
                            self.strategies['3b'] = mk["close"][-1]
                            if self.long_avg_price < 0.1:
                                self.long_avg_price = mk["close"][-1]
                            else:
                                self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode3b_duotiaozheng_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                self.sh.write("Double check whether it's weizhi zhendang_duo and already above zhongyang or it. if so, just zhiying.\n")

                    # 根据时间判定3c机会:类似3a类机会，只是中间可能是第二波小空止跌后多信号
                    if cur_time > time(hour=9,minute=50) and cur_time < time(hour=14,minute=10) \
                        and mk['close'][-1] < mk["vwap"][-1]*1.003 and day_CH < mk["vwap"][day_CH_index]*self.duobeili_threshold:
                        if (len(self.zd_prices) == 0 or median_adjust_low != self.zd_prices[-1][0]):
                            self.zd_prices.append((median_adjust_low, num_bar))
                            if len(self.zd_prices) > 1 and (self.zd_prices[-1][1] - self.zd_prices[-2][1]) < 5:
                                pass
                            else:
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: NORMAL_SIGNAL_mode3c_xiangduidi_30mins_zhidie at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        if '3c' in self.long_mode.split(' ') and '3c' not in self.strategies:# and self.is_30k_positive:
                            self.strategies['3c'] = mk["close"][-1]
                            if self.long_avg_price < 0.1:
                                self.long_avg_price = mk["close"][-1]
                            else:
                                self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: SIGNAL_mode3c_xiangduidi_30mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                                self.sh.write("Double check 30k is above MA.\n") 

            # Signal：zhishun
            # 11：05~11:20定点止损
            mk_l20 = mk[-20:]
            if cur_time > time(hour=11,minute=5) and cur_time < time(hour=11,minute=20) and self.long_avg_price >0.1 \
                and len(mk_l20[mk_l20["close"] > mk_l20["vwap"]*0.998]) < 16:
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: SIGNAL_dingdian_zhishun_1110 at price %.2f with profit: %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                    self.sh.write("Double check whether it's creating xindi. if not, wait until 14:45 and check 30k.\n")
                self.long_avg_price = 0.0
            # 破相后反弹时局部滞涨止损
            if self.long_avg_price >0.1 \
                and day_CL < self.first20_low*0.998 and day_CL < self.yestoday_close*0.997 \
                and (mk_l20["close"][-1] > (day_CL + day_CH)/2 or mk_l20["close"][-1]>mk_l20["vwap"][-1]*0.999) \
                and (mk_l20["close"][-5:] < mk_l20["close"][:-5].max()*1.001).all():
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                with open(self.signal_log, mode='a') as self.sh:
                    self.sh.write("%s: SIGNAL_changqi_piankong_fantan_zhishun at price %.2f with profit %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                self.long_avg_price = 0.0

            # 止盈
            if num_bar - self.median_start > 10:
                median_CH_index, median_CH = max(enumerate(mk["close"][self.median_start:]), key=operator.itemgetter(1))
                median_h_zz = mk[self.median_start+median_CH_index:]
                if median_CH > median_h_zz["vwap"][0] * self.duobeili_threshold:
                    if len(median_h_zz) == 10 or len(median_h_zz) == 18:
                        with open(self.signal_log, mode='a') as self.sh:
                            self.sh.write("%s: NORMAL_SIGNAL_jubu_zz %d minutes, at price %.2f\n"%(mk.index[-1], len(median_h_zz), mk["close"][-1]))
                    elif len(median_h_zz) >= 30 and len(median_h_zz) < 60:
                        if len(median_h_zz) == 30:
                            self.zz_count += 1
                            if self.zz_count == 1:
                                self.zz_1_high = median_CH
                            with open(self.signal_log, mode='a') as self.sh:
                                self.sh.write("%s: NORMAL_SIGNAL_zz: %d times, at price %.2f\n"%(mk.index[-1], self.zz_count, mk["close"][-1]))
                            #print((mk["vwap"][-1] + median_CH )*0.5)
                            #case1&2: close long while zz 1 or 2 times
                        if self.zz_count >= self.zz_count_max \
                            and median_CH > self.yestoday_close*1.015 and mk["close"][-1] > (mk["vwap"][-1] + median_CH )*0.5:
                            if self.long_avg_price > 0.1:
                                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                                with open(self.signal_log, mode='a') as self.sh:
                                    self.sh.write("%s: SIGNAL_zz_zhiying: %d times, at price %.2f with profit %.1f\n"%(mk.index[-1], \
                                        self.zz_count, mk["close"][-1], res))
                                    self.sh.write("Double check whether it's a strong trending; if so, wait second ZZ signal.\n")
                                self.long_avg_price = 0.0

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
